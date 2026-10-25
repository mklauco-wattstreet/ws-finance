"""Europe/Prague delivery-day calendar: slot numbers <-> UTC instants.

A delivery day is the local Prague calendar day. Its 15-min slots are numbered
1..N in time order, where N is 92 (spring-forward day), 96 (normal day) or 100
(fall-back day) - the same numbering OTE uses in its reports. 60-min slots
work the same way with N = 23 / 24 / 25.

`delivery_datetime` (timestamptz, the UTC instant a slot starts) is the
unambiguous key. `time_interval` is only a readable local wall-clock label
("HH:MM-HH:MM"); on the fall-back day 02:00-03:00 occurs twice, so the label
repeats and must never be used as a key.
"""

import re
import zoneinfo
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

PRAGUE_TZ = zoneinfo.ZoneInfo("Europe/Prague")


def day_start_utc(d: date) -> datetime:
    """UTC instant of local midnight at the start of delivery day `d`."""
    return datetime(d.year, d.month, d.day, tzinfo=PRAGUE_TZ).astimezone(timezone.utc)


def slots_in_day(d: date, minutes: int = 15) -> int:
    """Number of `minutes`-long slots in delivery day `d` (92/96/100 for 15 min)."""
    seconds = (day_start_utc(d + timedelta(days=1)) - day_start_utc(d)).total_seconds()
    return int(seconds // (minutes * 60))


def slot_start_utc(d: date, slot: int, minutes: int = 15) -> datetime:
    """UTC start instant of 1-based `slot` in delivery day `d`.

    Raises ValueError if `slot` does not exist on that day.
    """
    n = slots_in_day(d, minutes)
    if not 1 <= slot <= n:
        raise ValueError(f"slot {slot} out of range 1..{n} for {d} ({minutes}-min)")
    return day_start_utc(d) + timedelta(minutes=(slot - 1) * minutes)


def local_label(start_utc: datetime, minutes: int = 15, midnight: str = "24:00") -> str:
    """Local wall-clock label 'HH:MM-HH:MM' for a slot starting at `start_utc`.

    `midnight` is how an end at local midnight is written ("24:00" or "00:00");
    tables differ, and each caller keeps its table's existing convention.
    """
    start = start_utc.astimezone(PRAGUE_TZ)
    end = (start_utc + timedelta(minutes=minutes)).astimezone(PRAGUE_TZ)
    end_txt = midnight if (end.hour, end.minute) == (0, 0) else end.strftime("%H:%M")
    return f"{start.strftime('%H:%M')}-{end_txt}"


def slot_key(d: date, slot: int, minutes: int = 15, midnight: str = "24:00") -> Tuple[datetime, str]:
    """(delivery_datetime, time_interval) for slot `slot` of delivery day `d`."""
    start = slot_start_utc(d, slot, minutes)
    return start, local_label(start, minutes, midnight)


def wall_slots(d: date, minutes: int = 15) -> Dict[Tuple[int, int], List[int]]:
    """Map local wall-clock start (hour, minute) -> slot numbers that start then.

    One slot per wall time, except during the repeated fall-back hour, which
    maps to two slots (first pass = CEST, second pass = CET).
    """
    result: Dict[Tuple[int, int], List[int]] = {}
    for slot in range(1, slots_in_day(d, minutes) + 1):
        local = slot_start_utc(d, slot, minutes).astimezone(PRAGUE_TZ)
        result.setdefault((local.hour, local.minute), []).append(slot)
    return result


# Start of an OTE interval label, in any of the spellings OTE uses on the
# fall-back day: "02:00", "02a:00", "02:00A", "02:00 A".
_LABEL_START_RE = re.compile(r"^\s*(\d{1,2})\s*([abAB]?)\s*:\s*(\d{2})\s*([abAB]?)")


def parse_label_start(label: str) -> Tuple[int, int, Optional[str]]:
    """Parse the start of an OTE label into (hour, minute, 'A'|'B'|None)."""
    m = _LABEL_START_RE.match(str(label))
    if not m:
        raise ValueError(f"unrecognised time label: {label!r}")
    hour, marker1, minute, marker2 = m.groups()
    marker = (marker1 or marker2 or "").upper() or None
    return int(hour), int(minute), marker


class AmbiguousLabelError(ValueError):
    """An unmarked label in the repeated fall-back hour that cannot be placed."""

    def __init__(self, message: str, slots: List[int]):
        super().__init__(message)
        self.slots = slots


class LabelResolver:
    """Resolve OTE interval labels of one delivery day to slot numbers.

    Labels during the repeated fall-back hour are resolved by their A/B marker
    when present. An unmarked repeated label is only placed when the day's
    labels contain it exactly twice (`occurrences`): the first in file order is
    the first pass (A), the second is the second pass (B). If it appears once,
    there is no way to tell which pass it belongs to, and AmbiguousLabelError
    (carrying both candidate slots) is raised instead of guessing.
    """

    def __init__(self, d: date, labels: List[str], minutes: int = 15):
        self.day = d
        self.minutes = minutes
        self._wall = wall_slots(d, minutes)
        self._seen: Dict[Tuple[int, int], int] = {}
        self._occurrences: Dict[Tuple[int, int], int] = {}
        for label in labels:
            hour, minute, marker = parse_label_start(label)
            if marker is None:
                key = (hour, minute)
                self._occurrences[key] = self._occurrences.get(key, 0) + 1

    def slot(self, label: str) -> int:
        hour, minute, marker = parse_label_start(label)
        candidates = self._wall.get((hour, minute))
        if not candidates:
            raise ValueError(f"{label!r} does not exist on {self.day}")
        if len(candidates) == 1:
            if marker is not None:
                raise ValueError(f"{label!r} has an A/B marker but is not ambiguous on {self.day}")
            return candidates[0]
        if marker is not None:
            return candidates[0] if marker == "A" else candidates[1]
        if self._occurrences.get((hour, minute), 0) != len(candidates):
            raise AmbiguousLabelError(
                f"{label!r} on {self.day} has no A/B marker and appears "
                f"{self._occurrences.get((hour, minute), 0)}x; cannot tell which pass it is",
                candidates,
            )
        index = self._seen.get((hour, minute), 0)
        self._seen[(hour, minute)] = index + 1
        return candidates[index]
