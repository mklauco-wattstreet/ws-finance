"""Time helpers shared by the CEPS uploader, feature aggregation and backfills.

CEPS rows are keyed on delivery_datetime (TIMESTAMPTZ, UTC start of the
minute / interval). trade_date and time_interval are the local Prague date
and "HH:MM-HH:MM" label derived from it; on the fall-back day the label of
02:00-03:00 repeats, so it is never used to group or join.

Buckets are computed on the instant: Prague offsets are whole hours, so
flooring epoch seconds to 900 / 3600 gives the local quarter-hour / hour
start regardless of the session TimeZone.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Set, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from dst_calendar import PRAGUE_TZ

TZ = 'Europe/Prague'


def bucket_start_sql(col: str = 'delivery_datetime', minutes: int = 15) -> str:
    """SQL: start instant of the `minutes` bucket containing `col`."""
    return f"to_timestamp(floor(extract(epoch FROM {col}) / {minutes * 60}) * {minutes * 60})"


def local_date_sql(col: str) -> str:
    """SQL: local Prague date of instant `col`."""
    return f"({col} AT TIME ZONE '{TZ}')::date"


def local_label_sql(col: str, minutes: int = 15) -> str:
    """SQL: local 'HH:MM-HH:MM' label of the interval starting at instant `col`.

    Same convention as dst_calendar.local_label(..., midnight='00:00'):
    fall-back first pass ends "02:45-02:00", spring "01:45-03:00".
    """
    return (
        f"to_char({col} AT TIME ZONE '{TZ}', 'HH24:MI') || '-' || "
        f"to_char(({col} + interval '{minutes} minutes') AT TIME ZONE '{TZ}', 'HH24:MI')"
    )


def quarter_start(dt: datetime) -> datetime:
    """UTC start of the 15-min interval containing aware `dt`."""
    utc = dt.astimezone(timezone.utc)
    return utc.replace(minute=utc.minute - utc.minute % 15, second=0, microsecond=0)


def affected_quarters(records: Iterable[dict]) -> Set[datetime]:
    """UTC starts of the 15-min intervals touched by `records` (by delivery_datetime)."""
    return {quarter_start(r['delivery_datetime']) for r in records}


def instant_window(instants: Set[datetime], minutes: int = 15) -> Tuple[datetime, datetime]:
    """[first start, last start + minutes) covering `instants`, in UTC."""
    return min(instants), max(instants) + timedelta(minutes=minutes)


def local_window(lo_utc: datetime, hi_utc: datetime) -> Tuple[datetime, datetime]:
    """Naive local bounds covering [lo_utc, hi_utc), padded by an hour on each side.

    Used on delivery_timestamp (the 1-min partition column) so the planner
    can prune partitions; the exact filter is on delivery_datetime.
    """
    pad = timedelta(hours=1)
    return (
        lo_utc.astimezone(PRAGUE_TZ).replace(tzinfo=None) - pad,
        hi_utc.astimezone(PRAGUE_TZ).replace(tzinfo=None) + pad,
    )


def local_dates(instants: Set[datetime]) -> list:
    """Sorted local Prague dates of `instants`."""
    return sorted({i.astimezone(PRAGUE_TZ).date() for i in instants})
