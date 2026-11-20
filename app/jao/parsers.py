"""Parsers for JAO Core publication tool responses -> long-format rows.

Every parser takes the ``data`` array of one response and returns a list of
tuples whose layout matches the column list named in its docstring. All hubs
and borders present in the response are kept (not only CZ), with JAO's own
codes as text. Null values are skipped; rows are de-duplicated on the table key
(last one wins) because one ``INSERT ... ON CONFLICT`` cannot touch a key twice.

Source columns -> target columns
--------------------------------
``maxNetPos`` (hourly; one row per interval, columns ``min<HUB>``/``max<HUB>``)
    jao_core_max_net_position (delivery_datetime, hub, min_mw, max_mw)
    ``minCZ`` -> hub=CZ min_mw, ``maxCZ`` -> hub=CZ max_mw.

``maxExchanges`` (hourly; columns ``border_<FROM>_<TO>``)
    jao_core_max_exchange (delivery_datetime, from_hub, to_hub, max_bex_mw)

``netPos`` and ``ID1_/ID2_/ID3_netPos`` (15 min; columns ``hub_<HUB>``)
    jao_core_net_position (delivery_datetime, hub, market, net_position_mw)
    Sign as published: positive = net export of the hub, negative = net import
    (CZ hub_CZ equals the sum of CZ outgoing minus incoming scheduled exchange).

``scheduledExchanges`` and ``ID*_scheduledExchanges`` (15 min; ``border_<FROM>_<TO>``)
    jao_core_scheduled_exchange (delivery_datetime, from_hub, to_hub, market, scheduled_mw)
    ``border_CZ_DE`` is the exchange from CZ to DE. Both directions are published
    as separate non-negative columns (``border_DE_CZ`` is DE to CZ); stored as is.

``priceSpread`` and ``ID*_priceSpread`` (15 min; ``border_<FROM>_<TO>``)
    jao_core_price_spread (delivery_datetime, from_hub, to_hub, market, spread_eur)
    Stored exactly as published: ``border_CZ_DE`` and ``border_DE_CZ`` carry
    opposite signs of the same number. Checked against ENTSO-E day-ahead prices
    (AT/DE, 2026-11-10, 96 intervals, zero difference): ``border_<FROM>_<TO>`` =
    price(TO) - price(FROM), so a positive value means the destination is dearer.

``allocationConstraint`` (15 min; columns ``limitUp_<HUB>``/``limitDown_<HUB>``)
    jao_core_allocation_constraint (delivery_datetime, hub, market, limit_up_mw, limit_down_mw)

``IDCC<A-E>_intradayAtc`` / ``IDCC<A-E>_intradayNtc`` (15 min; ``border_<FROM>_<TO>``)
    jao_core_intraday_atc (delivery_datetime, from_hub, to_hub, calc_round, atc_mw, ntc_mw)
    ``intradayAtc`` -> atc_mw, ``intradayNtc`` -> ntc_mw of the same round and
    directed border. Values can be negative (NTC is the net schedule of the
    border, so ``border_DE_CZ`` mirrors ``border_CZ_DE``).

Border column names
-------------------
``border_<FROM>_<TO>`` is ambiguous when a hub code contains an underscore. Two
sets of codes occur:

* In ``maxExchanges`` the virtual hubs use the same codes as ``netPos``
  (``DE_DK2_BigHub``...), so the split is resolved against the known hub codes
  (``border_DE_DE_DK2_BigHub`` = DE -> DE_DK2_BigHub).
* In ``scheduledExchanges`` the virtual hubs are spelled differently
  (``border_DE_DK2_BIGHUB_VH``, ``border_BG_VH_RO``...). They are mapped by hand
  to the ``netPos`` hub code (table ``_SCHEDULED_ALIASES``); the mapping was
  checked against the published net positions (hub net position equals outgoing
  minus incoming scheduled exchange).

A column that cannot be split is skipped and reported through ``unresolved``.
"""

import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

Row = Dict[str, Any]

# Core hubs (bidding zones) as published in netPos / maxNetPos.
CORE_HUBS: Tuple[str, ...] = (
    "ALBE", "ALDE", "AT", "BE", "CZ", "DE", "FR", "HR", "HU", "NL", "PL", "RO", "SI", "SK",
)
# Virtual hubs as published in netPos / maxNetPos / maxExchanges.
VIRTUAL_HUBS: Tuple[str, ...] = (
    "DE_NO2_BigHub", "NL_NO2_NorNed", "DE_DK2_BigHub", "DE_SE4_Baltic",
    "PL_SE4_SwePol", "PL_LT_BigHub", "RO_BG_VH", "NL_DK1_COBRA", "DE_DK1_VH",
)
KNOWN_HUBS: Set[str] = set(CORE_HUBS) | set(VIRTUAL_HUBS) | {"ES", "DK1", "DK2"}

# scheduledExchanges names its virtual-hub borders differently from netPos.
# Key: column suffix after "border_" (upper-cased) -> (from_hub, to_hub).
_SCHEDULED_ALIASES: Dict[str, Tuple[str, str]] = {
    "DE_DK1_VH": ("DE", "DE_DK1_VH"),
    "DK1_VH_DE": ("DE_DK1_VH", "DE"),
    "NL_COBRA_DK1_VH": ("NL", "NL_DK1_COBRA"),
    "DK1_COBRA_NL_VH": ("NL_DK1_COBRA", "NL"),
    "DE_DK2_BIGHUB_VH": ("DE", "DE_DK2_BigHub"),
    "DK2_BIGHUB_VH_DE": ("DE_DK2_BigHub", "DE"),
    "NL_NO2_NORNED_VH": ("NL", "NL_NO2_NorNed"),
    "NO2_NORNED_VH_NL": ("NL_NO2_NorNed", "NL"),
    "RO_BG_VH": ("RO", "RO_BG_VH"),
    "BG_VH_RO": ("RO_BG_VH", "RO"),
    "PL_SE4_SWEPOL_VH": ("PL", "PL_SE4_SwePol"),
    "SE4_SWEPOL_VH_PL": ("PL_SE4_SwePol", "PL"),
    "PL_LT_BIGHUB_VH": ("PL", "PL_LT_BigHub"),
    "LT_BIGHUB_VH_PL": ("PL_LT_BigHub", "PL"),
    "DE_SE4_BALTIC_VH": ("DE", "DE_SE4_Baltic"),
    "SE4_BALTIC_VH_DE": ("DE_SE4_Baltic", "DE"),
    "DE_NO2_BIGHUB_VH": ("DE", "DE_NO2_BigHub"),
    "NO2_BIGHUB_VH_DE": ("DE_NO2_BigHub", "DE"),
}

# Longest hub code the tables take (VARCHAR(20)).
MAX_HUB_LEN = 20


def parse_utc(value: str) -> datetime:
    """Parse ``2026-11-18T22:00:00Z`` into an aware UTC datetime."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def to_number(value: Any) -> Optional[float]:
    """Return a finite float, or None for null / empty / non-numeric / NaN."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def split_border(
    suffix: str, aliases: Optional[Dict[str, Tuple[str, str]]] = None
) -> Optional[Tuple[str, str]]:
    """Split ``<FROM>_<TO>`` (the part after ``border_``) into (from_hub, to_hub).

    Order of attempts: explicit alias table, then a split where both sides are
    known hub codes, then (only when the name has exactly one underscore) a
    plain split so a new two-part border is not lost. Returns None otherwise.
    """
    if aliases:
        hit = aliases.get(suffix.upper())
        if hit:
            return hit
    parts = suffix.split("_")
    for i in range(1, len(parts)):
        left, right = "_".join(parts[:i]), "_".join(parts[i:])
        if left in KNOWN_HUBS and right in KNOWN_HUBS:
            return left, right
    if len(parts) == 2:
        return parts[0], parts[1]
    return None


def _valid_hubs(*hubs: str) -> bool:
    return all(0 < len(h) <= MAX_HUB_LEN for h in hubs)


def _border_map(
    columns: Iterable[str], aliases: Optional[Dict[str, Tuple[str, str]]],
    unresolved: Set[str],
) -> Dict[str, Tuple[str, str]]:
    """Map every ``border_*`` column of a response to its (from, to) hubs."""
    result: Dict[str, Tuple[str, str]] = {}
    for column in columns:
        if not column.startswith("border_"):
            continue
        pair = split_border(column[len("border_"):], aliases)
        if pair is None or pair[0] == pair[1] or not _valid_hubs(*pair):
            unresolved.add(column)
            continue
        result[column] = pair
    return result


def _columns(rows: List[Row]) -> List[str]:
    """Union of column names across rows, in first-seen order."""
    seen: Dict[str, None] = {}
    for row in rows:
        for key in row:
            seen.setdefault(key, None)
    return list(seen)


# ----------------------------------------------------------------------
# Day-ahead / auction parsers
# ----------------------------------------------------------------------


def parse_max_net_position(rows: List[Row]) -> List[Tuple]:
    """-> (delivery_datetime, hub, min_mw, max_mw)."""
    out: Dict[Tuple, Tuple] = {}
    for row in rows:
        ts = parse_utc(row["dateTimeUtc"])
        values: Dict[str, List[Optional[float]]] = {}
        for column, raw in row.items():
            for prefix, idx in (("min", 0), ("max", 1)):
                if column.startswith(prefix) and len(column) > 3 and column[3].isupper():
                    hub = column[3:]
                    if not _valid_hubs(hub):
                        continue
                    values.setdefault(hub, [None, None])[idx] = to_number(raw)
        for hub, (low, high) in values.items():
            if low is None and high is None:
                continue
            out[(ts, hub)] = (ts, hub, low, high)
    return list(out.values())


def parse_max_exchange(rows: List[Row], unresolved: Optional[Set[str]] = None) -> List[Tuple]:
    """-> (delivery_datetime, from_hub, to_hub, max_bex_mw)."""
    unresolved = unresolved if unresolved is not None else set()
    borders = _border_map(_columns(rows), None, unresolved)
    out: Dict[Tuple, Tuple] = {}
    for row in rows:
        ts = parse_utc(row["dateTimeUtc"])
        for column, (src, dst) in borders.items():
            value = to_number(row.get(column))
            if value is None:
                continue
            out[(ts, src, dst)] = (ts, src, dst, value)
    return list(out.values())


def parse_net_position(rows: List[Row], market: str) -> List[Tuple]:
    """-> (delivery_datetime, hub, market, net_position_mw)."""
    out: Dict[Tuple, Tuple] = {}
    for row in rows:
        ts = parse_utc(row["dateTimeUtc"])
        for column, raw in row.items():
            if not column.startswith("hub_"):
                continue
            hub = column[len("hub_"):]
            value = to_number(raw)
            if value is None or not _valid_hubs(hub):
                continue
            out[(ts, hub)] = (ts, hub, market, value)
    return list(out.values())


def _parse_border_values(
    rows: List[Row], market: str,
    aliases: Optional[Dict[str, Tuple[str, str]]], unresolved: Set[str],
) -> List[Tuple]:
    borders = _border_map(_columns(rows), aliases, unresolved)
    out: Dict[Tuple, Tuple] = {}
    for row in rows:
        ts = parse_utc(row["dateTimeUtc"])
        for column, (src, dst) in borders.items():
            value = to_number(row.get(column))
            if value is None:
                continue
            out[(ts, src, dst)] = (ts, src, dst, market, value)
    return list(out.values())


def parse_scheduled_exchange(
    rows: List[Row], market: str, unresolved: Optional[Set[str]] = None
) -> List[Tuple]:
    """-> (delivery_datetime, from_hub, to_hub, market, scheduled_mw)."""
    return _parse_border_values(
        rows, market, _SCHEDULED_ALIASES, unresolved if unresolved is not None else set()
    )


def parse_price_spread(
    rows: List[Row], market: str, unresolved: Optional[Set[str]] = None
) -> List[Tuple]:
    """-> (delivery_datetime, from_hub, to_hub, market, spread_eur)."""
    return _parse_border_values(
        rows, market, None, unresolved if unresolved is not None else set()
    )


def parse_allocation_constraint(rows: List[Row], market: str) -> List[Tuple]:
    """-> (delivery_datetime, hub, market, limit_up_mw, limit_down_mw)."""
    out: Dict[Tuple, Tuple] = {}
    for row in rows:
        ts = parse_utc(row["dateTimeUtc"])
        values: Dict[str, List[Optional[float]]] = {}
        for column, raw in row.items():
            for prefix, idx in (("limitUp_", 0), ("limitDown_", 1)):
                if column.startswith(prefix):
                    hub = column[len(prefix):]
                    if _valid_hubs(hub):
                        values.setdefault(hub, [None, None])[idx] = to_number(raw)
        for hub, (up, down) in values.items():
            if up is None and down is None:
                continue
            out[(ts, hub)] = (ts, hub, market, up, down)
    return list(out.values())


# ----------------------------------------------------------------------
# Intraday capacity (ATC / NTC)
# ----------------------------------------------------------------------


def parse_intraday_capacity(
    rows: List[Row], unresolved: Optional[Set[str]] = None
) -> Dict[Tuple, float]:
    """One intradayAtc or intradayNtc response -> {(delivery_datetime, from_hub, to_hub): value}."""
    unresolved = unresolved if unresolved is not None else set()
    borders = _border_map(_columns(rows), None, unresolved)
    out: Dict[Tuple, float] = {}
    for row in rows:
        ts = parse_utc(row["dateTimeUtc"])
        for column, (src, dst) in borders.items():
            value = to_number(row.get(column))
            if value is not None:
                out[(ts, src, dst)] = value
    return out


def merge_intraday_atc(
    calc_round: str,
    atc: Optional[Dict[Tuple, float]],
    ntc: Optional[Dict[Tuple, float]],
) -> List[Tuple]:
    """Merge ATC and NTC of one round.

    -> (delivery_datetime, from_hub, to_hub, calc_round, atc_mw, ntc_mw) when both
    were fetched. A side passed as None (its fetch failed) is left out of the
    tuple entirely, so the caller upserts only the column list that matches and
    never overwrites the other value with NULL.
    """
    keys: Set[Tuple] = set()
    if atc:
        keys |= set(atc)
    if ntc:
        keys |= set(ntc)
    rows: List[Tuple] = []
    for key in sorted(keys):
        ts, src, dst = key
        row: List[Any] = [ts, src, dst, calc_round]
        if atc is not None:
            row.append(atc.get(key))
        if ntc is not None:
            row.append(ntc.get(key))
        rows.append(tuple(row))
    return rows
