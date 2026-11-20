"""Parsers for ENTSO-E zone-level net positions and congestion costs.

Documents handled:

* ``A25`` / ``B09`` (Publication_MarketDocument) - implicit allocation net
  positions. ``contract_MarketAgreement.type`` A01 = day-ahead, A07 = intraday.
* ``A09`` / ``B09`` (Publication_MarketDocument) - scheduled net positions.
  ``contract_MarketAgreement.type`` A01 = day-ahead schedule, A05 = total
  schedule.
* ``A92`` (TransmissionNetwork_MarketDocument) - costs of congestion
  management, monthly (P1M), one TimeSeries per ``businessType``.

Sign convention (all net position series): positive = net EXPORT of the zone,
negative = net IMPORT. ENTSO-E states the direction through the domains of each
TimeSeries and always publishes non-negative quantities: ``out_Domain`` = zone
means the zone sends energy out (export, +1); ``in_Domain`` = zone means the
zone receives it (import, -1). The counter domain is a region code
(``REGION_CODE-----``, ``10Y1001C--00059P``) and is ignored. A document may
carry one TimeSeries per direction covering complementary Periods (the intraday
net position does: export Periods in one TimeSeries, import Periods in the
other); slots covered by both directions are netted by summing the signed
values.

Exception, verified against the data: in the INTRADAY net position (A25,
contract type A07) the domain labels are the other way round. There the
TimeSeries with ``in_Domain`` = zone carries a net EXPORT increase and the one
with ``out_Domain`` = zone a net IMPORT increase: over 2026-11-12..19 (768
slots) ``sched_net_position_total - sched_net_position_da`` equals
``-(intraday read with the day-ahead rule)`` with a mean absolute error of 2.9
MW and a correlation of -0.999, so only the flipped reading satisfies
total schedule = day-ahead + intraday. ``CONTRACT_SIGN`` below flips the sign
for A07 so that all four stored columns share "positive = export".

Period expansion: ``curveType`` A03 omits Points that repeat the previous one,
so values are forward-filled within each Period; PT60M slots are expanded to
four 15-minute slots. Trade date, period number and time label are derived like
``CrossBorderFlowsParser`` does: from the UTC instant, in Prague local time
(period counted in elapsed 15-minute slots from local midnight, 1..92/96/100).
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
import zoneinfo
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from dst_calendar import day_start_utc, local_label

logger = logging.getLogger(__name__)

PRAGUE_TZ = zoneinfo.ZoneInfo("Europe/Prague")
SLOT_MINUTES = 15

# contract_MarketAgreement.type -> multiplier applied on top of the in/out domain
# rule (see the module docstring: A07 intraday labels the directions inverted).
CONTRACT_SIGN = {"A07": -1.0}

# businessType of A92 TimeSeries -> readable cost_type. The XML carries only the
# code, so the names come from the ENTSO-E EIC/business type code list. The raw
# code is kept in the string so a wrong guess is still recoverable.
COST_TYPE_BY_BUSINESS_TYPE = {
    "A46": "redispatching_A46",     # system operator re-dispatching
    "B03": "countertrade_B03",      # counter trade
    "B04": "congestion_costs_B04",  # congestion costs
}


def _parse_ts(text: str) -> datetime:
    """Parse an ENTSO-E instant (``2026-11-18T23:00Z``) into aware UTC."""
    value = text.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _find_text(el: ET.Element, tag: str) -> Optional[str]:
    child = el.find(f"{{*}}{tag}")
    return child.text.strip() if child is not None and child.text else None


def _resolution_minutes(resolution: str) -> int:
    """PT15M / PT60M / PT1H -> minutes (sub-day resolutions only)."""
    match = re.fullmatch(r"PT(\d+)([MH])", resolution.strip())
    if not match:
        raise ValueError(f"unsupported resolution {resolution!r}")
    value = int(match.group(1))
    return value * 60 if match.group(2) == "H" else value


def slot_fields(slot_utc: datetime) -> Tuple[date, int, str]:
    """``(trade_date, period, time_interval)`` of a 15-minute slot start.

    Same derivation as ``BaseParser`` in ``parsers.py``: trade date in Prague
    local time, period counted in elapsed slots from Prague midnight, label in
    local wall-clock time.
    """
    trade_date = slot_utc.astimezone(PRAGUE_TZ).date()
    period = int((slot_utc - day_start_utc(trade_date)).total_seconds() // (SLOT_MINUTES * 60)) + 1
    return trade_date, period, local_label(slot_utc, SLOT_MINUTES, midnight="00:00")


def parse_net_position_xml(
    xml_text: str, zone_eic: str
) -> Dict[Optional[str], Dict[datetime, float]]:
    """Parse one net position document into signed 15-minute series.

    Args:
        xml_text: A25/B09 or A09/B09 Publication_MarketDocument.
        zone_eic: EIC of the zone the net position belongs to.

    Returns:
        ``{contract_MarketAgreement.type: {slot_start_utc: signed_mw}}``. The
        key is ``None`` for TimeSeries without a contract type. Positive =
        net export of the zone, negative = net import.
    """
    root = ET.fromstring(xml_text)
    result: Dict[Optional[str], Dict[datetime, float]] = {}

    for ts in root.findall("{*}TimeSeries"):
        in_domain = _find_text(ts, "in_Domain.mRID")
        out_domain = _find_text(ts, "out_Domain.mRID")
        if out_domain == zone_eic and in_domain != zone_eic:
            sign = 1.0
        elif in_domain == zone_eic and out_domain != zone_eic:
            sign = -1.0
        else:
            logger.debug("net position TimeSeries without a single zone side (in=%s out=%s), skipped",
                         in_domain, out_domain)
            continue

        contract = _find_text(ts, "contract_MarketAgreement.type")
        sign *= CONTRACT_SIGN.get(contract or "", 1.0)
        forward_fill = (_find_text(ts, "curveType") or "A03") == "A03"
        series = result.setdefault(contract, {})

        for period in ts.findall("{*}Period"):
            for slot, value in _expand_period(period, forward_fill).items():
                series[slot] = series.get(slot, 0.0) + sign * value

    return result


def _expand_period(period: ET.Element, forward_fill: bool) -> Dict[datetime, float]:
    """Expand one Period to ``{slot_start_utc: quantity}`` on the 15-minute grid."""
    interval = period.find("{*}timeInterval")
    if interval is None:
        return {}
    start = _parse_ts(_find_text(interval, "start") or "")
    end = _parse_ts(_find_text(interval, "end") or "")
    step = _resolution_minutes(_find_text(period, "resolution") or "PT15M")
    positions = int((end - start).total_seconds() // (step * 60))

    points: Dict[int, float] = {}
    for point in period.findall("{*}Point"):
        position = _find_text(point, "position")
        quantity = _find_text(point, "quantity")
        if position is not None and quantity is not None:
            points[int(position)] = float(quantity)

    out: Dict[datetime, float] = {}
    last: Optional[float] = None
    for position in range(1, positions + 1):
        if position in points:
            last = points[position]
        elif not forward_fill:
            continue
        if last is None:
            continue
        slot_start = start + timedelta(minutes=(position - 1) * step)
        for sub in range(step // SLOT_MINUTES):
            out[slot_start + timedelta(minutes=sub * SLOT_MINUTES)] = last
    return out


def _add_months(first_of_month: date, months: int) -> date:
    index = first_of_month.year * 12 + (first_of_month.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def _prague_midnight_utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=PRAGUE_TZ).astimezone(timezone.utc)


def parse_congestion_costs_xml(xml_text: str) -> List[Dict[str, object]]:
    """Parse an A92 document into one row per month and cost type.

    Months are calendar months in Prague local time (a Period starts and ends
    on local month boundaries, expressed in UTC). A03 omitted Points repeat the
    previous month, so a Period that spans several months with a single Point
    yields one row per month.

    Returns:
        ``[{"period_start", "period_end", "cost_type", "amount_eur"}, ...]``
        with UTC datetimes; ``cost_type`` from ``COST_TYPE_BY_BUSINESS_TYPE``
        (an unknown code becomes ``unknown_<code>``).
    """
    root = ET.fromstring(xml_text)
    rows: Dict[Tuple[datetime, str], Dict[str, object]] = {}

    for ts in root.findall("{*}TimeSeries"):
        business_type = _find_text(ts, "businessType") or "unknown"
        cost_type = COST_TYPE_BY_BUSINESS_TYPE.get(business_type, f"unknown_{business_type}")[:30]
        forward_fill = (_find_text(ts, "curveType") or "A03") == "A03"

        for period in ts.findall("{*}Period"):
            resolution = _find_text(period, "resolution") or ""
            if resolution not in ("P1M", "P1Y"):
                logger.warning("A92 Period with unsupported resolution %s skipped", resolution)
                continue
            months_per_step = 12 if resolution == "P1Y" else 1

            interval = period.find("{*}timeInterval")
            start = _parse_ts(_find_text(interval, "start") or "")
            end = _parse_ts(_find_text(interval, "end") or "")
            first = start.astimezone(PRAGUE_TZ).date()

            amounts: Dict[int, float] = {}
            for point in period.findall("{*}Point"):
                position = _find_text(point, "position")
                amount = _find_text(point, "congestionCost_Price.amount")
                if amount is None:
                    amount = _find_text(point, "quantity")
                if position is not None and amount is not None:
                    amounts[int(position)] = float(amount)

            position = 0
            last: Optional[float] = None
            month_start = first
            while _prague_midnight_utc(month_start) < end:
                position += 1
                next_start = _add_months(month_start, months_per_step)
                if position in amounts:
                    last = amounts[position]
                elif not forward_fill:
                    month_start = next_start
                    continue
                if last is not None:
                    p_start = _prague_midnight_utc(month_start)
                    rows[(p_start, cost_type)] = {
                        "period_start": p_start,
                        "period_end": _prague_midnight_utc(next_start),
                        "cost_type": cost_type,
                        "amount_eur": last,
                    }
                month_start = next_start

    return list(rows.values())
