"""Generic parser for ENTSO-E cross-border capacity documents.

One parser for every document family the border capacity runner reads:

    A61  estimated net transfer capacity (forecast)      Publication_MarketDocument
    A31  offered capacity (explicit / intraday)          Publication_MarketDocument
    A26  already allocated / nominated capacity          Publication_MarketDocument
    A25  explicit auction results / auction revenue      Publication_MarketDocument

The parser does not know what a series *means*: it returns every TimeSeries
with its raw attributes (businessType, contract type, auction type, unit,
domains, update time) and its points expanded to one entry per position of the
Period, at the source's own resolution. The runner decides the table item and
horizon from the attributes.

Curve types:
    A01  every position carries a Point.
    A03  a Point is omitted when its value equals the previous position, so
         omitted positions are forward-filled within the Period. A leading
         omitted position (nothing seen yet) is left out, never invented as 0.

Calendar resolutions (P1D, P1M, P1Y) are stepped in Europe/Prague wall-clock
time, so a "day" is 23 or 25 hours long on the DST switch days and a "month"
starts at local midnight. PTnM / PTnH resolutions are plain durations.

Point values: ``quantity`` is the primary value. ``price.amount`` (explicit
auction results) is returned separately. A Point without ``quantity`` and
without ``price.amount`` is skipped.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional
import zoneinfo

PRAGUE_TZ = zoneinfo.ZoneInfo("Europe/Prague")

_ISO_DURATION = re.compile(
    r"^P(?:(?P<years>\d+)Y)?(?:(?P<months>\d+)M)?(?:(?P<weeks>\d+)W)?(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?)?$"
)

# ENTSO-E unit names -> short strings that fit entsoe_border_capacity.unit
# (VARCHAR(10)). Unknown names are passed through truncated.
_UNIT_NAMES = {
    "MAW": "MW",
    "MWH": "MWh",
    "EUR": "EUR",
}


def normalise_unit(name: Optional[str]) -> Optional[str]:
    """Short unit string (at most 10 characters) from an ENTSO-E unit name."""
    if not name:
        return None
    short = _UNIT_NAMES.get(name.strip().upper(), name.strip())
    return short[:10]


@dataclass(frozen=True)
class Resolution:
    """A parsed ISO 8601 duration used as a Period resolution."""

    text: str
    years: int = 0
    months: int = 0
    days: int = 0
    minutes: int = 0  # hours folded in, weeks folded into days

    @property
    def is_calendar(self) -> bool:
        return bool(self.years or self.months or self.days)


def parse_resolution(text: str) -> Resolution:
    """Parse an ISO 8601 duration such as PT15M, PT60M, P1D, P1M, P1Y."""
    match = _ISO_DURATION.match(text.strip())
    if not match:
        raise ValueError(f"Unsupported resolution {text!r}")
    g = {k: int(v) if v else 0 for k, v in match.groupdict().items()}
    return Resolution(
        text=text.strip(),
        years=g["years"],
        months=g["months"],
        days=g["days"] + 7 * g["weeks"],
        minutes=g["hours"] * 60 + g["minutes"],
    )


def _add_calendar(local: datetime, res: Resolution, steps: int) -> datetime:
    """Prague wall-clock ``local`` advanced by ``steps`` resolutions, as UTC."""
    naive = local.replace(tzinfo=None)
    total_months = (res.years * 12 + res.months) * steps
    if total_months:
        month_index = naive.year * 12 + (naive.month - 1) + total_months
        year, month = divmod(month_index, 12)
        naive = naive.replace(year=year, month=month + 1)
    if res.days:
        naive = naive + timedelta(days=res.days * steps)
    if res.minutes:
        naive = naive + timedelta(minutes=res.minutes * steps)
    return naive.replace(tzinfo=PRAGUE_TZ).astimezone(timezone.utc)


def position_bounds(
    period_start: datetime, period_end: datetime, res: Resolution
) -> list[tuple[datetime, datetime]]:
    """[start, end) UTC bounds of every position of a Period, in order.

    Position n (1-based) is element n-1 of the list. The last position is cut
    at ``period_end`` if the Period does not divide evenly.
    """
    bounds: list[tuple[datetime, datetime]] = []
    if res.is_calendar:
        local_start = period_start.astimezone(PRAGUE_TZ)
        step = 0
        while True:
            start = _add_calendar(local_start, res, step)
            if start >= period_end:
                break
            end = min(_add_calendar(local_start, res, step + 1), period_end)
            bounds.append((start, end))
            step += 1
    else:
        if res.minutes <= 0:
            raise ValueError(f"Resolution {res.text!r} has zero length")
        delta = timedelta(minutes=res.minutes)
        start = period_start
        while start < period_end:
            bounds.append((start, min(start + delta, period_end)))
            start += delta
    return bounds


@dataclass
class ParsedPoint:
    """One position of a Period, forward-filled where the curve is A03."""

    period_start: datetime
    period_end: datetime
    resolution: str
    quantity: Optional[float]
    price: Optional[float]


@dataclass
class ParsedSeries:
    """One TimeSeries with its raw attributes and expanded points."""

    mrid: Optional[str]
    in_eic: Optional[str]
    out_eic: Optional[str]
    business_type: Optional[str]
    contract_type: Optional[str]
    auction_type: Optional[str]
    auction_category: Optional[str]
    classification_sequence: Optional[str]
    curve_type: Optional[str]
    quantity_unit: Optional[str]     # raw ENTSO-E name, e.g. MAW
    price_unit: Optional[str]        # raw ENTSO-E name, e.g. EUR / EUR per MW
    currency: Optional[str]
    updated_at: Optional[datetime]
    points: list[ParsedPoint] = field(default_factory=list)


def _text(parent: ET.Element, tag: str) -> Optional[str]:
    elem = parent.find(f"{{*}}{tag}")
    if elem is None or elem.text is None:
        return None
    return elem.text.strip()


def _parse_dt(text: str) -> datetime:
    """ENTSO-E ISO timestamp ('2026-11-20T08:13:22Z', '2026-11-15T23:00Z') -> UTC."""
    text = text.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _float(text: Optional[str]) -> Optional[float]:
    if text is None or text == "":
        return None
    return float(text)


def _period_layout(
    period: ET.Element,
) -> tuple[dict[int, tuple[Optional[float], Optional[float]]], list[tuple[datetime, datetime]], str]:
    """Read one Period: its Points by position, the position bounds, the resolution text."""
    interval = period.find("{*}timeInterval")
    start = _parse_dt(interval.find("{*}start").text)
    end = _parse_dt(interval.find("{*}end").text)
    res_text = _text(period, "resolution")
    if res_text is None:
        raise ValueError("Period without resolution")
    bounds = position_bounds(start, end, parse_resolution(res_text))

    by_position: dict[int, tuple[Optional[float], Optional[float]]] = {}
    for point in period.findall("{*}Point"):
        position = int(_text(point, "position"))
        quantity = _float(_text(point, "quantity"))
        price = _float(_text(point, "price.amount"))
        if quantity is None and price is None:
            continue
        by_position[position] = (quantity, price)
    return by_position, bounds, res_text


def parse_capacity_document(xml_text: str) -> list[ParsedSeries]:
    """Parse one capacity document into its TimeSeries (see module docstring)."""
    root = ET.fromstring(xml_text)
    series_list: list[ParsedSeries] = []

    for ts in root.findall(".//{*}TimeSeries"):
        curve = _text(ts, "curveType")
        forward_fill = curve == "A03"
        update_text = _text(ts, "update_DateAndOrTime.dateTime")
        series = ParsedSeries(
            mrid=_text(ts, "mRID"),
            in_eic=_text(ts, "in_Domain.mRID"),
            out_eic=_text(ts, "out_Domain.mRID"),
            business_type=_text(ts, "businessType"),
            contract_type=_text(ts, "contract_MarketAgreement.type"),
            auction_type=_text(ts, "auction.type"),
            auction_category=_text(ts, "auction.category"),
            classification_sequence=_text(
                ts, "classificationSequence_AttributeInstanceComponent.position"
            ),
            curve_type=curve,
            quantity_unit=_text(ts, "quantity_Measure_Unit.name"),
            price_unit=_text(ts, "price_Measure_Unit.name"),
            currency=_text(ts, "currency_Unit.name"),
            updated_at=_parse_dt(update_text) if update_text else None,
        )

        for period in ts.findall("{*}Period"):
            by_position, bounds, res_text = _period_layout(period)
            last: Optional[tuple[Optional[float], Optional[float]]] = None
            for index, (p_start, p_end) in enumerate(bounds):
                position = index + 1
                if position in by_position:
                    last = by_position[position]
                    value = last
                elif forward_fill and last is not None:
                    value = last
                else:
                    continue
                series.points.append(
                    ParsedPoint(p_start, p_end, res_text, value[0], value[1])
                )
        series_list.append(series)

    return series_list


def merge_equal_runs(points: list[ParsedPoint]) -> list[ParsedPoint]:
    """Merge neighbouring points with equal values into one span.

    Used for long-term documents whose Period is hourly (PT60M) although the
    value changes only a few times a year (a month or a year forward-filled to
    hourly would be 720 or 8760 identical rows per border direction). The
    merged point keeps the source resolution text and covers
    ``[first.period_start, last.period_end)``; points are only merged when they
    touch (previous end == next start) and carry the same quantity and price.
    """
    merged: list[ParsedPoint] = []
    for point in points:
        last = merged[-1] if merged else None
        if (
            last is not None
            and last.period_end == point.period_start
            and last.resolution == point.resolution
            and last.quantity == point.quantity
            and last.price == point.price
        ):
            merged[-1] = ParsedPoint(
                last.period_start, point.period_end, last.resolution,
                last.quantity, last.price,
            )
        else:
            merged.append(point)
    return merged
