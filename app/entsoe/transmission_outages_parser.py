"""Parser for ENTSO-E transmission infrastructure unavailability (A78).

One XML file is one ``Unavailability_MarketDocument``: document identity
(mRID, revisionNumber, docStatus, createdDateTime) wrapping one or more
TimeSeries, each describing one affected asset (line, transformer, ...) on one
border direction. ``out_Domain`` -> ``in_Domain`` is the direction the reduced
capacity applies to.

``Available_Period`` carries the remaining cross-border capacity in MW (the
"new NTC") while the outage lasts. With ``curveType`` A03 the Points are a step
function: a Point holds from its position until the next Point (or the end of
the period). Positions count in units of the period's resolution (PT1M for the
documents seen). Some documents (50Hertz) carry no Available_Period at all: the
outage is still returned, with no points and ``min_capacity_mw`` None.

Differences to ``outages_parser.py`` (A77/A80 generation outages): domains
instead of a bidding zone, ``Asset_RegisteredResource`` instead of
``production_RegisteredResource``, capacity (not availability of a unit with a
nominal power), and curve points are returned as validity segments
``(point_start, point_end, capacity_mw)`` rather than one row per position.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Column widths of entsoe_transmission_outages (migration 090).
_MAX_LEN = {
    "doc_mrid": 60, "timeseries_mrid": 60, "business_type": 3, "doc_status": 3,
    "asset_mrid": 60, "asset_name": 200, "asset_location": 200, "asset_type": 10,
    "quantity_unit": 10, "curve_type": 3, "reason_code": 10,
}


def _clip(column: str, value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    limit = _MAX_LEN.get(column)
    return value[:limit] if limit else value


def _txt(el: Optional[ET.Element], path: str) -> Optional[str]:
    """Text of the first element matching ``path`` (namespace-agnostic), stripped."""
    if el is None:
        return None
    found = el.find(path)
    if found is None or found.text is None:
        return None
    text = found.text.strip()
    return text or None


def _dt(iso: Optional[str]) -> Optional[datetime]:
    if not iso:
        return None
    value = iso.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _combine(date_str: Optional[str], time_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    return _dt(f"{date_str}T{time_str or '00:00:00Z'}")


def _resolution_minutes(resolution: Optional[str]) -> int:
    """PT1M / PT15M / PT60M / PT1H -> minutes; anything else raises."""
    if resolution and resolution.startswith("PT") and resolution[-1] in "MH":
        value = int(resolution[2:-1])
        return value * 60 if resolution[-1] == "H" else value
    raise ValueError(f"unsupported resolution {resolution!r}")


def _period_segments(
    period: ET.Element, curve_type: Optional[str]
) -> List[Tuple[datetime, datetime, float]]:
    """Validity segments ``(start, end, capacity_mw)`` of one Available_Period."""
    interval = period.find("{*}timeInterval")
    start = _dt(_txt(interval, "{*}start"))
    end = _dt(_txt(interval, "{*}end"))
    if start is None or end is None:
        return []
    step = timedelta(minutes=_resolution_minutes(_txt(period, "{*}resolution")))

    points: List[Tuple[int, float]] = []
    for point in period.findall("{*}Point"):
        position = _txt(point, "{*}position")
        quantity = _txt(point, "{*}quantity")
        if position is not None and quantity is not None:
            points.append((int(position), float(quantity)))
    points.sort()

    segments: List[Tuple[datetime, datetime, float]] = []
    for index, (position, quantity) in enumerate(points):
        seg_start = start + (position - 1) * step
        if seg_start >= end:
            continue
        if curve_type == "A03" or curve_type is None:
            # Step function: holds until the next Point or the end of the period.
            seg_end = start + (points[index + 1][0] - 1) * step if index + 1 < len(points) else end
        else:
            # Fixed sequential blocks: each Point covers exactly one resolution step.
            seg_end = seg_start + step
        segments.append((seg_start, min(seg_end, end), quantity))
    return segments


def parse_transmission_outage_document(
    xml_text: str,
    area_by_eic: Dict[str, int],
    fallback_direction: Optional[Tuple[int, int]] = None,
) -> List[Dict[str, Any]]:
    """Parse one A78 document into outage rows (one per TimeSeries).

    Args:
        xml_text: The ``Unavailability_MarketDocument`` XML.
        area_by_eic: EIC -> ``entsoe_areas.id`` for the areas that may occur.
        fallback_direction: ``(from_area_id, to_area_id)`` used when a
            TimeSeries' own domains are missing or unknown (the direction the
            document was requested for).

    Returns:
        A list of dicts with the ``entsoe_transmission_outages`` columns
        (without ``id``/audit columns) and a ``points`` key holding the
        ``(point_start, point_end, capacity_mw)`` segments. ``from_area_id`` is
        taken from the TimeSeries' ``out_Domain`` and ``to_area_id`` from its
        ``in_Domain`` (energy/capacity flows out -> in). TimeSeries whose
        direction cannot be resolved, or whose start/end are missing, are
        skipped with a warning.
    """
    root = ET.fromstring(xml_text)

    doc_mrid = _txt(root, "{*}mRID")
    revision = int(_txt(root, "{*}revisionNumber") or 0)
    created = _dt(_txt(root, "{*}createdDateTime"))
    # docStatus is omitted for active records; store the explicit code (A05),
    # like outages_parser.py does for generation outages.
    doc_status = _txt(root, "{*}docStatus/{*}value") or "A05"
    doc_reason_code = _txt(root, "{*}Reason/{*}code")
    doc_reason_text = _txt(root, "{*}Reason/{*}text")

    rows: List[Dict[str, Any]] = []
    for ts in root.findall("{*}TimeSeries"):
        ts_mrid = _txt(ts, "{*}mRID")
        in_eic = _txt(ts, "{*}in_Domain.mRID")
        out_eic = _txt(ts, "{*}out_Domain.mRID")
        from_id = area_by_eic.get(out_eic) if out_eic else None
        to_id = area_by_eic.get(in_eic) if in_eic else None
        if (from_id is None or to_id is None) and fallback_direction is not None:
            from_id, to_id = fallback_direction
        if from_id is None or to_id is None or from_id == to_id:
            logger.warning("A78 %s/%s: unresolved direction (in=%s out=%s), skipped",
                           doc_mrid, ts_mrid, in_eic, out_eic)
            continue

        unavail_start = _combine(_txt(ts, "{*}start_DateAndOrTime.date"),
                                 _txt(ts, "{*}start_DateAndOrTime.time"))
        unavail_end = _combine(_txt(ts, "{*}end_DateAndOrTime.date"),
                               _txt(ts, "{*}end_DateAndOrTime.time"))
        if unavail_start is None or unavail_end is None or doc_mrid is None or ts_mrid is None:
            logger.warning("A78 %s/%s: missing identity or period, skipped", doc_mrid, ts_mrid)
            continue

        curve_type = _txt(ts, "{*}curveType")
        segments: Dict[datetime, Tuple[datetime, datetime, float]] = {}
        for period in ts.findall("{*}Available_Period"):
            for seg in _period_segments(period, curve_type):
                segments[seg[0]] = seg
        points = [segments[k] for k in sorted(segments)]
        min_capacity = min((p[2] for p in points), default=None)

        asset = ts.find("{*}Asset_RegisteredResource")
        rows.append({
            "doc_mrid": _clip("doc_mrid", doc_mrid),
            "revision_number": revision,
            "timeseries_mrid": _clip("timeseries_mrid", ts_mrid),
            "from_area_id": from_id,
            "to_area_id": to_id,
            "business_type": _clip("business_type", _txt(ts, "{*}businessType")),
            "doc_status": _clip("doc_status", doc_status),
            "created_datetime": created,
            "asset_mrid": _clip("asset_mrid", _txt(asset, "{*}mRID")),
            "asset_name": _clip("asset_name", _txt(asset, "{*}name")),
            "asset_location": _clip("asset_location", _txt(asset, "{*}location.name")),
            "asset_type": _clip("asset_type", _txt(asset, "{*}asset_PSRType.psrType")),
            "unavail_start": unavail_start,
            "unavail_end": unavail_end,
            "quantity_unit": _clip("quantity_unit", _txt(ts, "{*}quantity_Measure_Unit.name")),
            "curve_type": _clip("curve_type", curve_type),
            "min_capacity_mw": min_capacity,
            # Reason may sit inside the TimeSeries or at document level.
            "reason_code": _clip("reason_code", _txt(ts, "{*}Reason/{*}code") or doc_reason_code),
            "reason_text": _txt(ts, "{*}Reason/{*}text") or doc_reason_text,
            "points": points,
        })
    return rows
