"""Parser for ENTSO-E outage documents (A77 production unit unavailability).

Outage documents are EVENT-based, not a 96-period time series. Each XML file is
one Unavailability_MarketDocument: document-level identity (mRID, revisionNumber,
docStatus, createdDateTime) wrapping one or more TimeSeries (the affected asset)
each with an Available_Period curve of *available* MW (unavailable = nominal-available).

parse_document(xml_bytes) -> (events: list[dict], points: list[dict])
where events feed entsoe_outages and points feed entsoe_outage_points.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

NS = {"o": "urn:iec62325.351:tc57wg16:451-6:outagedocument:3:0"}


def _txt(el: ET.Element, path: str) -> Optional[str]:
    f = el.find(path, NS)
    return f.text if f is not None else None


def _dt(iso: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 instant (UTC 'Z') into a tz-aware datetime."""
    if not iso:
        return None
    return datetime.fromisoformat(iso.strip().replace("Z", "+00:00"))


def _combine(date_str: Optional[str], time_str: Optional[str]) -> Optional[datetime]:
    """Combine start_DateAndOrTime.date + .time into a tz-aware datetime."""
    if not date_str:
        return None
    return _dt(f"{date_str}T{time_str or '00:00:00Z'}")


def _num(s: Optional[str]) -> Optional[float]:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_document(
    xml_bytes: bytes, area_id: int, country_code: str
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Parse one outage XML document into event rows + curve point rows."""
    root = ET.fromstring(xml_bytes)

    doc_mrid = _txt(root, "o:mRID")
    revision = int(_txt(root, "o:revisionNumber") or 0)
    doc_type = _txt(root, "o:type")
    process_type = _txt(root, "o:process.processType")
    created = _dt(_txt(root, "o:createdDateTime"))
    # ENTSO-E omits docStatus for active records; normalize absent -> 'A05' (Active)
    # so every row carries an explicit, ML-friendly status code (A05/A09/A13).
    doc_status = _txt(root, "o:docStatus/o:value") or "A05"
    # Reason may sit at document level (most common) or per-TimeSeries.
    doc_reason_code = _txt(root, "o:Reason/o:code")
    doc_reason_text = _txt(root, "o:Reason/o:text")

    events: List[Dict[str, Any]] = []
    points: List[Dict[str, Any]] = []

    for ts in root.findall("o:TimeSeries", NS):
        ts_mrid = _txt(ts, "o:mRID")
        nominal = _num(_txt(ts, "o:production_RegisteredResource.pSRType.powerSystemResources.nominalP"))

        # walk the Available_Period curve(s); position is made unique across the
        # whole TimeSeries so it can be part of the points PK.
        pos = 0
        min_available: Optional[float] = None
        for ap in ts.findall("o:Available_Period", NS):
            p_start = _dt(_txt(ap, "o:timeInterval/o:start"))
            p_end = _dt(_txt(ap, "o:timeInterval/o:end"))
            resolution = _txt(ap, "o:resolution")
            for pt in ap.findall("o:Point", NS):
                pos += 1
                avail = _num(_txt(pt, "o:quantity"))
                if avail is not None:
                    min_available = avail if min_available is None else min(min_available, avail)
                points.append({
                    "doc_mrid": doc_mrid,
                    "revision_number": revision,
                    "timeseries_mrid": ts_mrid,
                    "area_id": area_id,
                    "country_code": country_code,
                    "point_start": p_start,
                    "point_end": p_end,
                    "resolution": resolution,
                    "position": pos,
                    "available_mw": avail,
                })

        max_unavailable = (
            nominal - min_available
            if (nominal is not None and min_available is not None)
            else None
        )

        events.append({
            "doc_mrid": doc_mrid,
            "revision_number": revision,
            "timeseries_mrid": ts_mrid,
            "doc_type": doc_type,
            "business_type": _txt(ts, "o:businessType"),
            "doc_status": doc_status,
            "process_type": process_type,
            "created_datetime": created,
            "area_id": area_id,
            "country_code": country_code,
            "biddingzone_domain": _txt(ts, "o:biddingZone_Domain.mRID"),
            "production_resource_mrid": _txt(ts, "o:production_RegisteredResource.mRID"),
            "production_resource_name": _txt(ts, "o:production_RegisteredResource.name"),
            "location_name": _txt(ts, "o:production_RegisteredResource.location.name"),
            "psr_type": _txt(ts, "o:production_RegisteredResource.pSRType.psrType"),
            "power_system_resource_mrid": _txt(ts, "o:production_RegisteredResource.pSRType.powerSystemResources.mRID"),
            "power_system_resource_name": _txt(ts, "o:production_RegisteredResource.pSRType.powerSystemResources.name"),
            "nominal_power_mw": nominal,
            "quantity_unit": _txt(ts, "o:quantity_Measure_Unit.name"),
            "curve_type": _txt(ts, "o:curveType"),
            "unavail_start": _combine(_txt(ts, "o:start_DateAndOrTime.date"), _txt(ts, "o:start_DateAndOrTime.time")),
            "unavail_end": _combine(_txt(ts, "o:end_DateAndOrTime.date"), _txt(ts, "o:end_DateAndOrTime.time")),
            "min_available_mw": min_available,
            "max_unavailable_mw": max_unavailable,
            "reason_code": _txt(ts, "o:Reason/o:code") or doc_reason_code,
            "reason_text": _txt(ts, "o:Reason/o:text") or doc_reason_text,
        })

    return events, points
