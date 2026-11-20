"""Parser for ENTSO-E per-border flow documents: physical flows (A11) and
scheduled commercial exchanges (A09).

Both are Publication_MarketDocument with one TimeSeries per direction
(in_Domain / out_Domain) and ``<quantity>`` points in MW. Values are
non-negative in the direction the source publishes them (``out_Domain`` ->
``in_Domain``); this parser neither nets nor sign-flips them.

ENTSO-E publication documents are step functions (curveType A03): when a value
equals the previous one, the Point is omitted. Every Period is therefore
walked position by position from 1 to the length implied by its timeInterval,
carrying the last seen value forward. ``last_value`` starts as None, so a
genuinely absent leading Point stays absent (no row) instead of becoming 0.

Resolutions PT15M and PT60M are supported; a PT60M Period is expanded into four
15-minute rows carrying the same MW value (power, so no division). A response
may hold several TimeSeries and several Periods (one per delivery day, with
different resolutions); a finer resolution wins when two Periods cover the same
15-minute slot, otherwise the later Period wins.

``period`` counts 15-minute slots from Prague local midnight (1..92/96/100) and
``time_interval`` is the descriptive local label; ``delivery_datetime`` (the UTC
start instant) is the authoritative key and is unique even in the repeated
fall-back hour.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from entsoe.parsers import BaseParser

NATIVE_INTERVAL_MINUTES = 15

logger = logging.getLogger(__name__)


class BorderFlowParser(BaseParser):
    """Parses one border direction's A11 / A09 XML into 15-minute value rows.

    Args:
        from_area_id / from_eic: the sending area (ENTSO-E ``out_Domain``).
        to_area_id / to_eic: the receiving area (ENTSO-E ``in_Domain``).

    TimeSeries whose domains do not match this direction are skipped and
    counted in ``skipped_series`` (a request asks for exactly one direction, so
    a mismatch means the source answered something else than was asked).
    """

    def __init__(self, from_area_id: int, from_eic: str, to_area_id: int, to_eic: str):
        super().__init__()
        self.from_area_id = from_area_id
        self.from_eic = from_eic
        self.to_area_id = to_area_id
        self.to_eic = to_eic
        self.skipped_series = 0

    def parse_xml(self, xml_file_path: str) -> List[Dict[str, Any]]:
        """Read a file and delegate to :meth:`parse_xml_content`."""
        with open(xml_file_path, "r", encoding="utf-8") as f:
            return self.parse_xml_content(f.read())

    def parse_xml_content(self, xml_string: str) -> List[Dict[str, Any]]:
        """Parse raw XML into one dict per 15-minute slot, sorted by time.

        Each dict: ``delivery_datetime`` (UTC), ``from_area_id``,
        ``to_area_id``, ``trade_date``, ``period``, ``time_interval``,
        ``value`` (Decimal MW).
        """
        root = ET.fromstring(xml_string)
        # key: delivery_datetime -> (value, source resolution minutes)
        slots: Dict[datetime, Tuple[Decimal, int]] = {}

        for series in root.findall(".//{*}TimeSeries"):
            in_el = series.find("{*}in_Domain.mRID")
            out_el = series.find("{*}out_Domain.mRID")
            in_eic = in_el.text.strip() if in_el is not None and in_el.text else None
            out_eic = out_el.text.strip() if out_el is not None and out_el.text else None
            if in_eic != self.to_eic or out_eic != self.from_eic:
                self.skipped_series += 1
                logger.debug(
                    "Skipping TimeSeries %s -> %s (expected %s -> %s)",
                    out_eic, in_eic, self.from_eic, self.to_eic,
                )
                continue
            for period in series.findall("{*}Period"):
                self._process_period(period, slots)

        rows = []
        for delivery_datetime in sorted(slots):
            local_dt = self.convert_to_local_time(delivery_datetime)
            rows.append({
                "delivery_datetime": delivery_datetime,
                "from_area_id": self.from_area_id,
                "to_area_id": self.to_area_id,
                "trade_date": local_dt.date(),
                "period": self.calculate_period_number(delivery_datetime),
                "time_interval": self.format_time_interval(
                    delivery_datetime, NATIVE_INTERVAL_MINUTES
                ),
                "value": slots[delivery_datetime][0],
            })
        return rows

    def _process_period(
        self, period: ET.Element, slots: Dict[datetime, Tuple[Decimal, int]]
    ) -> None:
        """Expand one Period (forward-filled) into 15-minute slots."""
        interval = period.find("{*}timeInterval")
        period_start = self.parse_timestamp(interval.find("{*}start").text)
        period_end = self.parse_timestamp(interval.find("{*}end").text)

        resolution_el = period.find("{*}resolution")
        resolution = resolution_el.text if resolution_el is not None else "PT15M"
        resolution_minutes = self.get_resolution_minutes(resolution)
        if resolution_minutes % NATIVE_INTERVAL_MINUTES != 0:
            raise ValueError(f"Unsupported resolution {resolution!r}")
        sub_intervals = resolution_minutes // NATIVE_INTERVAL_MINUTES

        duration_minutes = int((period_end - period_start).total_seconds() / 60)
        num_intervals = duration_minutes // resolution_minutes

        points: Dict[int, Optional[Decimal]] = {}
        for point in period.findall("{*}Point"):
            position = int(point.find("{*}position").text)
            quantity = point.find("{*}quantity")
            points[position] = (
                Decimal(quantity.text.strip())
                if quantity is not None and quantity.text
                else None
            )

        last_value: Optional[Decimal] = None
        for idx in range(num_intervals):
            position = idx + 1
            if position in points:
                last_value = points[position]
            if last_value is None:
                continue
            start = period_start + timedelta(minutes=idx * resolution_minutes)
            for sub in range(sub_intervals):
                slot_start = start + timedelta(minutes=sub * NATIVE_INTERVAL_MINUTES)
                current = slots.get(slot_start)
                # Finer (or equal, later) resolution wins an overlap.
                if current is None or resolution_minutes <= current[1]:
                    slots[slot_start] = (last_value, resolution_minutes)
