"""Parser for ENTSO-E Intraday Offered Transfer Capacity (A31, 12.1.A/B).

CZ-border capacity is fetched one HTTP call per (product, border, direction) -
8 border-direction calls per product (4 neighbours x import/export). This
parser accumulates those per-call responses into a single wide-format table
keyed by delivery timestamp, mirroring CrossBorderFlowsParser in parsers.py.

Direction convention (matches CZ_CAPACITY_BORDERS in constants.py and
migration 076):
    in_Domain=CZ,        out_Domain=<neighbour> -> cap_import_<nb>_mw
    in_Domain=<neighbour>, out_Domain=CZ        -> cap_export_<nb>_mw
in_Domain is always the RECEIVING zone; "import" means capacity flowing INTO
CZ, "export" means capacity flowing OUT OF CZ.

Forward-fill: ENTSO-E's A31 curveType A03 omits a Point when its value is
unchanged from the previous position. Each Period is walked position-by-
position, carrying the last seen quantity forward; a genuinely absent
*leading* Point (no quantity seen yet) is left NULL rather than fabricated as
0. This is the same pattern used across the other nine parsers (see commit
b54c235 for the reference implementation on the day-ahead prices parser and
bf01943 for its rollout to the rest).

idct (auction.Type=A08) TimeSeries carry <update_DateAndOrTime.dateTime>,
reflecting continuous revisions during the delivery day; ida1/ida2/ida3
(auction.Type=A01) do not, since those documents are static once published.
published_at is the max update timestamp observed across all border
responses contributing to a given delivery timestamp (None for ida*).
"""
from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from entsoe.constants import CZ_CAPACITY_BORDERS
from entsoe.parsers import BaseParser

logger = logging.getLogger(__name__)

# Column suffix -> neighbour EIC (de/at/pl/sk), reused from constants.py so
# this module never hardcodes an EIC code of its own.
BORDER_SUFFIXES = tuple(CZ_CAPACITY_BORDERS.keys())


class IntradayTransferCapacityParser(BaseParser):
    """Wide-format parser for CZ intraday offered transfer capacity.

    Usage: call parse_xml_content() once per border-direction XML response for
    a single product, then call get_wide_format_data(product) to obtain the
    final rows. Call clear() (or construct a new instance) before reusing for
    a different product.
    """

    def __init__(self, area_id: int, country_code: str):
        super().__init__()
        self.area_id = area_id
        self.country_code = country_code
        # delivery_datetime (Prague local, naive) -> {column: value}
        self._wide_data: Dict[datetime, Dict[str, float]] = {}
        # delivery_datetime -> max update_DateAndOrTime.dateTime seen (tz-aware UTC)
        self._published_at: Dict[datetime, datetime] = {}

    # BaseParser is an ABC requiring parse_xml(); this parser works on
    # in-memory XML strings from EntsoeClient instead (parse_xml_content()),
    # so parse_xml() itself is unused.
    def parse_xml(self, xml_file_path: str) -> List[Dict[str, Any]]:
        raise NotImplementedError(
            "IntradayTransferCapacityParser uses parse_xml_content(), not parse_xml()"
        )

    def parse_xml_content(self, xml_string: Optional[str], border_key: str, direction: str) -> None:
        """Parse one border-direction A31 response and accumulate it.

        Args:
            xml_string: raw XML response body (already unzipped by
                EntsoeClient), or None (e.g. "no matching data" - no-op).
            border_key: one of 'de', 'at', 'pl', 'sk' (column suffix).
            direction: 'import' (capacity INTO CZ) or 'export' (capacity OUT
                OF CZ).
        """
        if not xml_string:
            return
        if border_key not in BORDER_SUFFIXES:
            logger.warning(f"Unknown border_key={border_key!r}, skipping")
            return
        if direction not in ("import", "export"):
            raise ValueError(f"direction must be 'import' or 'export', got {direction!r}")

        column = f"cap_{direction}_{border_key}_mw"

        root = ET.fromstring(xml_string)

        # A single response may contain several TimeSeries/Period blocks, one
        # per delivery day (e.g. a 2-day default window).
        for timeseries in root.findall('.//{*}TimeSeries'):
            update_elem = timeseries.find('{*}update_DateAndOrTime.dateTime')
            update_dt = (
                self.parse_timestamp(update_elem.text)
                if update_elem is not None and update_elem.text
                else None
            )

            for period in timeseries.findall('{*}Period'):
                self._process_period(period, column, update_dt)

    def _process_period(self, period: ET.Element, column: str, update_dt: Optional[datetime]) -> None:
        """Forward-fill one Period's Points into self._wide_data for `column`."""
        time_interval = period.find('{*}timeInterval')
        period_start = self.parse_timestamp(time_interval.find('{*}start').text)
        period_end = self.parse_timestamp(time_interval.find('{*}end').text)

        resolution_elem = period.find('{*}resolution')
        resolution = resolution_elem.text if resolution_elem is not None else 'PT15M'
        resolution_minutes = self.get_resolution_minutes(resolution)

        period_duration_minutes = int((period_end - period_start).total_seconds() / 60)
        num_intervals = period_duration_minutes // resolution_minutes

        points_by_position: Dict[int, float] = {}
        for point in period.findall('{*}Point'):
            position = int(point.find('{*}position').text)
            quantity_elem = point.find('{*}quantity')
            if quantity_elem is not None and quantity_elem.text is not None:
                points_by_position[position] = float(quantity_elem.text)

        # Starts as None so a genuinely absent leading Point stays NULL rather
        # than being fabricated as 0.
        last_value: Optional[float] = None

        for interval_idx in range(num_intervals):
            position = interval_idx + 1
            if position in points_by_position:
                last_value = points_by_position[position]
            if last_value is None:
                continue

            point_time_utc = period_start + timedelta(minutes=interval_idx * resolution_minutes)
            point_time_local = self.convert_to_local_time(point_time_utc)
            delivery_datetime = point_time_local.replace(tzinfo=None)

            self._wide_data.setdefault(delivery_datetime, {})[column] = last_value

            if update_dt is not None:
                existing = self._published_at.get(delivery_datetime)
                if existing is None or update_dt > existing:
                    self._published_at[delivery_datetime] = update_dt

    def get_wide_format_data(self, product: str) -> List[Dict[str, Any]]:
        """Build the final wide-format records for one product.

        Args:
            product: 'idct' | 'ida1' | 'ida2' | 'ida3' - stored verbatim in
                the `product` column.

        Returns:
            List of records, one per delivery timestamp, sorted ascending.
        """
        result: List[Dict[str, Any]] = []

        for delivery_datetime in sorted(self._wide_data.keys()):
            cols = self._wide_data[delivery_datetime]

            record: Dict[str, Any] = {
                'trade_date': delivery_datetime.date(),
                'period': self.calculate_period_number(delivery_datetime),
                'time_interval': self.format_time_interval(delivery_datetime, 15),
                'delivery_datetime': delivery_datetime,
                'area_id': self.area_id,
                'country_code': self.country_code,
                'product': product,
            }

            total_import = 0.0
            has_import = False
            total_export = 0.0
            has_export = False

            for suffix in BORDER_SUFFIXES:
                import_col = f"cap_import_{suffix}_mw"
                export_col = f"cap_export_{suffix}_mw"
                import_val = cols.get(import_col)
                export_val = cols.get(export_col)
                record[import_col] = import_val
                record[export_col] = export_val
                if import_val is not None:
                    total_import += import_val
                    has_import = True
                if export_val is not None:
                    total_export += export_val
                    has_export = True

            # NULL-safe totals: skip missing borders, only NULL the total when
            # every border on that side is missing.
            record['cap_import_total_mw'] = total_import if has_import else None
            record['cap_export_total_mw'] = total_export if has_export else None
            record['published_at'] = self._published_at.get(delivery_datetime)

            result.append(record)

        return result

    def clear(self) -> None:
        """Clear intermediate data for reuse."""
        self._wide_data.clear()
        self._published_at.clear()
