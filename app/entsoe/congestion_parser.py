"""Parser for ENTSO-E Congestion Income [12.1.E] documents (A25).

Publication_MarketDocument, documentType=A25, businessType=B10, queried per
bidding zone (in_Domain = out_Domain, CZ only today). Each TimeSeries has
auction.type A01, currency EUR, price_Measure_Unit MWH, curveType A03. The
value in <price.amount> is a congestion income AMOUNT in EUR for that MTU
(not a rate).

ENTSO-E publication documents are step functions: when a value is unchanged
from the previous position, the Point is omitted and the consumer is
expected to carry the previous value forward. This parser forward-fills
missing positions following the shape landed in commit b54c235 for the A44
day-ahead prices parser: a {position: value} map is built from the Points
actually present, num_intervals is derived from
(period_end - period_start) / resolution_minutes, and every expected
interval is walked while carrying the last seen value forward. last_value
starts as None so a genuinely absent leading Point stays NULL rather than
being fabricated as 0.

Resolution is PT15M from 2025-10-01 onward and PT60M before that (the Core
MTU change); a single response can contain several Period blocks (one per
delivery day) with differing resolution. A PT60M Period is expanded into 4
consecutive 15-minute rows; because congestion_income_eur is an EUR AMOUNT
per MTU (not a rate), each hourly value is DIVIDED BY 4 when expanded so the
four quarter-hour rows sum back to the published hourly amount.
source_resolution records the Period's native resolution ('PT15M' or
'PT60M') so downstream can distinguish expanded rows from native ones.

`period` is a convenience column derived from the local time-of-day; nothing
here assumes 96 periods per day (DST switch days have 92/100) and nothing
keys off it. (trade_date, time_interval) is the authoritative key.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import timedelta
from typing import Any, Dict, List, Optional

from entsoe.parsers import BaseParser

NATIVE_INTERVAL_MINUTES = 15


class CongestionIncomeParser(BaseParser):
    """Parses A25 Congestion Income XML into per-15-minute-interval rows."""

    def __init__(self, area_id: int, country_code: str):
        super().__init__()
        self.area_id = area_id
        self.country_code = country_code

    def parse_xml(self, xml_file_path: str) -> List[Dict[str, Any]]:
        """Read xml_file_path and delegate to parse_xml_content().

        Implements BaseParser's abstract parse_xml() so this class remains
        instantiable; the runner calls parse_xml_content() directly with the
        in-memory XML string returned by the client.
        """
        with open(xml_file_path, 'r', encoding='utf-8') as f:
            return self.parse_xml_content(f.read())

    def parse_xml_content(self, xml_string: str) -> List[Dict[str, Any]]:
        """Parse raw XML content (string) into a list of row dicts.

        Returns one dict per 15-minute interval, keyed by
        (trade_date, time_interval, area_id, country_code):
            trade_date, period, time_interval, delivery_datetime,
            area_id, country_code, congestion_income_eur, source_resolution
        """
        root = ET.fromstring(xml_string)

        # Namespace-agnostic element access, matching BaseParser's use of
        # '{*}' wildcard find() elsewhere in this codebase.
        rows_by_key: Dict[tuple, Dict[str, Any]] = {}

        for timeseries in root.findall('{*}TimeSeries'):
            for period in timeseries.findall('{*}Period'):
                self._process_period(period, rows_by_key)

        return list(rows_by_key.values())

    def _process_period(
        self, period: ET.Element, rows_by_key: Dict[tuple, Dict[str, Any]]
    ) -> None:
        """Expand a single Period element into 15-minute row dicts."""
        time_interval_el = period.find('{*}timeInterval')
        start_elem = time_interval_el.find('{*}start')
        end_elem = time_interval_el.find('{*}end')
        period_start = self.parse_timestamp(start_elem.text)
        period_end = self.parse_timestamp(end_elem.text)

        resolution_elem = period.find('{*}resolution')
        resolution = resolution_elem.text if resolution_elem is not None else 'PT60M'
        resolution_minutes = self.get_resolution_minutes(resolution)

        period_duration_minutes = int((period_end - period_start).total_seconds() / 60)
        num_intervals = period_duration_minutes // resolution_minutes

        # position -> raw published amount, for the Points actually present.
        points_by_position: Dict[int, Optional[float]] = {}
        for point in period.findall('{*}Point'):
            position = int(point.find('{*}position').text)
            amount_elem = point.find('{*}price.amount')
            points_by_position[position] = (
                float(amount_elem.text) if amount_elem is not None else None
            )

        # Walk every expected native interval, carrying the last seen amount
        # forward. Starts as None so a genuinely absent leading Point stays
        # NULL rather than being fabricated as 0.
        last_value: Optional[float] = None

        # How many 15-minute native rows the native resolution expands into,
        # and the EUR amount divisor applied per row (the published amount
        # is per-MTU; splitting a 60-minute MTU into 4x 15-minute rows must
        # divide the amount by 4 so the four rows sum back to the original).
        if resolution_minutes == NATIVE_INTERVAL_MINUTES:
            sub_intervals = 1
        elif resolution_minutes % NATIVE_INTERVAL_MINUTES == 0:
            sub_intervals = resolution_minutes // NATIVE_INTERVAL_MINUTES
        else:
            # Unexpected resolution - treat as a single native row (no split).
            sub_intervals = 1

        for interval_idx in range(num_intervals):
            position = interval_idx + 1
            if position in points_by_position:
                last_value = points_by_position[position]

            interval_start_utc = period_start + timedelta(
                minutes=interval_idx * resolution_minutes
            )

            row_amount = (
                last_value / sub_intervals if last_value is not None else None
            )

            for sub_idx in range(sub_intervals):
                sub_start_utc = interval_start_utc + timedelta(
                    minutes=sub_idx * NATIVE_INTERVAL_MINUTES
                )
                self._emit_row(
                    rows_by_key, sub_start_utc, row_amount, resolution
                )

    def _emit_row(
        self,
        rows_by_key: Dict[tuple, Dict[str, Any]],
        sub_start_utc,
        congestion_income_eur: Optional[float],
        source_resolution: str,
    ) -> None:
        """Build and store a single 15-minute row, keyed for de-duplication."""
        local_dt = self.convert_to_local_time(sub_start_utc)
        trade_date = local_dt.date()
        period_num = self.calculate_period_number(local_dt)
        time_interval = self.format_time_interval(local_dt, NATIVE_INTERVAL_MINUTES)
        # Naive local delivery timestamp (matches other ENTSO-E parsers'
        # convention of storing Prague-local wall-clock time without tzinfo).
        delivery_datetime = local_dt.replace(tzinfo=None)

        key = (trade_date, time_interval, self.area_id, self.country_code)

        if key not in rows_by_key:
            rows_by_key[key] = {
                'trade_date': trade_date,
                'period': period_num,
                'time_interval': time_interval,
                'delivery_datetime': delivery_datetime,
                'area_id': self.area_id,
                'country_code': self.country_code,
                'congestion_income_eur': congestion_income_eur,
                'source_resolution': source_resolution,
            }
        elif congestion_income_eur is not None:
            # Same slot seen twice (overlapping Periods across TimeSeries) -
            # prefer non-null, mirroring the day-ahead-prices parser.
            rows_by_key[key]['congestion_income_eur'] = congestion_income_eur
            rows_by_key[key]['source_resolution'] = source_resolution
