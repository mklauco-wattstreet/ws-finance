#!/usr/bin/env python3
"""
Base runner for ENTSO-E Generation Forecast (A69) pipelines.

Shared logic for Day-Ahead (A01), Intraday (A40), and Current (A18)
generation forecast runners. Each subclass defines:
- RUNNER_NAME, TABLE_NAME, PROCESS_TYPE
- run() with its own scheduling/availability logic
"""

import sys
from pathlib import Path
from typing import List, Tuple
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from entsoe.client import EntsoeClient
from entsoe.parsers import GenerationForecastParser
from entsoe.constants import ACTIVE_GENERATION_AREAS, ACTIVE_CURRENT_FORECAST_AREAS, FORECAST_PROCESS_TYPES


class BaseForecastRunner(BaseRunner):
    """Base class for all A69 generation forecast runners."""

    # Subclasses must override
    RUNNER_NAME = "BaseForecastRunner"
    TABLE_NAME = ""
    PROCESS_TYPE = ""
    ACTIVE_AREAS = ACTIVE_GENERATION_AREAS

    COLUMNS = [
        "trade_date", "period", "area_id", "country_code", "time_interval",
        "forecast_solar_mw", "forecast_wind_mw", "forecast_wind_offshore_mw"
    ]
    CONFLICT_COLUMNS = ["trade_date", "period", "area_id", "country_code"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = None

    def _init_client(self) -> bool:
        self.logger.debug("Initializing ENTSO-E client...")
        try:
            self.client = EntsoeClient()
            self.logger.debug("Client initialized")
            return True
        except Exception as e:
            self.logger.error(f"Client initialization failed: {e}")
            return False

    def _fetch_data_for_area(self, period_start, period_end, area_code: str) -> str:
        xml_content = self.client.fetch_generation_forecast_for_domain(
            period_start, period_end, in_domain=area_code,
            process_type=self.PROCESS_TYPE
        )
        return xml_content

    def _save_xml_file(self, xml_content: str, period_start, period_end, label: str) -> Path:
        start_str = period_start.strftime('%Y%m%d%H%M')
        end_str = period_end.strftime('%Y%m%d%H%M')
        proc_label = FORECAST_PROCESS_TYPES.get(self.PROCESS_TYPE, self.PROCESS_TYPE).lower().replace("-", "")
        output_file = self.get_output_path(
            f'entsoe_gen_forecast_{proc_label}_{label.lower()}_{start_str}_{end_str}.xml',
            period_start
        )
        self.save_xml(xml_content, output_file)
        self.logger.debug(f"  Saved: {output_file.name}")
        return output_file

    def _parse_data(self, xml_file: Path, area_id: int, country_code: str) -> List[dict]:
        parser = GenerationForecastParser(area_id=area_id, country_code=country_code)
        return parser.parse_xml(str(xml_file))

    def _prepare_records(self, data: List[dict]) -> List[Tuple]:
        records = []
        for record in data:
            records.append((
                record['trade_date'],
                record['period'],
                record['area_id'],
                record['country_code'],
                record['time_interval'],
                record.get('forecast_solar_mw'),
                record.get('forecast_wind_mw'),
                record.get('forecast_wind_offshore_mw')
            ))
        return records

    def _process_area(self, period_start, period_end,
                      area_id: int, area_code: str, display_label: str,
                      country_code: str, conn=None) -> int:
        self.logger.debug(f"  Fetching {display_label} (area_id={area_id}, country={country_code})...")
        try:
            xml_content = self._fetch_data_for_area(period_start, period_end, area_code)
            self.logger.debug(f"    Received {len(xml_content)} bytes")

            xml_file = self._save_xml_file(xml_content, period_start, period_end, display_label)
            data = self._parse_data(xml_file, area_id, country_code)

            if not data:
                self.logger.info(f"{self.RUNNER_NAME}: no data for {country_code}")
                return 0

            self.logger.debug(f"    Parsed {len(data)} records")
            records = self._prepare_records(data)

            if not self.dry_run and conn:
                self.bulk_upsert(conn, self.TABLE_NAME, self.COLUMNS, records, self.CONFLICT_COLUMNS)
            elif self.dry_run:
                self.logger.info(f"    DRY RUN - Would upload {len(records)} records")

            self.track_country(country_code, len(records))
            return len(records)

        except Exception as e:
            if self.is_data_unavailable_error(e):
                self.logger.info(
                    f"{self.RUNNER_NAME}: {country_code} not available for "
                    f"[{period_start.strftime('%Y-%m-%d %H:%M')}-{period_end.strftime('%Y-%m-%d %H:%M')}]"
                )
            else:
                self.logger.error(f"  Failed {country_code}: {e}")
                if self.debug:
                    import traceback
                    traceback.print_exc()
            return 0

    def _process_chunk(self, period_start, period_end, conn=None) -> int:
        self.logger.debug(
            f"Processing: {period_start.strftime('%Y-%m-%d %H:%M')} "
            f"to {period_end.strftime('%Y-%m-%d %H:%M')} UTC"
        )
        total_records = 0
        for area_id, area_code, display_label, country_code in self.ACTIVE_AREAS:
            records = self._process_area(
                period_start, period_end,
                area_id, area_code, display_label, country_code, conn
            )
            total_records += records
        return total_records

    def _data_exists_for_date(self, conn, target_date, area_id: int, country_code: str) -> bool:
        """Check if data already exists for a given date/area. Uses PK index + partition pruning."""
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT 1 FROM {self.TABLE_NAME} "
                f"WHERE trade_date = %s AND area_id = %s AND country_code = %s LIMIT 1",
                (target_date, area_id, country_code)
            )
            return cur.fetchone() is not None
        finally:
            cur.close()

    def _run_backfill(self) -> bool:
        """Standard chunked backfill through all areas."""
        self.logger.debug(f"Processing {len(self.ACTIVE_AREAS)} areas: "
                          f"{', '.join(label for _, _, label, _ in self.ACTIVE_AREAS)}")
        total_records = 0
        with self.database_connection() as conn:
            for period_start, period_end in self.get_backfill_chunks():
                try:
                    records = self._process_chunk(period_start, period_end, conn)
                    total_records += records
                except Exception as e:
                    self.logger.error(f"Chunk failed: {e}")
                    if self.debug:
                        import traceback
                        traceback.print_exc()
                    continue
        return total_records

    def _run_with_availability_check(self, target_date) -> int:
        """For A01/A40: check DB per area, fetch only if missing."""
        # Convert target_date to UTC range (midnight-to-midnight Prague → UTC)
        start_prague = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=PRAGUE_TZ)
        end_prague = start_prague + timedelta(days=1)
        period_start = start_prague.astimezone(timezone.utc)
        period_end = end_prague.astimezone(timezone.utc)

        total_records = 0
        with self.database_connection() as conn:
            for area_id, area_code, display_label, country_code in self.ACTIVE_AREAS:
                if self._data_exists_for_date(conn, target_date, area_id, country_code):
                    self.logger.debug(f"  {display_label}: data exists for {target_date}, skipping")
                    continue

                records = self._process_area(
                    period_start, period_end,
                    area_id, area_code, display_label, country_code, conn
                )
                total_records += records

        return total_records

    def _run_continuous(self, period_start, period_end) -> int:
        """For A18: always fetch, no DB check."""
        total_records = 0
        if not self.dry_run:
            with self.database_connection() as conn:
                total_records = self._process_chunk(period_start, period_end, conn)
        else:
            total_records = self._process_chunk(period_start, period_end)
        return total_records
