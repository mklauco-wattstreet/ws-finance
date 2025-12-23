#!/usr/bin/env python3
"""
ENTSO-E Unified Load Data Runner.

Fetches actual load (A65/A16) and day-ahead forecast (A65/A01) data for ALL active areas,
parses the XML into records with area_id, and uploads to the partitioned entsoe_load table.

Active areas (from entsoe_areas table):
- CZ (Czech Republic) - area_id=1
- DE (Germany TenneT) - area_id=2
- AT (Austria) - area_id=3
- PL (Poland) - area_id=4
- SK (Slovakia) - area_id=5

Usage:
    python3 entsoe_unified_load_runner.py [--debug] [--dry-run]
    python3 entsoe_unified_load_runner.py --start 2024-12-01 --end 2024-12-22
"""

import sys
from pathlib import Path
from typing import List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner
from entsoe.client import EntsoeClient
from entsoe.parsers import LoadParser
from entsoe.constants import ACTIVE_GENERATION_AREAS


class UnifiedLoadRunner(BaseRunner):
    """Unified runner for ENTSO-E Load data (A65) - All Areas."""

    RUNNER_NAME = "ENTSO-E Unified Load Runner"

    # Table configuration - partitioned by country_code
    TABLE_NAME = "entsoe_load"
    COLUMNS = [
        "trade_date", "period", "area_id", "country_code", "time_interval",
        "actual_load_mw", "forecast_load_mw"
    ]
    CONFLICT_COLUMNS = ["trade_date", "period", "area_id", "country_code"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = None

    def _init_client(self) -> bool:
        """Initialize ENTSO-E client."""
        self.logger.info("Initializing ENTSO-E client...")
        try:
            self.client = EntsoeClient()
            self.logger.info("✓ Client initialized")
            return True
        except Exception as e:
            self.logger.error(f"✗ Client initialization failed: {e}")
            return False

    def _fetch_data_for_area(self, period_start, period_end, area_code: str) -> Tuple[str, str]:
        """
        Fetch actual load and forecast load XML for a specific area.

        Args:
            period_start: Start datetime
            period_end: End datetime
            area_code: EIC code for the area

        Returns:
            Tuple of (actual_xml, forecast_xml)
        """
        actual_xml = self.client.fetch_actual_load_for_domain(
            period_start, period_end, out_bidding_zone=area_code
        )
        forecast_xml = self.client.fetch_load_forecast_for_domain(
            period_start, period_end, out_bidding_zone=area_code
        )
        return actual_xml, forecast_xml

    def _save_xml_files(
        self, actual_xml: str, forecast_xml: str, period_start, period_end, country_code: str
    ) -> Tuple[Path, Path]:
        """Save XML files to disk with area-specific naming."""
        start_str = period_start.strftime('%Y%m%d%H%M')
        end_str = period_end.strftime('%Y%m%d%H%M')

        actual_file = self.get_output_path(
            f'entsoe_actual_load_{country_code.lower()}_{start_str}_{end_str}.xml',
            period_start
        )
        forecast_file = self.get_output_path(
            f'entsoe_forecast_load_{country_code.lower()}_{start_str}_{end_str}.xml',
            period_start
        )

        self.save_xml(actual_xml, actual_file)
        self.save_xml(forecast_xml, forecast_file)

        return actual_file, forecast_file

    def _parse_data(self, actual_file: Path, forecast_file: Path, area_id: int, country_code: str) -> List[dict]:
        """Parse XML files and combine data with area_id and country_code."""
        parser = LoadParser(area_id=area_id, country_code=country_code)
        parser.parse_actual_load_xml(str(actual_file))
        parser.parse_forecast_load_xml(str(forecast_file))
        return parser.combine_data()

    def _prepare_records(self, data: List[dict]) -> List[Tuple]:
        """Convert parsed data to tuples for bulk insert."""
        records = []
        for record in data:
            records.append((
                record['trade_date'],
                record['period'],
                record['area_id'],
                record['country_code'],
                record['time_interval'],
                record.get('actual_load_mw'),
                record.get('forecast_load_mw')
            ))
        return records

    def _process_area(
        self, period_start, period_end,
        area_id: int, area_code: str, display_label: str, country_code: str,
        conn=None
    ) -> int:
        """
        Process a single area: fetch, parse, and upload.

        Args:
            period_start: Start datetime (UTC)
            period_end: End datetime (UTC)
            area_id: Integer area ID for partitioning
            area_code: EIC code (e.g., '10YCZ-CEPS-----N')
            display_label: Display label (e.g., 'CZ', 'DE-TenneT')
            country_code: Country code for partition routing (e.g., 'CZ', 'DE')
            conn: Optional database connection

        Returns:
            Number of records processed
        """
        self.logger.info(f"  Fetching {display_label} (area_id={area_id}, country={country_code})...")

        try:
            # Fetch data
            actual_xml, forecast_xml = self._fetch_data_for_area(period_start, period_end, area_code)
            self.logger.debug(f"    Actual: {len(actual_xml)} bytes, Forecast: {len(forecast_xml)} bytes")

            # Save XML files
            actual_file, forecast_file = self._save_xml_files(
                actual_xml, forecast_xml, period_start, period_end, display_label
            )

            # Parse data with area_id and country_code
            data = self._parse_data(actual_file, forecast_file, area_id, country_code)

            if not data:
                self.logger.warning(f"    No data for {country_code}")
                return 0

            self.logger.info(f"    Parsed {len(data)} records")

            # Prepare records for bulk insert
            records = self._prepare_records(data)

            # Upload to database
            if not self.dry_run and conn:
                self.bulk_upsert(
                    conn,
                    self.TABLE_NAME,
                    self.COLUMNS,
                    records,
                    self.CONFLICT_COLUMNS
                )
            elif self.dry_run:
                self.logger.info(f"    DRY RUN - Would upload {len(records)} records")

            return len(records)

        except Exception as e:
            self.logger.error(f"    Failed to process {country_code}: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return 0

    def _process_chunk(self, period_start, period_end, conn=None) -> int:
        """
        Process a single time chunk for ALL areas.

        Args:
            period_start: Start datetime (UTC)
            period_end: End datetime (UTC)
            conn: Optional database connection

        Returns:
            Total number of records processed across all areas
        """
        self.logger.info(
            f"Processing: {period_start.strftime('%Y-%m-%d %H:%M')} "
            f"to {period_end.strftime('%Y-%m-%d %H:%M')} UTC"
        )

        total_records = 0

        # Process each active area sequentially (to avoid API rate limits)
        for area_id, area_code, display_label, country_code in ACTIVE_GENERATION_AREAS:
            records = self._process_area(
                period_start, period_end,
                area_id, area_code, display_label, country_code,
                conn
            )
            total_records += records

        return total_records

    def run(self) -> bool:
        """Execute the unified load data pipeline for all areas."""
        self.print_header()

        # Initialize client
        if not self._init_client():
            return False

        total_records = 0

        try:
            if self.is_backfill:
                # Backfill mode: process multiple chunks
                self.logger.info("")
                self.logger.info(f"Processing {len(ACTIVE_GENERATION_AREAS)} areas: "
                               f"{', '.join(label for _, _, label, _ in ACTIVE_GENERATION_AREAS)}")
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
            else:
                # Normal mode: single time range
                period_start, period_end = self.get_time_range(hours=3)
                self.logger.info(
                    f"Period (UTC): {period_start.strftime('%Y-%m-%d %H:%M')} "
                    f"to {period_end.strftime('%Y-%m-%d %H:%M')}"
                )
                self.logger.info(f"Processing {len(ACTIVE_GENERATION_AREAS)} areas: "
                               f"{', '.join(label for _, _, label, _ in ACTIVE_GENERATION_AREAS)}")
                self.logger.info("")

                if not self.dry_run:
                    with self.database_connection() as conn:
                        total_records = self._process_chunk(period_start, period_end, conn)
                else:
                    total_records = self._process_chunk(period_start, period_end)

            self.logger.info("")
            self.logger.info(f"Total records processed: {total_records}")
            self.print_footer(success=True)
            return True

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            self.print_footer(success=False)
            return False


if __name__ == '__main__':
    UnifiedLoadRunner.main()
