#!/usr/bin/env python3
"""
ENTSO-E Load Data Runner.

Fetches actual load (A65/A16) and day-ahead forecast (A65/A01) data,
parses the XML, and uploads to PostgreSQL database using bulk upserts.

This script runs every 15 minutes via cron.

Usage:
    python3 entsoe_load_runner.py [--debug] [--dry-run]
"""

import sys
from pathlib import Path
from typing import List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner
from entsoe.client import EntsoeClient
from entsoe.parsers import LoadParser


class LoadRunner(BaseRunner):
    """Runner for ENTSO-E Load data (A65 actual + forecast)."""

    RUNNER_NAME = "ENTSO-E Load Runner"

    # Table configuration
    TABLE_NAME = "entsoe_load"
    COLUMNS = [
        "trade_date", "period", "time_interval",
        "actual_load_mw", "forecast_load_mw"
    ]
    CONFLICT_COLUMNS = ["trade_date", "period"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = None
        self.parser = None

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

    def _fetch_data(self, period_start, period_end) -> Tuple[str, str]:
        """
        Fetch actual load and forecast load XML.

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            Tuple of (actual_xml, forecast_xml)
        """
        self.logger.info("")
        self.logger.info("Fetching Actual Load (A65/A16)...")
        actual_xml = self.client.fetch_actual_load(period_start, period_end)
        self.logger.info(f"✓ Received {len(actual_xml)} bytes")

        self.logger.info("")
        self.logger.info("Fetching Load Forecast (A65/A01)...")
        forecast_xml = self.client.fetch_load_forecast(period_start, period_end)
        self.logger.info(f"✓ Received {len(forecast_xml)} bytes")

        return actual_xml, forecast_xml

    def _save_xml_files(
        self, actual_xml: str, forecast_xml: str, period_start, period_end
    ) -> Tuple[Path, Path]:
        """Save XML files to disk."""
        start_str = period_start.strftime('%Y%m%d%H%M')
        end_str = period_end.strftime('%Y%m%d%H%M')

        actual_file = self.get_output_path(
            f'entsoe_actual_load_{start_str}_{end_str}.xml',
            period_start
        )
        forecast_file = self.get_output_path(
            f'entsoe_forecast_load_{start_str}_{end_str}.xml',
            period_start
        )

        self.save_xml(actual_xml, actual_file)
        self.logger.info(f"✓ Saved: {actual_file.name}")

        self.save_xml(forecast_xml, forecast_file)
        self.logger.info(f"✓ Saved: {forecast_file.name}")

        return actual_file, forecast_file

    def _parse_data(self, actual_file: Path, forecast_file: Path) -> List[dict]:
        """Parse XML files and combine data."""
        self.logger.info("")
        self.logger.info("Parsing XML data...")

        self.parser = LoadParser()
        self.parser.parse_actual_load_xml(str(actual_file))
        self.parser.parse_forecast_load_xml(str(forecast_file))
        combined = self.parser.combine_data()

        self.logger.info(f"✓ Parsed {len(combined)} records")
        return combined

    def _prepare_records(self, combined_data: List[dict]) -> List[Tuple]:
        """
        Convert parsed data to tuples for bulk insert.

        Args:
            combined_data: List of parsed record dicts

        Returns:
            List of tuples ready for execute_values
        """
        records = []
        for record in combined_data:
            records.append((
                record['trade_date'],
                record['period'],
                record['time_interval'],
                record.get('actual_load_mw'),
                record.get('forecast_load_mw')
            ))
        return records

    def _process_chunk(self, period_start, period_end, conn=None) -> int:
        """
        Process a single time chunk: fetch, parse, and upload.

        Args:
            period_start: Start datetime (UTC)
            period_end: End datetime (UTC)
            conn: Optional database connection (for batch operations)

        Returns:
            Number of records processed
        """
        self.logger.info(
            f"Processing: {period_start.strftime('%Y-%m-%d %H:%M')} "
            f"to {period_end.strftime('%Y-%m-%d %H:%M')} UTC"
        )

        # Fetch data
        actual_xml, forecast_xml = self._fetch_data(period_start, period_end)

        # Save XML files
        actual_file, forecast_file = self._save_xml_files(
            actual_xml, forecast_xml, period_start, period_end
        )

        # Parse data
        combined_data = self._parse_data(actual_file, forecast_file)

        if not combined_data:
            self.logger.warning("No data in this chunk")
            return 0

        # Prepare records for bulk insert
        records = self._prepare_records(combined_data)

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
            self.logger.info(f"DRY RUN - Would upload {len(records)} records")

        return len(records)

    def run(self) -> bool:
        """Execute the load data pipeline."""
        self.print_header()

        # Initialize client
        if not self._init_client():
            return False

        total_records = 0

        try:
            if self.is_backfill:
                # Backfill mode: process multiple chunks
                self.logger.info("")
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
                            # Continue with next chunk
                            continue
            else:
                # Normal mode: single time range
                period_start, period_end = self.get_time_range(hours=3)
                self.logger.info(
                    f"Period (UTC): {period_start.strftime('%Y-%m-%d %H:%M')} "
                    f"to {period_end.strftime('%Y-%m-%d %H:%M')}"
                )

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
    LoadRunner.main()
