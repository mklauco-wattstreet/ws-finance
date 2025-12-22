#!/usr/bin/env python3
"""
ENTSO-E Balancing Energy Runner (A84).

Fetches activated balancing energy (aFRR/mFRR) data,
parses the XML into wide-format records, and uploads to PostgreSQL
database using bulk upserts.

This script runs every 15 minutes via cron.

Wide-format columns:
- afrr_up_mw: Automatic Frequency Restoration Reserve (upward activation)
- afrr_down_mw: Automatic Frequency Restoration Reserve (downward activation)
- mfrr_up_mw: Manual Frequency Restoration Reserve (upward activation)
- mfrr_down_mw: Manual Frequency Restoration Reserve (downward activation)

Usage:
    python3 entsoe_balancing_runner.py [--debug] [--dry-run]
    python3 entsoe_balancing_runner.py --start 2024-12-01 --end 2024-12-15
"""

import sys
from pathlib import Path
from typing import List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner
from entsoe.client import EntsoeClient
from entsoe.parsers import BalancingEnergyParser


class BalancingEnergyRunner(BaseRunner):
    """Runner for ENTSO-E Activated Balancing Energy data (A84) - Wide Format."""

    RUNNER_NAME = "ENTSO-E Balancing Energy Runner"

    # Table configuration - Wide format
    TABLE_NAME = "entsoe_balancing_energy"
    COLUMNS = [
        "trade_date", "period", "time_interval",
        "afrr_up_mw", "afrr_down_mw", "mfrr_up_mw", "mfrr_down_mw"
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

    def _fetch_data(self, period_start, period_end) -> str:
        """
        Fetch activated balancing energy XML.

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            XML content string
        """
        self.logger.info("")
        self.logger.info("Fetching Activated Balancing Energy (A84)...")
        xml_content = self.client.fetch_activated_balancing_energy(period_start, period_end)
        self.logger.info(f"✓ Received {len(xml_content)} bytes")

        return xml_content

    def _save_xml_file(
        self, xml_content: str, period_start, period_end
    ) -> Path:
        """Save XML file to disk."""
        start_str = period_start.strftime('%Y%m%d%H%M')
        end_str = period_end.strftime('%Y%m%d%H%M')

        output_file = self.get_output_path(
            f'entsoe_balancing_{start_str}_{end_str}.xml',
            period_start
        )

        self.save_xml(xml_content, output_file)
        self.logger.info(f"✓ Saved: {output_file.name}")

        return output_file

    def _parse_data(self, xml_file: Path) -> List[dict]:
        """Parse XML file into wide-format records."""
        self.logger.info("")
        self.logger.info("Parsing XML data (wide format)...")

        self.parser = BalancingEnergyParser()
        data = self.parser.parse_xml(str(xml_file))

        self.logger.info(f"✓ Parsed {len(data)} wide-format records")

        # Log sample values for first record
        if data and self.debug:
            sample = data[0]
            self.logger.debug(f"  Sample record for {sample['trade_date']} period {sample['period']}:")
            for col in BalancingEnergyParser.WIDE_COLUMNS:
                value = sample.get(col)
                value_str = f"{value:.1f} MW" if value is not None else "N/A"
                self.logger.debug(f"    {col}: {value_str}")

        return data

    def _prepare_records(self, data: List[dict]) -> List[Tuple]:
        """
        Convert parsed data to tuples for bulk insert.

        Args:
            data: List of parsed wide-format record dicts

        Returns:
            List of tuples ready for execute_values
        """
        records = []
        for record in data:
            records.append((
                record['trade_date'],
                record['period'],
                record['time_interval'],
                record.get('afrr_up_mw'),
                record.get('afrr_down_mw'),
                record.get('mfrr_up_mw'),
                record.get('mfrr_down_mw')
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
        xml_content = self._fetch_data(period_start, period_end)

        # Save XML file
        xml_file = self._save_xml_file(xml_content, period_start, period_end)

        # Parse data
        data = self._parse_data(xml_file)

        if not data:
            self.logger.warning("No data in this chunk")
            return 0

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
            self.logger.info(f"DRY RUN - Would upload {len(records)} records")

        return len(records)

    def run(self) -> bool:
        """Execute the balancing energy data pipeline."""
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
    BalancingEnergyRunner.main()
