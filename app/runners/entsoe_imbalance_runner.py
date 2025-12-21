#!/usr/bin/env python3
"""
ENTSO-E Imbalance Data Runner.

Fetches imbalance prices (A85) and volumes (A86) for the preceding hours,
parses the XML data, and uploads to PostgreSQL database using bulk upserts.

This script runs every 15 minutes via cron.

Usage:
    python3 entsoe_imbalance_runner.py [--debug] [--dry-run]
"""

import sys
from pathlib import Path
from typing import List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner
from entsoe.client import EntsoeClient
from entsoe.parsers import ImbalanceParser


class ImbalanceRunner(BaseRunner):
    """Runner for ENTSO-E Imbalance data (A85 prices + A86 volumes)."""

    RUNNER_NAME = "ENTSO-E Imbalance Runner"

    # Table configuration
    TABLE_NAME = "entsoe_imbalance_prices"
    COLUMNS = [
        "trade_date", "period", "time_interval",
        "pos_imb_price_czk_mwh", "pos_imb_scarcity_czk_mwh",
        "pos_imb_incentive_czk_mwh", "pos_imb_financial_neutrality_czk_mwh",
        "neg_imb_price_czk_mwh", "neg_imb_scarcity_czk_mwh",
        "neg_imb_incentive_czk_mwh", "neg_imb_financial_neutrality_czk_mwh",
        "imbalance_mwh", "difference_mwh", "situation", "status"
    ]
    CONFLICT_COLUMNS = ["trade_date", "time_interval"]

    def __init__(self, debug: bool = False, dry_run: bool = False):
        super().__init__(debug, dry_run)
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
        Fetch imbalance prices and volumes XML.

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            Tuple of (prices_xml, volumes_xml)
        """
        self.logger.info("")
        self.logger.info("Fetching Imbalance Prices (A85)...")
        prices_xml = self.client.fetch_data('A85', period_start, period_end)
        self.logger.info(f"✓ Received {len(prices_xml)} bytes")

        self.logger.info("")
        self.logger.info("Fetching Imbalance Volumes (A86)...")
        volumes_xml = self.client.fetch_data('A86', period_start, period_end)
        self.logger.info(f"✓ Received {len(volumes_xml)} bytes")

        return prices_xml, volumes_xml

    def _save_xml_files(self, prices_xml: str, volumes_xml: str, period_start, period_end) -> Tuple[Path, Path]:
        """Save XML files to disk."""
        start_str = period_start.strftime('%Y%m%d%H%M')
        end_str = period_end.strftime('%Y%m%d%H%M')

        prices_file = self.get_output_path(
            f'entsoe_imbalance_prices_{start_str}_{end_str}.xml',
            period_start
        )
        volumes_file = self.get_output_path(
            f'entsoe_imbalance_volumes_{start_str}_{end_str}.xml',
            period_start
        )

        self.save_xml(prices_xml, prices_file)
        self.logger.info(f"✓ Saved: {prices_file.name}")

        self.save_xml(volumes_xml, volumes_file)
        self.logger.info(f"✓ Saved: {volumes_file.name}")

        return prices_file, volumes_file

    def _parse_data(self, prices_file: Path, volumes_file: Path) -> List[dict]:
        """Parse XML files and combine data."""
        self.logger.info("")
        self.logger.info("Parsing XML data...")

        self.parser = ImbalanceParser()
        self.parser.parse_prices_xml(str(prices_file))
        self.parser.parse_volumes_xml(str(volumes_file))
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
                record['pos_imb_price_czk_mwh'],
                record['pos_imb_scarcity_czk_mwh'],
                record['pos_imb_incentive_czk_mwh'],
                record['pos_imb_financial_neutrality_czk_mwh'],
                record['neg_imb_price_czk_mwh'],
                record['neg_imb_scarcity_czk_mwh'],
                record['neg_imb_incentive_czk_mwh'],
                record['neg_imb_financial_neutrality_czk_mwh'],
                record['imbalance_mwh'],
                record['difference_mwh'],
                record['situation'],
                record['status']
            ))
        return records

    def run(self) -> bool:
        """Execute the imbalance data pipeline."""
        self.print_header()

        # Initialize client
        if not self._init_client():
            return False

        # Get time range
        period_start, period_end = self.get_time_range(hours=3)
        self.logger.info(
            f"Period (UTC): {period_start.strftime('%Y-%m-%d %H:%M')} "
            f"to {period_end.strftime('%Y-%m-%d %H:%M')}"
        )

        try:
            # Fetch data
            prices_xml, volumes_xml = self._fetch_data(period_start, period_end)

            # Save XML files
            prices_file, volumes_file = self._save_xml_files(
                prices_xml, volumes_xml, period_start, period_end
            )

            # Parse data
            combined_data = self._parse_data(prices_file, volumes_file)

            if not combined_data:
                self.logger.warning("No data to upload")
                self.print_footer(success=True)
                return True

            # Prepare records for bulk insert
            records = self._prepare_records(combined_data)

            # Upload to database
            if not self.dry_run:
                self.logger.info("")
                self.logger.info("Uploading to database...")
                with self.database_connection() as conn:
                    self.bulk_upsert(
                        conn,
                        self.TABLE_NAME,
                        self.COLUMNS,
                        records,
                        self.CONFLICT_COLUMNS
                    )
            else:
                self.logger.info("")
                self.logger.info(f"DRY RUN - Would upload {len(records)} records")

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
    ImbalanceRunner.main()
