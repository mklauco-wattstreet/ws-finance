#!/usr/bin/env python3
"""
ENTSO-E Unified Imbalance Prices Runner.

Fetches imbalance prices (A85) and volumes (A86) for active areas,
parses the XML into records with area_id, and uploads
to the partitioned entsoe_imbalance_prices table.

Note: Imbalance prices are currently CZ-specific due to CZK currency.
This runner supports multi-area fetching for future expansion.

Usage:
    python3 entsoe_unified_imbalance_runner.py [--debug] [--dry-run]
    python3 entsoe_unified_imbalance_runner.py --start 2024-12-01 --end 2024-12-22
"""

import sys
from pathlib import Path
from typing import List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner
from entsoe.client import EntsoeClient
from entsoe.parsers import ImbalanceParser
from entsoe.constants import ACTIVE_GENERATION_AREAS


class UnifiedImbalanceRunner(BaseRunner):
    """Unified runner for ENTSO-E Imbalance Prices data (A85/A86)."""

    RUNNER_NAME = "ENTSO-E Unified Imbalance Prices Runner"

    # Table configuration - partitioned by country_code
    TABLE_NAME = "entsoe_imbalance_prices"
    COLUMNS = [
        "trade_date", "period", "area_id", "country_code", "time_interval",
        "pos_imb_price_czk_mwh", "pos_imb_scarcity_czk_mwh",
        "pos_imb_incentive_czk_mwh", "pos_imb_financial_neutrality_czk_mwh",
        "neg_imb_price_czk_mwh", "neg_imb_scarcity_czk_mwh",
        "neg_imb_incentive_czk_mwh", "neg_imb_financial_neutrality_czk_mwh",
        "imbalance_mwh", "difference_mwh", "situation", "status"
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
        """Fetch imbalance prices and volumes XML for a specific area."""
        prices_xml = self.client.fetch_imbalance_prices_for_domain(
            period_start, period_end, control_area=area_code
        )
        volumes_xml = self.client.fetch_imbalance_volumes_for_domain(
            period_start, period_end, control_area=area_code
        )
        return prices_xml, volumes_xml

    def _save_xml_files(
        self, prices_xml: str, volumes_xml: str, period_start, period_end, country_code: str
    ) -> Tuple[Path, Path]:
        """Save XML files to disk with area-specific naming."""
        start_str = period_start.strftime('%Y%m%d%H%M')
        end_str = period_end.strftime('%Y%m%d%H%M')

        prices_file = self.get_output_path(
            f'entsoe_imbalance_prices_{country_code.lower()}_{start_str}_{end_str}.xml',
            period_start
        )
        volumes_file = self.get_output_path(
            f'entsoe_imbalance_volumes_{country_code.lower()}_{start_str}_{end_str}.xml',
            period_start
        )

        self.save_xml(prices_xml, prices_file)
        self.save_xml(volumes_xml, volumes_file)

        return prices_file, volumes_file

    def _parse_data(
        self, prices_file: Path, volumes_file: Path, area_id: int, country_code: str
    ) -> List[dict]:
        """Parse XML files and combine data with area_id and country_code."""
        parser = ImbalanceParser(area_id=area_id, country_code=country_code)
        parser.parse_prices_xml(str(prices_file))
        parser.parse_volumes_xml(str(volumes_file))
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
                record.get('pos_imb_price_czk_mwh'),
                record.get('pos_imb_scarcity_czk_mwh'),
                record.get('pos_imb_incentive_czk_mwh'),
                record.get('pos_imb_financial_neutrality_czk_mwh'),
                record.get('neg_imb_price_czk_mwh'),
                record.get('neg_imb_scarcity_czk_mwh'),
                record.get('neg_imb_incentive_czk_mwh'),
                record.get('neg_imb_financial_neutrality_czk_mwh'),
                record.get('imbalance_mwh'),
                record.get('difference_mwh'),
                record.get('situation'),
                record.get('status')
            ))
        return records

    def _process_area(
        self, period_start, period_end,
        area_id: int, area_code: str, display_label: str, country_code: str,
        conn=None
    ) -> int:
        """Process imbalance data for a single area."""
        self.logger.info(f"  Fetching {display_label} (area_id={area_id}, country={country_code})...")

        try:
            # Fetch data for this specific area
            prices_xml, volumes_xml = self._fetch_data_for_area(period_start, period_end, area_code)
            self.logger.debug(f"    Prices: {len(prices_xml)} bytes, Volumes: {len(volumes_xml)} bytes")

            # Save XML files
            prices_file, volumes_file = self._save_xml_files(
                prices_xml, volumes_xml, period_start, period_end, display_label
            )

            # Parse data with area_id and country_code
            data = self._parse_data(prices_file, volumes_file, area_id, country_code)

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
        """Process a single time chunk for ALL areas."""
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
        """Execute the unified imbalance prices data pipeline."""
        self.print_header()

        if not self._init_client():
            return False

        total_records = 0

        try:
            if self.is_backfill:
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
    UnifiedImbalanceRunner.main()
