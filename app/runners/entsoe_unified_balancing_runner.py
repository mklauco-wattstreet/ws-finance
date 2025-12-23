#!/usr/bin/env python3
"""
ENTSO-E Unified Balancing Energy Runner.

Fetches activated balancing energy prices (A84) for ALL active areas,
parses the XML into wide-format records with area_id, and uploads
to the partitioned entsoe_balancing_energy table.

Active areas (from entsoe_areas table):
- CZ (Czech Republic) - area_id=1
- DE (Germany TenneT) - area_id=2
- AT (Austria) - area_id=3
- PL (Poland) - area_id=4
- SK (Slovakia) - area_id=5

Wide-format columns:
- afrr_up_price_eur: aFRR upward activation price
- afrr_down_price_eur: aFRR downward activation price
- mfrr_up_price_eur: mFRR upward activation price
- mfrr_down_price_eur: mFRR downward activation price

Usage:
    python3 entsoe_unified_balancing_runner.py [--debug] [--dry-run]
    python3 entsoe_unified_balancing_runner.py --start 2024-12-01 --end 2024-12-22
"""

import sys
from pathlib import Path
from typing import List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner
from entsoe.client import EntsoeClient
from entsoe.parsers import BalancingEnergyParser
from entsoe.constants import ACTIVE_GENERATION_AREAS


class UnifiedBalancingRunner(BaseRunner):
    """Unified runner for ENTSO-E Activated Balancing Energy Prices (A84) - All Areas."""

    RUNNER_NAME = "ENTSO-E Unified Balancing Energy Runner"

    # Table configuration - partitioned by country_code
    TABLE_NAME = "entsoe_balancing_energy"
    COLUMNS = [
        "trade_date", "period", "area_id", "country_code", "time_interval",
        "afrr_up_price_eur", "afrr_down_price_eur", "mfrr_up_price_eur", "mfrr_down_price_eur"
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

    def _fetch_data_for_area(self, period_start, period_end, area_code: str) -> str:
        """Fetch activated balancing energy XML for a specific area."""
        xml_content = self.client.fetch_activated_balancing_energy_for_domain(
            period_start, period_end, control_area=area_code
        )
        return xml_content

    def _save_xml_file(
        self, xml_content: str, period_start, period_end, country_code: str
    ) -> Path:
        """Save XML file to disk with area-specific naming."""
        start_str = period_start.strftime('%Y%m%d%H%M')
        end_str = period_end.strftime('%Y%m%d%H%M')

        output_file = self.get_output_path(
            f'entsoe_balancing_{country_code.lower()}_{start_str}_{end_str}.xml',
            period_start
        )

        self.save_xml(xml_content, output_file)
        self.logger.debug(f"  Saved: {output_file.name}")

        return output_file

    def _parse_data(self, xml_file: Path, area_id: int, country_code: str) -> List[dict]:
        """Parse XML file into wide-format records with area_id and country_code."""
        parser = BalancingEnergyParser(area_id=area_id, country_code=country_code)
        data = parser.parse_xml(str(xml_file))
        return data

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
                record.get('afrr_up_price_eur'),
                record.get('afrr_down_price_eur'),
                record.get('mfrr_up_price_eur'),
                record.get('mfrr_down_price_eur')
            ))
        return records

    def _process_area(
        self, period_start, period_end,
        area_id: int, area_code: str, display_label: str, country_code: str,
        conn=None
    ) -> int:
        """Process a single area: fetch, parse, and upload."""
        self.logger.info(f"  Fetching {display_label} (area_id={area_id}, country={country_code})...")

        try:
            xml_content = self._fetch_data_for_area(period_start, period_end, area_code)
            self.logger.debug(f"    Received {len(xml_content)} bytes")

            xml_file = self._save_xml_file(xml_content, period_start, period_end, display_label)
            data = self._parse_data(xml_file, area_id, country_code)

            if not data:
                self.logger.warning(f"    No data for {country_code}")
                return 0

            self.logger.info(f"    Parsed {len(data)} records")

            records = self._prepare_records(data)

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

        for area_id, area_code, display_label, country_code in ACTIVE_GENERATION_AREAS:
            records = self._process_area(
                period_start, period_end,
                area_id, area_code, display_label, country_code,
                conn
            )
            total_records += records

        return total_records

    def run(self) -> bool:
        """Execute the unified balancing energy data pipeline for all areas."""
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
    UnifiedBalancingRunner.main()
