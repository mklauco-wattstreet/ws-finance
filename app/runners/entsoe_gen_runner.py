#!/usr/bin/env python3
"""
ENTSO-E Generation Data Runner (Wide Format).

Fetches actual generation per type (A75) data for all PSR types,
parses the XML into wide-format records, and uploads to PostgreSQL
database using bulk upserts.

This script runs every 15 minutes via cron.

Wide-format columns with aggregated PSR types:
- gen_nuclear_mw: B14 (Nuclear)
- gen_coal_mw: B02 (Brown coal/Lignite) + B05 (Hard coal)
- gen_gas_mw: B04 (Fossil Gas)
- gen_solar_mw: B16 (Solar)
- gen_wind_mw: B19 (Wind Onshore)
- gen_hydro_pumped_mw: B10 (Hydro Pumped Storage)
- gen_biomass_mw: B01 (Biomass)
- gen_hydro_other_mw: B11 (Run-of-river) + B12 (Water Reservoir)

Usage:
    python3 entsoe_gen_runner.py [--debug] [--dry-run]
"""

import sys
from pathlib import Path
from typing import List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner
from entsoe.client import EntsoeClient
from entsoe.parsers import GenerationParser


class GenerationRunner(BaseRunner):
    """Runner for ENTSO-E Generation per Type data (A75) - Wide Format."""

    RUNNER_NAME = "ENTSO-E Generation Runner"

    # Table configuration - Wide format
    TABLE_NAME = "entsoe_generation_actual"
    COLUMNS = [
        "trade_date", "period", "time_interval",
        "gen_nuclear_mw", "gen_coal_mw", "gen_gas_mw", "gen_solar_mw",
        "gen_wind_mw", "gen_hydro_pumped_mw", "gen_biomass_mw", "gen_hydro_other_mw"
    ]
    CONFLICT_COLUMNS = ["trade_date", "period"]

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

    def _fetch_data(self, period_start, period_end) -> str:
        """
        Fetch generation per type XML.

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            XML content string
        """
        self.logger.info("")
        self.logger.info("Fetching Generation per Type (A75)...")
        xml_content = self.client.fetch_generation_per_type(period_start, period_end)
        self.logger.info(f"✓ Received {len(xml_content)} bytes")

        return xml_content

    def _save_xml_file(
        self, xml_content: str, period_start, period_end
    ) -> Path:
        """Save XML file to disk."""
        start_str = period_start.strftime('%Y%m%d%H%M')
        end_str = period_end.strftime('%Y%m%d%H%M')

        output_file = self.get_output_path(
            f'entsoe_generation_{start_str}_{end_str}.xml',
            period_start
        )

        self.save_xml(xml_content, output_file)
        self.logger.info(f"✓ Saved: {output_file.name}")

        return output_file

    def _parse_data(self, xml_file: Path) -> List[dict]:
        """Parse XML file into wide-format records."""
        self.logger.info("")
        self.logger.info("Parsing XML data (wide format)...")

        self.parser = GenerationParser()
        data = self.parser.parse_xml(str(xml_file))

        self.logger.info(f"✓ Parsed {len(data)} wide-format records")

        # Log sample values for first record
        if data and self.debug:
            sample = data[0]
            self.logger.debug(f"  Sample record for {sample['trade_date']} period {sample['period']}:")
            for col in GenerationParser.WIDE_COLUMNS:
                self.logger.debug(f"    {col}: {sample.get(col, 0.0):.1f} MW")

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
                record.get('gen_nuclear_mw', 0.0),
                record.get('gen_coal_mw', 0.0),
                record.get('gen_gas_mw', 0.0),
                record.get('gen_solar_mw', 0.0),
                record.get('gen_wind_mw', 0.0),
                record.get('gen_hydro_pumped_mw', 0.0),
                record.get('gen_biomass_mw', 0.0),
                record.get('gen_hydro_other_mw', 0.0)
            ))
        return records

    def run(self) -> bool:
        """Execute the generation data pipeline."""
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
            xml_content = self._fetch_data(period_start, period_end)

            # Save XML file
            xml_file = self._save_xml_file(xml_content, period_start, period_end)

            # Parse data
            data = self._parse_data(xml_file)

            if not data:
                self.logger.warning("No data to upload")
                self.print_footer(success=True)
                return True

            # Prepare records for bulk insert
            records = self._prepare_records(data)

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
                self.logger.info(f"DRY RUN - Would upload {len(records)} wide-format records")

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
    GenerationRunner.main()
