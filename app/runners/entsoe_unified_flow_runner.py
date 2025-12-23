#!/usr/bin/env python3
"""
ENTSO-E Unified Cross-Border Physical Flows Runner (A11).

Fetches physical flows data for ALL active areas' borders,
parses the XML into wide-format records with area_id, and uploads
to the partitioned entsoe_cross_border_flows table.

Currently supports CZ borders. Each area can have different border configurations.

Wide-format columns:
- flow_de_mw: Physical flow to/from Germany (positive = import)
- flow_at_mw: Physical flow to/from Austria
- flow_pl_mw: Physical flow to/from Poland
- flow_sk_mw: Physical flow to/from Slovakia
- flow_total_net_mw: Sum of all border flows

Usage:
    python3 entsoe_unified_flow_runner.py [--debug] [--dry-run]
    python3 entsoe_unified_flow_runner.py --start 2024-12-01 --end 2024-12-22
"""

import sys
from pathlib import Path
from typing import List, Tuple, Dict

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner
from entsoe.client import EntsoeClient
from entsoe.parsers import CrossBorderFlowsParser
from entsoe.constants import CZ_BZN, CZ_NEIGHBORS


class UnifiedFlowRunner(BaseRunner):
    """Unified runner for ENTSO-E Cross-Border Physical Flows data (A11)."""

    RUNNER_NAME = "ENTSO-E Unified Cross-Border Flows Runner"

    # Table configuration - partitioned by country_code
    TABLE_NAME = "entsoe_cross_border_flows"
    COLUMNS = [
        "trade_date", "period", "area_id", "country_code", "time_interval",
        "delivery_datetime", "flow_de_mw", "flow_at_mw", "flow_pl_mw", "flow_sk_mw",
        "flow_total_net_mw"
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

    def _fetch_all_borders(self, period_start, period_end, area_code: str) -> Dict[str, str]:
        """
        Fetch A11 XML for all borders of a specific area.

        For CZ, fetches two directions per neighbor:
        - CZ -> Neighbor (export, negative)
        - Neighbor -> CZ (import, positive)

        Returns:
            Dict mapping border key to XML content
        """
        xml_data = {}

        for neighbor_key, neighbor_eic in CZ_NEIGHBORS.items():
            # Fetch CZ -> Neighbor (export from CZ)
            try:
                self.logger.debug(f"    Fetching CZ -> {neighbor_key.upper()} (export)...")
                xml_export = self.client.fetch_cross_border_flows(
                    period_start, period_end,
                    in_domain=neighbor_eic,
                    out_domain=area_code
                )
                xml_data[f"{neighbor_key}_export"] = xml_export
            except Exception as e:
                self.logger.warning(f"    Failed to fetch CZ -> {neighbor_key.upper()}: {e}")

            # Fetch Neighbor -> CZ (import to CZ)
            try:
                self.logger.debug(f"    Fetching {neighbor_key.upper()} -> CZ (import)...")
                xml_import = self.client.fetch_cross_border_flows(
                    period_start, period_end,
                    in_domain=area_code,
                    out_domain=neighbor_eic
                )
                xml_data[f"{neighbor_key}_import"] = xml_import
            except Exception as e:
                self.logger.warning(f"    Failed to fetch {neighbor_key.upper()} -> CZ: {e}")

        return xml_data

    def _save_xml_files(
        self, xml_data: Dict[str, str], period_start, period_end, country_code: str
    ) -> Dict[str, Path]:
        """Save all XML files to disk."""
        saved_files = {}
        start_str = period_start.strftime('%Y%m%d%H%M')
        end_str = period_end.strftime('%Y%m%d%H%M')

        for border_key, xml_content in xml_data.items():
            output_file = self.get_output_path(
                f'entsoe_flow_{country_code.lower()}_{border_key}_{start_str}_{end_str}.xml',
                period_start
            )
            self.save_xml(xml_content, output_file)
            saved_files[border_key] = output_file

        self.logger.debug(f"    Saved {len(saved_files)} XML files")
        return saved_files

    def _parse_data(
        self, xml_files: Dict[str, Path], area_id: int, country_code: str
    ) -> List[dict]:
        """Parse all XML files into wide-format records."""
        parser = CrossBorderFlowsParser(area_id=area_id, country_code=country_code)

        for border_key, xml_file in xml_files.items():
            try:
                parser.parse_xml(str(xml_file))
                self.logger.debug(f"    Parsed: {border_key}")
            except Exception as e:
                self.logger.warning(f"    Failed to parse {border_key}: {e}")

        return parser.get_wide_format_data()

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
                record['delivery_datetime'],
                record.get('flow_de_mw'),
                record.get('flow_at_mw'),
                record.get('flow_pl_mw'),
                record.get('flow_sk_mw'),
                record.get('flow_total_net_mw')
            ))
        return records

    def _process_area(
        self, period_start, period_end,
        area_id: int, area_code: str, display_label: str, country_code: str,
        conn=None
    ) -> int:
        """Process cross-border flows for a single area."""
        self.logger.info(f"  Processing {display_label} (area_id={area_id}, country={country_code})...")

        try:
            # Fetch all border data
            xml_data = self._fetch_all_borders(period_start, period_end, area_code)

            if not xml_data:
                self.logger.warning(f"    No data fetched for {country_code}")
                return 0

            # Save XML files
            xml_files = self._save_xml_files(xml_data, period_start, period_end, display_label)

            # Parse data with area_id and country_code
            data = self._parse_data(xml_files, area_id, country_code)

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
        """Process a single time chunk for CZ (primary area with border data)."""
        self.logger.info(
            f"Processing: {period_start.strftime('%Y-%m-%d %H:%M')} "
            f"to {period_end.strftime('%Y-%m-%d %H:%M')} UTC"
        )

        # Currently only process CZ borders (area_id=1)
        # Other areas would need different border configurations
        return self._process_area(
            period_start, period_end,
            area_id=1, area_code=CZ_BZN, display_label="CZ", country_code="CZ",
            conn=conn
        )

    def run(self) -> bool:
        """Execute the unified cross-border flows data pipeline."""
        self.print_header()

        if not self._init_client():
            return False

        total_records = 0

        try:
            if self.is_backfill:
                self.logger.info("")
                self.logger.info("Processing CZ cross-border flows")
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
                self.logger.info("Processing CZ cross-border flows")
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
    UnifiedFlowRunner.main()
