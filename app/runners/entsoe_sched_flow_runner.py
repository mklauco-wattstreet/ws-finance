#!/usr/bin/env python3
"""
ENTSO-E Scheduled Cross-Border Flows Runner (A09).

Fetches day-ahead scheduled commercial exchanges for all CZ border pairs,
parses the XML into wide-format records, and uploads to PostgreSQL
database using bulk upserts.

This script runs every 15 minutes via cron.

Wide-format columns:
- scheduled_de_mw: Scheduled exchange with Germany (positive = import, negative = export)
- scheduled_at_mw: Scheduled exchange with Austria
- scheduled_pl_mw: Scheduled exchange with Poland
- scheduled_sk_mw: Scheduled exchange with Slovakia
- scheduled_total_net_mw: Sum of all scheduled exchanges

Usage:
    python3 entsoe_sched_flow_runner.py [--debug] [--dry-run]
    python3 entsoe_sched_flow_runner.py --start 2024-12-01 --end 2024-12-15
"""

import sys
from pathlib import Path
from typing import List, Tuple, Dict

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner
from entsoe.client import EntsoeClient
from entsoe.parsers import ScheduledExchangesParser
from entsoe.constants import CZ_BZN, CZ_NEIGHBORS


class ScheduledFlowRunner(BaseRunner):
    """Runner for ENTSO-E Scheduled Cross-Border Flows data (A09) - Wide Format."""

    RUNNER_NAME = "ENTSO-E Scheduled Cross-Border Flows Runner"

    # Table configuration - Wide format with trade_date/period pattern
    TABLE_NAME = "entsoe_scheduled_cross_border_flows"
    COLUMNS = [
        "trade_date", "period", "time_interval",
        "scheduled_de_mw", "scheduled_at_mw", "scheduled_pl_mw", "scheduled_sk_mw",
        "scheduled_total_net_mw"
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

    def _fetch_all_borders(self, period_start, period_end) -> Dict[str, Tuple[str, str, str]]:
        """
        Fetch A09 XML for all CZ border pairs.

        For each neighbor, fetches two directions:
        - CZ -> Neighbor (export, negative)
        - Neighbor -> CZ (import, positive)

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            Dict mapping border key to (xml_content, in_domain, out_domain)
        """
        xml_data = {}

        for neighbor_key, neighbor_eic in CZ_NEIGHBORS.items():
            self.logger.info("")
            self.logger.info(f"Fetching scheduled exchanges for CZ <-> {neighbor_key.upper()}...")

            # Fetch CZ -> Neighbor (export from CZ)
            try:
                self.logger.info(f"  Fetching CZ -> {neighbor_key.upper()} (export)...")
                xml_export = self.client.fetch_scheduled_exchanges(
                    period_start, period_end,
                    in_domain=neighbor_eic,
                    out_domain=CZ_BZN
                )
                xml_data[f"{neighbor_key}_export"] = (xml_export, neighbor_eic, CZ_BZN)
                self.logger.info(f"  ✓ Received {len(xml_export)} bytes")
            except Exception as e:
                self.logger.warning(f"  ✗ Failed to fetch CZ -> {neighbor_key.upper()}: {e}")

            # Fetch Neighbor -> CZ (import to CZ)
            try:
                self.logger.info(f"  Fetching {neighbor_key.upper()} -> CZ (import)...")
                xml_import = self.client.fetch_scheduled_exchanges(
                    period_start, period_end,
                    in_domain=CZ_BZN,
                    out_domain=neighbor_eic
                )
                xml_data[f"{neighbor_key}_import"] = (xml_import, CZ_BZN, neighbor_eic)
                self.logger.info(f"  ✓ Received {len(xml_import)} bytes")
            except Exception as e:
                self.logger.warning(f"  ✗ Failed to fetch {neighbor_key.upper()} -> CZ: {e}")

        return xml_data

    def _save_xml_files(
        self, xml_data: Dict[str, Tuple[str, str, str]], period_start, period_end
    ) -> Dict[str, Tuple[Path, str, str]]:
        """
        Save all XML files to disk.

        Args:
            xml_data: Dict mapping border key to (xml_content, in_domain, out_domain)
            period_start: Period start datetime
            period_end: Period end datetime

        Returns:
            Dict mapping border key to (file_path, in_domain, out_domain)
        """
        saved_files = {}
        start_str = period_start.strftime('%Y%m%d%H%M')
        end_str = period_end.strftime('%Y%m%d%H%M')

        for border_key, (xml_content, in_domain, out_domain) in xml_data.items():
            output_file = self.get_output_path(
                f'entsoe_sched_flow_{border_key}_{start_str}_{end_str}.xml',
                period_start
            )
            self.save_xml(xml_content, output_file)
            saved_files[border_key] = (output_file, in_domain, out_domain)
            self.logger.debug(f"  Saved: {output_file.name}")

        self.logger.info(f"✓ Saved {len(saved_files)} XML files")
        return saved_files

    def _parse_data(self, xml_files: Dict[str, Tuple[Path, str, str]]) -> List[dict]:
        """
        Parse all XML files into wide-format records.

        Args:
            xml_files: Dict mapping border key to (file_path, in_domain, out_domain)

        Returns:
            List of wide-format scheduled flow records
        """
        self.logger.info("")
        self.logger.info("Parsing XML data (wide format)...")

        self.parser = ScheduledExchangesParser()

        # Parse all XML files into the same parser
        for border_key, (xml_file, in_domain, out_domain) in xml_files.items():
            try:
                self.parser.parse_xml(str(xml_file), in_domain, out_domain)
                self.logger.debug(f"  Parsed: {border_key}")
            except Exception as e:
                self.logger.warning(f"  ✗ Failed to parse {border_key}: {e}")

        # Get wide-format data
        data = self.parser.get_wide_format_records()

        self.logger.info(f"✓ Parsed {len(data)} wide-format records")

        # Log sample values for first record
        if data and self.debug:
            sample = data[0]
            self.logger.debug(f"  Sample record for {sample['trade_date']} period {sample['period']}:")
            for col in ScheduledExchangesParser.WIDE_COLUMNS:
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
                record.get('scheduled_de_mw'),
                record.get('scheduled_at_mw'),
                record.get('scheduled_pl_mw'),
                record.get('scheduled_sk_mw'),
                record.get('scheduled_total_net_mw')
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

        # Fetch data for all borders
        xml_data = self._fetch_all_borders(period_start, period_end)

        if not xml_data:
            self.logger.warning("No data fetched from any border")
            return 0

        # Save XML files
        xml_files = self._save_xml_files(xml_data, period_start, period_end)

        # Parse data
        data = self._parse_data(xml_files)

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
        """Execute the scheduled cross-border flows data pipeline."""
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
    ScheduledFlowRunner.main()
