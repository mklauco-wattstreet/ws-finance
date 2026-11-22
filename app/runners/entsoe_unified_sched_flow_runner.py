#!/usr/bin/env python3
"""
ENTSO-E Unified Scheduled Cross-Border Flows Runner (A09).

Fetches scheduled commercial exchanges for active areas' borders,
parses the XML into wide-format records with area_id, and uploads
to the partitioned entsoe_scheduled_cross_border_flows table.

Currently supports CZ borders. Each area can have different border configurations.

Wide-format columns:
- scheduled_de_mw: Scheduled exchange with Germany (positive = import)
- scheduled_at_mw: Scheduled exchange with Austria
- scheduled_pl_mw: Scheduled exchange with Poland
- scheduled_sk_mw: Scheduled exchange with Slovakia
- scheduled_total_net_mw: Sum of all four borders; NULL unless all four are present

Each neighbour column is ONE net value (import minus export), built from two
ENTSO-E documents (one per direction). A neighbour whose direction failed with
anything other than "No matching data" is written as NULL, never as a
one-direction value.

Completeness (cron mode): a delivery day is "done" only when every interval of
that Prague day (96 quarter-hours; 92/100 on DST change days) has all four
neighbour columns non-NULL. Until then the day is fetched again on every run
and the upsert fills the gaps without ever overwriting a stored value with NULL.
This strict rule applies to today, tomorrow and yesterday only; older days keep
the "any row = done" rule so a border that is permanently missing at the source
costs requests for three days at most.

Usage:
    python3 entsoe_unified_sched_flow_runner.py [--debug] [--dry-run]
    python3 entsoe_unified_sched_flow_runner.py --start 2024-12-01 --end 2024-12-22
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Dict

from psycopg2 import extras

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from entsoe.client import EntsoeClient
from entsoe.parsers import ScheduledExchangesParser
from entsoe.constants import CZ_BZN, CZ_NEIGHBORS


class UnifiedSchedFlowRunner(BaseRunner):
    """Unified runner for ENTSO-E Scheduled Commercial Exchanges (A09)."""

    RUNNER_NAME = "ENTSO-E Unified Scheduled Cross-Border Flows Runner"

    # Table configuration - partitioned by country_code
    TABLE_NAME = "entsoe_scheduled_cross_border_flows"
    COLUMNS = [
        "trade_date", "period", "area_id", "country_code", "time_interval",
        "delivery_datetime",
        "scheduled_de_mw", "scheduled_at_mw", "scheduled_pl_mw", "scheduled_sk_mw",
        "scheduled_total_net_mw"
    ]
    CONFLICT_COLUMNS = ["delivery_datetime", "area_id", "country_code"]
    BORDER_COLUMNS = ["scheduled_de_mw", "scheduled_at_mw", "scheduled_pl_mw", "scheduled_sk_mw"]
    TOTAL_COLUMN = "scheduled_total_net_mw"

    # Days from today - STRICT_DAYS_BACK .. tomorrow must be complete to count as done.
    STRICT_DAYS_BACK = 1

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = None
        # Neighbours (de/at/pl/sk) whose current fetch/parse had a real failure
        # in at least one direction; their column is written as NULL.
        self._tainted_neighbors = set()

    def _areas_with_data(self, conn, target_date, areas) -> set:
        """
        Areas whose delivery day is done.

        today-1 .. tomorrow: done only when every interval of the Prague day has
        all four neighbour columns non-NULL (expected count derived from the
        day length: 96, or 92/100 on DST change days).
        Older days: inherited "any row = done" rule.

        The read transaction is rolled back before returning, because the
        caller fetches from ENTSO-E next and production kills sessions that
        sit idle in a transaction.
        """
        today = datetime.now(PRAGUE_TZ).date()
        if target_date < today - timedelta(days=self.STRICT_DAYS_BACK):
            try:
                return super()._areas_with_data(conn, target_date, areas)
            finally:
                conn.rollback()

        day_start = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=PRAGUE_TZ)
        day_end = datetime.combine(target_date + timedelta(days=1), datetime.min.time()).replace(tzinfo=PRAGUE_TZ)
        # Subtract in UTC: aware datetimes with the same tzinfo subtract by wall clock.
        expected = int((day_end.astimezone(timezone.utc) - day_start.astimezone(timezone.utc)).total_seconds() // 900)

        not_null = " AND ".join(f"{c} IS NOT NULL" for c in self.BORDER_COLUMNS)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT area_id FROM {self.TABLE_NAME} "
                    f"WHERE trade_date = %s AND country_code = ANY(%s) AND area_id = ANY(%s) "
                    f"GROUP BY area_id "
                    f"HAVING COUNT(*) FILTER (WHERE {not_null}) >= %s",
                    (target_date, [a[3] for a in areas], [a[0] for a in areas], expected)
                )
                return {row[0] for row in cur.fetchall()}
        finally:
            conn.rollback()

    def _upsert_merge(self, conn, records: List[Tuple]) -> int:
        """
        Upsert that never overwrites a stored border value with NULL.

        bulk_upsert() sets every column to EXCLUDED.col, so a later fetch that
        got fewer borders would blank out values stored earlier. Here each
        border column is COALESCE(EXCLUDED.col, stored col), and the total is
        recomputed from the merged values (NULL unless all four are present).
        Unchanged rows are skipped so re-fetching an incomplete day does not
        bump updated_at.
        """
        if not records:
            return 0
        t = self.TABLE_NAME
        merged = {c: f"COALESCE(EXCLUDED.{c}, {t}.{c})" for c in self.BORDER_COLUMNS}
        total_expr = (
            "CASE WHEN " + " AND ".join(f"{m} IS NOT NULL" for m in merged.values())
            + " THEN " + " + ".join(merged.values()) + " ELSE NULL END"
        )
        set_parts = ["trade_date = EXCLUDED.trade_date", "period = EXCLUDED.period",
                     "time_interval = EXCLUDED.time_interval"]
        set_parts += [f"{c} = {m}" for c, m in merged.items()]
        set_parts += [f"{self.TOTAL_COLUMN} = {total_expr}", "updated_at = CURRENT_TIMESTAMP"]
        changed = [f"{t}.{c} IS DISTINCT FROM {m}" for c, m in merged.items()]
        changed.append(f"{t}.{self.TOTAL_COLUMN} IS DISTINCT FROM {total_expr}")

        query = (
            f"INSERT INTO {t} ({', '.join(self.COLUMNS)}) VALUES %s "
            f"ON CONFLICT ({', '.join(self.CONFLICT_COLUMNS)}) "
            f"DO UPDATE SET {', '.join(set_parts)} WHERE {' OR '.join(changed)}"
        )
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, records, page_size=1000)
            conn.commit()
            self.logger.debug(f"Upserted {len(records)} records to {t}")
            return len(records)
        except Exception as e:
            conn.rollback()
            self.logger.error(f"✗ Bulk upsert failed: {e}")
            raise
        finally:
            cursor.close()

    def _init_client(self) -> bool:
        """Initialize ENTSO-E client."""
        self.logger.debug("Initializing ENTSO-E client...")
        try:
            self.client = EntsoeClient()
            self.logger.debug("Client initialized")
            return True
        except Exception as e:
            self.logger.error(f"✗ Client initialization failed: {e}")
            return False

    def _log_border_failure(self, label: str, error: Exception, neighbor_key: str = None) -> None:
        """
        Log a failed border fetch: INFO when the data is simply not published yet.

        Any failure other than "No matching data" taints the neighbour: the
        other direction may have been fetched, and a one-direction value would
        be a wrong net exchange, so the neighbour's column is written as NULL.
        """
        if neighbor_key and "No matching data" not in str(error):
            self._tainted_neighbors.add(neighbor_key)
        if self.is_data_unavailable_error(error):
            self.logger.info(f"    No data yet for {label}: {error}")
        else:
            self.logger.warning(f"    Failed to fetch {label}: {error}")

    def _fetch_all_borders(self, period_start, period_end, area_code: str) -> Dict[str, str]:
        """
        Fetch A09 XML for all borders of a specific area.

        For CZ, fetches two directions per neighbor:
        - CZ -> Neighbor (export, negative)
        - Neighbor -> CZ (import, positive)

        Returns:
            Dict mapping border key to XML content
        """
        xml_data = {}
        self._tainted_neighbors = set()

        for neighbor_key, neighbor_eic in CZ_NEIGHBORS.items():
            # Fetch CZ -> Neighbor (export from CZ)
            try:
                self.logger.debug(f"    Fetching CZ -> {neighbor_key.upper()} (export)...")
                xml_export = self.client.fetch_scheduled_exchanges(
                    period_start, period_end,
                    in_domain=neighbor_eic,
                    out_domain=area_code
                )
                xml_data[f"{neighbor_key}_export"] = xml_export
            except Exception as e:
                self._log_border_failure(f"CZ -> {neighbor_key.upper()}", e, neighbor_key)

            # Fetch Neighbor -> CZ (import to CZ)
            try:
                self.logger.debug(f"    Fetching {neighbor_key.upper()} -> CZ (import)...")
                xml_import = self.client.fetch_scheduled_exchanges(
                    period_start, period_end,
                    in_domain=area_code,
                    out_domain=neighbor_eic
                )
                xml_data[f"{neighbor_key}_import"] = xml_import
            except Exception as e:
                self._log_border_failure(f"{neighbor_key.upper()} -> CZ", e, neighbor_key)

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
                f'entsoe_sched_flow_{country_code.lower()}_{border_key}_{start_str}_{end_str}.xml',
                period_start
            )
            self.save_xml(xml_content, output_file)
            saved_files[border_key] = output_file

        self.logger.debug(f"    Saved {len(saved_files)} XML files")
        return saved_files

    def _parse_data(
        self, xml_files: Dict[str, str], period_start, period_end,
        area_id: int, country_code: str, area_code: str
    ) -> List[dict]:
        """Parse all XML files into wide-format records."""
        parser = ScheduledExchangesParser(area_id=area_id, country_code=country_code)

        for border_key, xml_file in xml_files.items():
            neighbor_key = border_key.rsplit('_', 1)[0]
            try:
                # Determine in/out domains based on border key
                if border_key.endswith('_export'):
                    in_domain = CZ_NEIGHBORS[neighbor_key]
                    out_domain = area_code
                else:  # _import
                    in_domain = area_code
                    out_domain = CZ_NEIGHBORS[neighbor_key]

                parser.parse_xml(str(xml_file), in_domain=in_domain, out_domain=out_domain)
                self.logger.debug(f"    Parsed: {border_key}")
            except Exception as e:
                self.logger.warning(f"    Failed to parse {border_key}: {e}")
                self._tainted_neighbors.add(neighbor_key)

        records = parser.get_wide_format_records()
        # A neighbour with a real failure in one direction would carry a
        # one-direction value as its net exchange: store NULL instead.
        for neighbor_key in self._tainted_neighbors:
            column = f"scheduled_{neighbor_key}_mw"
            for record in records:
                record[column] = None
        return records

    def _prepare_records(self, data: List[dict]) -> List[Tuple]:
        """Convert parsed data to tuples for bulk insert.

        scheduled_total_net_mw is NULL unless all four borders are present.
        """
        records = []
        for record in data:
            borders = [record.get(c) for c in self.BORDER_COLUMNS]
            total = sum(borders) if all(b is not None for b in borders) else None
            records.append((
                record['trade_date'],
                record['period'],
                record['area_id'],
                record['country_code'],
                record['time_interval'],
                record['delivery_datetime'],
                record.get('scheduled_de_mw'),
                record.get('scheduled_at_mw'),
                record.get('scheduled_pl_mw'),
                record.get('scheduled_sk_mw'),
                total
            ))
        return records

    def _process_area(
        self, period_start, period_end,
        area_id: int, area_code: str, display_label: str, country_code: str,
        conn=None
    ) -> int:
        """Process scheduled exchanges for a single area."""
        self.logger.debug(f"  Processing {display_label} (area_id={area_id}, country={country_code})...")

        try:
            # Fetch all border data
            xml_data = self._fetch_all_borders(period_start, period_end, area_code)

            if not xml_data:
                self.logger.info(f"{self.RUNNER_NAME}: no data for {country_code}")
                return 0

            # Save XML files
            xml_files = self._save_xml_files(xml_data, period_start, period_end, display_label)

            # Parse data with area_id and country_code
            data = self._parse_data(xml_files, period_start, period_end, area_id, country_code, area_code)

            if not data:
                self.logger.info(f"{self.RUNNER_NAME}: no data for {country_code}")
                return 0

            self.logger.debug(f"    Parsed {len(data)} records")

            # Prepare records for bulk insert
            records = self._prepare_records(data)

            # Upload to database
            if not self.dry_run and conn:
                self._upsert_merge(conn, records)
            elif self.dry_run:
                self.logger.info(f"    DRY RUN - Would upload {len(records)} records")

            self.track_country(country_code, len(records))
            return len(records)

        except Exception as e:
            if self.is_data_unavailable_error(e):
                self.logger.info(f"{self.RUNNER_NAME}: {country_code} not available for [{period_start.strftime('%Y-%m-%d %H:%M')}-{period_end.strftime('%Y-%m-%d %H:%M')}]")
            else:
                # WARNING, not ERROR: one upstream outage x 6 countries x 4
                # runs/hour x ~14 runners would otherwise flood Sentry. The
                # aggregate is emitted once per run by print_footer().
                self.record_area_failure(country_code, e)
            return 0

    def _process_chunk(self, period_start, period_end, conn=None) -> int:
        """Process a single time chunk for CZ (primary area with border data)."""
        self.logger.debug(
            f"Processing: {period_start.strftime('%Y-%m-%d %H:%M')} "
            f"to {period_end.strftime('%Y-%m-%d %H:%M')} UTC"
        )

        # Currently only process CZ borders (area_id=1)
        return self._process_area(
            period_start, period_end,
            area_id=1, area_code=CZ_BZN, display_label="CZ", country_code="CZ",
            conn=conn
        )

    def run(self) -> bool:
        """Execute the unified scheduled cross-border flows data pipeline."""
        self.print_header()

        if not self._init_client():
            return False

        total_records = 0

        try:
            if self.is_backfill:
                self.logger.debug("")
                self.logger.debug("Processing CZ scheduled cross-border flows")
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
                # Day-ahead data: tomorrow (as soon as ENTSO-E publishes it) + today
                # + backfill up to 7 days if service was down.
                # Yesterday, today and tomorrow are re-fetched every run until all
                # four borders are present for every interval (see _areas_with_data).
                today = datetime.now(PRAGUE_TZ).date()
                for day_offset in range(-7, 2):
                    target_date = today + timedelta(days=day_offset)
                    total_records += self._run_with_availability_check(
                        target_date, [(1, CZ_BZN, "CZ", "CZ")]
                    )

            self.logger.debug("")
            self.logger.info(self.format_summary(total_records))
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
    UnifiedSchedFlowRunner.main()
