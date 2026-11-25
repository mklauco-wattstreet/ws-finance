#!/usr/bin/env python3
"""
ENTSO-E Unified Balancing Energy Runner.

Fetches activated balancing energy prices (A84) for all active areas into the
partitioned entsoe_balancing_energy table (EUR/MWh, 15 min):

    processType A16  fcr_* (A95), afrr_* (A96), mfrr_* (A97 standard product),
                     mfrr_specific_* (A97 specific product), rr_* (A98)
    processType A68  afrr_local_* (A96, local selection)

Each row writes only the columns its documents cover, so a series that is missing
from an answer never overwrites stored prices with NULL.

Default run: rolling 3 hours; the 03:14 run re-fetches the last 7 days, which
also fills rows missed by failed runs.

Usage:
    python3 -m runners.entsoe_unified_balancing_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_unified_balancing_runner --start 2024-12-01 --end 2024-12-22
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, FrozenSet, List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from entsoe.client import EntsoeClient
from entsoe.balancing_price_parser import PRICE_COLUMNS, BalancingPriceParser
from entsoe.query import fetch_document, fmt_period
from entsoe.constants import ACTIVE_GENERATION_AREAS


class UnifiedBalancingRunner(BaseRunner):
    """Unified runner for ENTSO-E Activated Balancing Energy Prices (A84) - All Areas."""

    RUNNER_NAME = "ENTSO-E Unified Balancing Energy Runner"

    # Table configuration - partitioned by country_code
    TABLE_NAME = "entsoe_balancing_energy"
    COLUMNS = [
        "trade_date", "period", "area_id", "country_code", "time_interval",
        "delivery_datetime",
    ] + PRICE_COLUMNS
    CONFLICT_COLUMNS = ["delivery_datetime", "area_id", "country_code"]

    PROCESS_TYPES = ("A16", "A68")
    REFETCH_DAYS = 7

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = None

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

    def _fetch_rows(
        self, period_start, period_end, process_type: str,
        area_id: int, area_code: str, display_label: str, country_code: str
    ) -> List[dict]:
        """Fetch, save and parse the documents of one area and process type."""
        documents = fetch_document(self.client, {
            "documentType": "A84",
            "processType": process_type,
            "controlArea_Domain": area_code,
            "periodStart": fmt_period(period_start),
            "periodEnd": fmt_period(period_end),
        })
        rows: List[dict] = []
        suffix = "" if process_type == "A16" else f"_{process_type.lower()}"
        for number, xml_content in enumerate(documents or []):
            part = f"_{number}" if number else ""
            self.save_xml(xml_content, self.get_output_path(
                f"entsoe_balancing_{display_label.lower()}_"
                f"{fmt_period(period_start)}_{fmt_period(period_end)}{suffix}{part}.xml",
                period_start
            ))
            parser = BalancingPriceParser(area_id, country_code, process_type)
            rows.extend(parser.parse_xml_content(xml_content))
        return rows

    def _upsert(self, conn, rows: Dict[datetime, dict]) -> None:
        """Upsert in groups of equally covered rows, updating only the covered columns."""
        groups: Dict[FrozenSet[str], List[dict]] = {}
        for row in rows.values():
            groups.setdefault(frozenset(row["prices"]), []).append(row)
        for covered, members in groups.items():
            prices = [c for c in PRICE_COLUMNS if c in covered]
            keys = [c for c in self.COLUMNS if c not in PRICE_COLUMNS]
            records: List[Tuple] = [
                tuple(r[k] for k in keys) + tuple(r["prices"][c] for c in prices) for r in members
            ]
            self.bulk_upsert(
                conn, self.TABLE_NAME, keys + prices, records, self.CONFLICT_COLUMNS,
                update_columns=prices, skip_unchanged=True,
            )

    def _process_area(
        self, period_start, period_end,
        area_id: int, area_code: str, display_label: str, country_code: str,
        conn=None
    ) -> int:
        """Process a single area: fetch, parse, and upload every process type."""
        total = 0
        for process_type in self.PROCESS_TYPES:
            try:
                rows: Dict[datetime, dict] = {}
                for row in self._fetch_rows(
                    period_start, period_end, process_type,
                    area_id, area_code, display_label, country_code
                ):
                    existing = rows.setdefault(row["delivery_datetime"], row)
                    existing["prices"].update(row["prices"])
                if not rows:
                    self.logger.debug(f"  {display_label} {process_type}: no data")
                    continue
                if self.dry_run:
                    self.logger.info(f"    DRY RUN - Would upload {len(rows)} {display_label} {process_type} records")
                    self.logger.debug(f"    Last row: {rows[max(rows)]}")
                elif conn:
                    self._upsert(conn, rows)
                total += len(rows)
            except Exception as e:
                # WARNING, not ERROR: one upstream outage x areas x 4 runs/hour
                # would flood Sentry; print_footer() emits the aggregate.
                if conn:
                    conn.rollback()
                self.record_area_failure(f"{display_label}/{process_type}", e)
        if total:
            self.track_country(country_code, total)
        return total

    def _process_chunk(self, period_start, period_end, conn=None) -> int:
        """Process a single time chunk for ALL areas."""
        self.logger.debug(
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
                self.logger.debug("")
                self.logger.debug(f"Processing {len(ACTIVE_GENERATION_AREAS)} areas: "
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
                now = datetime.now(PRAGUE_TZ)
                period_start, period_end = self.get_time_range(hours=3)
                # One run per night re-fetches the last days in full.
                if now.hour == 3 and now.minute < 29:
                    first_day = now.date() - timedelta(days=self.REFETCH_DAYS)
                    night_start = datetime.combine(
                        first_day, datetime.min.time()
                    ).replace(tzinfo=PRAGUE_TZ).astimezone(timezone.utc)
                    today_start = datetime.combine(
                        now.date(), datetime.min.time()
                    ).replace(tzinfo=PRAGUE_TZ).astimezone(timezone.utc)
                    windows = [(period_start, period_end), (night_start, today_start)]
                else:
                    windows = [(period_start, period_end)]
                if self.dry_run:
                    for start, end in windows:
                        total_records += self._process_chunk(start, end)
                else:
                    with self.database_connection() as conn:
                        for start, end in windows:
                            total_records += self._process_chunk(start, end, conn)

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
    UnifiedBalancingRunner.main()
