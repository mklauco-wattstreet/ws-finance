#!/usr/bin/env python3
"""
ENTSO-E Balancing Bids Runner: every Czech aFRR and mFRR energy bid.

documentType A37, businessType B74, processType A51 (afrr) / A47 (mfrr), one
request series per 15-minute slot: at most 100 bids per answer, paged by offset
-> entsoe_balancing_bids. Requests run in parallel, database writes do not.

Default run: the last 3 hours, plus up to HEAL_SLOTS missing slots of the last
7 days. Backfill (--start/--end): every slot of those days that has no rows yet
(--refetch: every slot), so an interrupted backfill can simply be restarted.
ENTSO-E appears to keep only about 93 days of bids.

Usage:
    python3 -m runners.entsoe_balancing_bids_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_balancing_bids_runner --start 2026-08-24 --end 2026-11-25 --workers 8
"""

import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner
from entsoe.balancing_bids_parser import BalancingBidsParser
from entsoe.client import EntsoeClient
from entsoe.constants import CZ_BZN
from entsoe.query import fetch_document, fmt_period

Slot = Tuple[datetime, str]


class BalancingBidsRunner(BaseRunner):
    """Runner for individual balancing energy bids."""

    RUNNER_NAME = "ENTSO-E Balancing Bids Runner"
    TABLE_NAME = "entsoe_balancing_bids"
    KEY = ["delivery_datetime", "area_id", "country_code", "process", "bid_mrid"]
    COLUMNS = KEY + [
        "delivery_end", "trade_date", "period", "time_interval", "direction", "product",
        "acquiring_eic", "quantity_mw", "price_eur", "divisible", "status",
    ]
    AREA = (1, CZ_BZN, "CZ")
    PROCESSES: Dict[str, str] = {"afrr": "A51", "mfrr": "A47"}
    PAGE_SIZE = 100
    MAX_PAGES = 100
    SLOT = timedelta(minutes=15)
    DEFAULT_HOURS = 3
    HEAL_DAYS = 7
    HEAL_SLOTS = 24

    def __init__(self, workers: int = 4, refetch: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.workers = workers
        self.refetch = refetch
        self._local = threading.local()

    @classmethod
    def create_argument_parser(cls):
        parser = super().create_argument_parser()
        parser.add_argument("--workers", type=int, default=4, choices=range(1, 13), metavar="1-12",
                            help="Parallel requests (default 4)")
        parser.add_argument("--refetch", action="store_true",
                            help="Backfill: also fetch slots that already have rows")
        return parser

    @classmethod
    def main(cls) -> None:
        args = cls.create_argument_parser().parse_args()
        try:
            start_date, end_date = cls.parse_date(args.start), cls.parse_date(args.end)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)
        runner = cls(
            workers=args.workers, refetch=args.refetch, debug=args.debug, dry_run=args.dry_run,
            start_date=start_date, end_date=end_date, max_runtime=args.max_runtime,
        )
        sys.exit(0 if runner.run() else 1)

    # ---------------------------------------------------------------- slots

    def _slots(self, start: datetime, end: datetime) -> List[Slot]:
        slots: List[Slot] = []
        current = start
        while current < end:
            slots.extend((current, process) for process in self.PROCESSES)
            current += self.SLOT
        return slots

    def _existing(self, conn, start: datetime, end: datetime) -> Set[Slot]:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT delivery_datetime, process FROM {self.TABLE_NAME} "
                f"WHERE country_code = %s AND area_id = %s AND delivery_datetime >= %s AND delivery_datetime < %s",
                (self.AREA[2], self.AREA[0], start, end),
            )
            found = {(row[0].astimezone(timezone.utc), row[1]) for row in cur.fetchall()}
        conn.rollback()  # never idle in a transaction while fetching
        return found

    def _plan(self, conn) -> List[Slot]:
        _, now_slot = self.get_time_range(hours=1)
        horizon = now_slot + self.SLOT
        if self.is_backfill:
            chunks = list(self.get_backfill_chunks())
            start, end = chunks[0][0], min(chunks[-1][1], horizon)
            slots = self._slots(start, end)
            if not self.refetch:
                existing = self._existing(conn, start, end)
                slots = [s for s in slots if s not in existing]
            return slots
        start = now_slot - timedelta(hours=self.DEFAULT_HOURS)
        recent = self._slots(start, horizon)
        heal_start = now_slot - timedelta(days=self.HEAL_DAYS)
        existing = self._existing(conn, heal_start, start)
        missing = [s for s in self._slots(heal_start, start) if s not in existing]
        return recent + missing[-self.HEAL_SLOTS:]

    # ---------------------------------------------------------------- fetch

    def _client(self) -> EntsoeClient:
        if not hasattr(self._local, "client"):
            self._local.client = EntsoeClient()
        return self._local.client

    def _fetch_slot(self, slot: Slot) -> Optional[List[dict]]:
        """All bids of one slot and process; None when the deadline passed before it started."""
        if self.deadline_exceeded():
            return None
        start, process = slot
        area_id, eic, country = self.AREA
        parser = BalancingBidsParser(area_id, country, process)
        rows: Dict[Tuple[datetime, str], dict] = {}
        for page in range(self.MAX_PAGES):
            documents = fetch_document(self._client(), {
                "documentType": "A37",
                "businessType": "B74",
                "processType": self.PROCESSES[process],
                "connecting_Domain": eic,
                "periodStart": fmt_period(start),
                "periodEnd": fmt_period(start + self.SLOT),
                "offset": str(page * self.PAGE_SIZE),
            })
            bids = [row for xml in documents or [] for row in parser.parse_xml_content(xml)]
            for row in bids:
                rows[(row["delivery_datetime"], row["bid_mrid"])] = row
            if len(bids) < self.PAGE_SIZE:
                return list(rows.values())
        raise ValueError(f"{process} {start:%Y-%m-%d %H:%M}: more than {self.MAX_PAGES} pages")

    def _store(self, conn, rows: List[dict]) -> int:
        records = [tuple(r[c] for c in self.COLUMNS) for r in rows]
        return self.bulk_upsert(
            conn, self.TABLE_NAME, self.COLUMNS, records, self.KEY, skip_unchanged=True,
        )

    # ------------------------------------------------------------------ run

    def run(self) -> bool:
        self.print_header()
        total = empty = 0
        try:
            with self.database_connection() as conn:
                slots = self._plan(conn)
                self.logger.info(f"{self.RUNNER_NAME}: {len(slots)} slots to fetch")
                # Batches keep memory bounded and let a deadline stop the run.
                batch_size = self.workers * 8
                for i in range(0, len(slots), batch_size):
                    if self.deadline_exceeded():
                        break
                    with ThreadPoolExecutor(max_workers=self.workers) as pool:
                        futures = {pool.submit(self._fetch_slot, s): s for s in slots[i:i + batch_size]}
                        for future in as_completed(futures):
                            start, process = futures[future]
                            label = f"CZ {process} {start:%Y-%m-%d %H:%M}Z"
                            try:
                                rows = future.result()
                                if rows is None:
                                    continue
                                if not rows:
                                    empty += 1
                                    self.logger.debug(f"  {label}: no bids")
                                    continue
                                total += self._store(conn, rows)
                                self.track_country("CZ", len(rows))
                                self.logger.debug(f"  {label}: {len(rows)} bids")
                            except Exception as e:
                                conn.rollback()
                                self.record_area_failure(label, e)
                    if self.is_backfill:
                        self.logger.info(f"  {min(i + batch_size, len(slots))}/{len(slots)} slots, {total} bids")
            self.logger.info(
                f"{self.format_summary(total)}; slots: {len(slots)} ({empty} empty, {len(self.area_failures)} failed)"
            )
            self.print_footer(success=not self.area_failures)
            return not self.area_failures
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            self.print_footer(success=False)
            return False


if __name__ == "__main__":
    BalancingBidsRunner.main()
