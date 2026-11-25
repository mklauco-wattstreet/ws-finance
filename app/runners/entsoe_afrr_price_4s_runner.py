#!/usr/bin/env python3
"""
ENTSO-E 4-second aFRR Price Runner (documentType A84, processType A67), CZ.

-> entsoe_afrr_price_4s. One day per request (about 5 MB). Default run: the last
3 hours; the 03:14 run re-fetches the last two full days as well.

Usage:
    python3 -m runners.entsoe_afrr_price_4s_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_afrr_price_4s_runner --start 2026-11-01 --end 2026-11-24
"""

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from entsoe.balancing_price_parser import AfrrPrice4sParser
from entsoe.client import EntsoeClient
from entsoe.constants import CZ_BZN
from entsoe.query import fetch_document, fmt_period


class AfrrPrice4sRunner(BaseRunner):
    """Runner for 4-second aFRR prices."""

    RUNNER_NAME = "ENTSO-E 4-second aFRR Price Runner"
    TABLE_NAME = "entsoe_afrr_price_4s"
    KEY = ["delivery_datetime", "area_id", "country_code"]
    COLUMNS = KEY + ["trade_date", "afrr_up_price_eur", "afrr_down_price_eur"]
    AREA = (1, CZ_BZN, "CZ")
    REFETCH_DAYS = 2

    @staticmethod
    def _day(day: date) -> Tuple[datetime, datetime]:
        start, end = (
            datetime.combine(d, datetime.min.time()).replace(tzinfo=PRAGUE_TZ).astimezone(timezone.utc)
            for d in (day, day + timedelta(days=1))
        )
        return start, end

    def _windows(self) -> List[Tuple[datetime, datetime]]:
        now = datetime.now(PRAGUE_TZ)
        today = now.date()
        recent = self.get_time_range(hours=3)
        if self.is_backfill:
            last = min(self.end_date or today, today)
            first = self.start_date or last - timedelta(days=7)
            windows = [self._day(first + timedelta(days=n)) for n in range((last - first).days + 1)]
            return [(s, min(e, recent[1])) for s, e in windows if s < recent[1]]
        windows = [recent]
        if now.hour == 3 and now.minute < 29:
            windows += [self._day(today - timedelta(days=n)) for n in range(1, self.REFETCH_DAYS + 1)]
        return windows

    def _process(self, conn, client: EntsoeClient, start: datetime, end: datetime) -> int:
        area_id, eic, country = self.AREA
        documents = fetch_document(client, {
            "documentType": "A84",
            "processType": "A67",
            "controlArea_Domain": eic,
            "periodStart": fmt_period(start),
            "periodEnd": fmt_period(end),
        }, timeout=(10, 180))
        parser = AfrrPrice4sParser(area_id, country)
        rows = {r["delivery_datetime"]: r for xml in documents or [] for r in parser.parse_xml_content(xml)}
        label = f"{start:%Y-%m-%d %H:%M}Z - {end:%Y-%m-%d %H:%M}Z"
        if not rows:
            self.logger.info(f"  {label}: no data")
            return 0
        self.logger.info(f"  {label}: {len(rows)} rows")
        records = [tuple(rows[k][c] for c in self.COLUMNS) for k in sorted(rows)]
        written = self.bulk_upsert(conn, self.TABLE_NAME, self.COLUMNS, records, self.KEY, skip_unchanged=True)
        self.track_country(country, written)
        return written

    def run(self) -> bool:
        self.print_header()
        total = 0
        try:
            client = EntsoeClient()
            with self.database_connection() as conn:
                for start, end in self._windows():
                    if self.deadline_exceeded():
                        break
                    try:
                        total += self._process(conn, client, start, end)
                    except Exception as e:
                        conn.rollback()
                        self.record_area_failure(f"CZ {start:%Y-%m-%d %H:%M}Z", e)
            self.logger.info(self.format_summary(total))
            self.print_footer(success=not self.area_failures)
            return not self.area_failures
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            self.print_footer(success=False)
            return False


if __name__ == "__main__":
    AfrrPrice4sRunner.main()
