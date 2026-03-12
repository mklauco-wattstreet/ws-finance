#!/usr/bin/env python3
"""
CNB Exchange Rate Runner.

Fetches daily CZK/EUR exchange rate from Czech National Bank and stores it.
CNB publishes at ~14:30 CET on business days.

Usage:
    python3 -m runners.cnb_exchange_rate_runner [--debug] [--dry-run]
    python3 -m runners.cnb_exchange_rate_runner --start 2026-01-01 --end 2026-03-12 --debug
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from cnb.cnb_client import CnbClient


class CnbExchangeRateRunner(BaseRunner):
    """Runner for CNB CZK/EUR daily exchange rate."""

    RUNNER_NAME = "CNB Exchange Rate Runner"
    TABLE_NAME = "cnb_exchange_rate"
    COLUMNS = ["rate_date", "czk_eur"]
    CONFLICT_COLUMNS = ["rate_date"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = None

    def _init_client(self) -> bool:
        self.client = CnbClient()
        return True

    def _rate_exists(self, conn, rate_date) -> bool:
        """Check if rate already exists for given date."""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM cnb_exchange_rate WHERE rate_date = %s LIMIT 1",
                (rate_date,)
            )
            return cur.fetchone() is not None

    def run(self) -> bool:
        """Execute the CNB exchange rate pipeline."""
        self._init_client()

        try:
            if self.is_backfill:
                end = self.end_date or datetime.now(PRAGUE_TZ).date()
                start = self.start_date or end - timedelta(days=7)
                self.logger.debug(f"Backfill: {start} to {end}")

                rates = self.client.fetch_rates_range(start, end)
                if not rates:
                    self.logger.info(f"{self.RUNNER_NAME}: no rates fetched")
                    return True

                with self.database_connection() as conn:
                    # Filter out existing dates
                    new_rates = [r for r in rates if not self._rate_exists(conn, r["rate_date"])]
                    if not new_rates:
                        self.logger.info(f"{self.RUNNER_NAME}: all {len(rates)} dates already exist")
                        return True

                    records = [(r["rate_date"], r["czk_eur"]) for r in new_rates]
                    self.bulk_upsert(
                        conn, self.TABLE_NAME, self.COLUMNS,
                        records, self.CONFLICT_COLUMNS
                    )
                    self.logger.info(f"{self.RUNNER_NAME}: {len(records)} rates inserted ({start} to {end})")
            else:
                today = datetime.now(PRAGUE_TZ).date()
                if today.weekday() >= 5:
                    self.logger.debug(f"{self.RUNNER_NAME}: weekend, skipping")
                    return True

                with self.database_connection() as conn:
                    if self._rate_exists(conn, today):
                        self.logger.debug(f"{self.RUNNER_NAME}: rate for {today} already exists")
                        return True

                    rate = self.client.fetch_rate(today)
                    records = [(rate["rate_date"], rate["czk_eur"])]
                    self.bulk_upsert(
                        conn, self.TABLE_NAME, self.COLUMNS,
                        records, self.CONFLICT_COLUMNS
                    )
                    self.logger.info(f"{self.RUNNER_NAME}: {today} CZK/EUR={rate['czk_eur']}")

            return True

        except Exception as e:
            self.logger.error(f"{self.RUNNER_NAME} failed: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return False


if __name__ == '__main__':
    CnbExchangeRateRunner.main()
