#!/usr/bin/env python3
"""
FDW Sync Runner for pblctradeconf (IDC public trade data).

Syncs new rows from production via Foreign Data Wrapper.
Uses tradeExecTime as the high-water mark (append-only).

Usage:
    python3 -m runners.fdw_sync_pblctradeconf_runner [--debug] [--dry-run]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner


class FdwSyncPblctradeconfRunner(BaseRunner):
    """Runner for syncing pblctradeconf from production via FDW."""

    RUNNER_NAME = "FDW Sync pblctradeconf"

    SYNC_SQL = """
        INSERT INTO public.pblctradeconf (
            "ws_ote_ts", "ws_our_ts", "ws_ote_dt", "ws_our_dt",
            "ws_headers", "ws_routing_key", "ws_marketID",
            "tradeExecTime", contract, contract_start, contract_end,
            contract_duration, "px_eur_mwh", "qty_mwh", "qty_mw",
            "tradeId", "revisionNo", state
        )
        SELECT
            "ws_ote_ts", "ws_our_ts", "ws_ote_dt", "ws_our_dt",
            "ws_headers", "ws_routing_key", "ws_marketID",
            "tradeExecTime", contract, contract_start, contract_end,
            contract_duration, "px_eur_mwh", "qty_mwh", "qty_mw",
            "tradeId", "revisionNo", state
        FROM prod_fdw.pblctradeconf
        WHERE "tradeExecTime" > COALESCE(%s, '1970-01-01'::timestamp)
    """

    def run(self) -> bool:
        try:
            with self.database_connection() as conn:
                cur = conn.cursor()
                try:
                    # Get local high-water mark
                    cur.execute('SELECT MAX("tradeExecTime") FROM public.pblctradeconf')
                    max_ts = cur.fetchone()[0]
                    self.logger.debug(f"Local max tradeExecTime: {max_ts or 'empty'}")

                    if self.dry_run:
                        cur.execute(
                            'SELECT COUNT(*) FROM prod_fdw.pblctradeconf WHERE "tradeExecTime" > COALESCE(%s, \'1970-01-01\'::timestamp)',
                            (max_ts,)
                        )
                        pending = cur.fetchone()[0]
                        self.logger.info(f"{self.RUNNER_NAME}: DRY RUN — {pending} rows pending")
                        return True

                    # Insert new rows
                    cur.execute(self.SYNC_SQL, (max_ts,))
                    count = cur.rowcount
                    conn.commit()

                    if count > 0:
                        self.logger.info(f"{self.RUNNER_NAME}: {count} rows synced (from {max_ts})")
                    else:
                        self.logger.debug(f"{self.RUNNER_NAME}: up to date")

                finally:
                    cur.close()

            return True

        except Exception as e:
            self.logger.error(f"{self.RUNNER_NAME} failed: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return False


if __name__ == '__main__':
    FdwSyncPblctradeconfRunner.main()
