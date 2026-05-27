#!/usr/bin/env python3
"""
Liquidate Expired Positions Runner.

Lists expired positions from the Trader API and liquidates each by
POSTing the OTE imbalance settlement price (converted via CNB CZK/EUR)
back to the Trader API. Mirrors the manual "Liquidate All" UI flow but
runs unattended.

Scheduled at `15 */2 * * *` — 15 minutes after the OTE imbalance fetch
at `0 */2 * * *`, giving the upload pipeline time to populate
`finance.ote_prices_imbalance` for new periods.

Usage:
    python3 -m runners.liquidate_expired_positions_runner [--debug] [--dry-run]
"""

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import sentry_init  # noqa: F401
sentry_init.set_module("liquidator")

from runners.base_runner import BaseRunner
from liquidator.liquidator import process_position
from liquidator.trader_api_client import TraderApiClient


class LiquidateExpiredPositionsRunner(BaseRunner):
    RUNNER_NAME = "Liquidate Expired Positions Runner"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = None

    def _init_client(self) -> None:
        self.client = TraderApiClient()

    def run(self) -> bool:
        try:
            self._init_client()
        except ValueError as e:
            self.logger.error(f"{self.RUNNER_NAME} config error: {e}")
            return False

        try:
            positions = self.client.list_expired_positions()
        except Exception as e:
            self.logger.error(
                f"{self.RUNNER_NAME}: failed to list expired positions: {e}"
            )
            if self.debug:
                import traceback
                traceback.print_exc()
            return False

        if not positions:
            self.logger.info(f"{self.RUNNER_NAME}: no expired positions")
            return True

        self.logger.info(
            f"{self.RUNNER_NAME}: {len(positions)} expired position(s) to process"
        )

        any_failure = False
        statuses: Counter = Counter()

        try:
            with self.database_connection() as conn:
                for pos in positions:
                    try:
                        result = process_position(
                            conn, self.client, pos, self.logger, self.dry_run
                        )
                    except Exception as e:
                        any_failure = True
                        self.logger.error(
                            f"  Exception processing {pos.get('position_id')}: {e}"
                        )
                        if self.debug:
                            import traceback
                            traceback.print_exc()
                        statuses["exception"] += 1
                        continue

                    statuses[result.status] += 1
                    if result.status in ("http_error", "exception"):
                        any_failure = True
        except Exception as e:
            self.logger.error(f"{self.RUNNER_NAME} DB error: {e}")
            return False

        summary = ", ".join(f"{k}={v}" for k, v in sorted(statuses.items()))
        self.logger.info(f"{self.RUNNER_NAME}: {summary}")
        return not any_failure


if __name__ == '__main__':
    LiquidateExpiredPositionsRunner.main()
