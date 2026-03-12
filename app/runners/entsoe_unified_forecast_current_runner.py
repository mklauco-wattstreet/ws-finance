#!/usr/bin/env python3
"""
ENTSO-E Current Generation Forecast Runner (A69 / A18).

Fetches current/rolling wind/solar generation forecasts for all active areas.
Normal mode: always fetches (now - 3h) to end-of-today (data updates continuously).
Backfill mode: fetches all chunks without availability check.

Usage:
    python3 -m runners.entsoe_unified_forecast_current_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_unified_forecast_current_runner --start 2024-12-01 --end 2024-12-22
"""

import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.forecast_runner_base import BaseForecastRunner, PRAGUE_TZ
from entsoe.constants import ACTIVE_CURRENT_FORECAST_AREAS


class CurrentForecastRunner(BaseForecastRunner):
    """Current (A18) generation forecast runner."""

    RUNNER_NAME = "ENTSO-E Current Forecast Runner (A18)"
    TABLE_NAME = "entsoe_generation_forecast_current"
    PROCESS_TYPE = "A18"
    ACTIVE_AREAS = ACTIVE_CURRENT_FORECAST_AREAS

    def run(self) -> bool:
        self.print_header()

        if not self._init_client():
            return False

        try:
            if self.is_backfill:
                total_records = self._run_backfill()
            else:
                # Rolling window: (now - 3h) to end of today (Prague)
                now_utc = datetime.now(timezone.utc)
                period_start = now_utc - timedelta(hours=3)
                # Round start down to nearest 15 minutes
                minutes = (period_start.minute // 15) * 15
                period_start = period_start.replace(minute=minutes, second=0, microsecond=0)

                # End at midnight tonight Prague -> UTC
                now_prague = now_utc.astimezone(PRAGUE_TZ)
                end_prague = datetime.combine(
                    now_prague.date() + timedelta(days=1),
                    datetime.min.time()
                ).replace(tzinfo=PRAGUE_TZ)
                period_end = end_prague.astimezone(timezone.utc)

                self.logger.debug(
                    f"Period (UTC): {period_start.strftime('%Y-%m-%d %H:%M')} "
                    f"to {period_end.strftime('%Y-%m-%d %H:%M')}"
                )
                total_records = self._run_continuous(period_start, period_end)

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
    CurrentForecastRunner.main()
