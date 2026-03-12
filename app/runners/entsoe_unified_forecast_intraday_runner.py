#!/usr/bin/env python3
"""
ENTSO-E Intraday Generation Forecast Runner (A69 / A40).

Fetches intraday wind/solar generation forecasts for all active areas.
Normal mode: checks DB for TODAY's data, fetches only if missing.
Backfill mode: fetches all chunks without availability check.

Usage:
    python3 -m runners.entsoe_unified_forecast_intraday_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_unified_forecast_intraday_runner --start 2024-12-01 --end 2024-12-22
"""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.forecast_runner_base import BaseForecastRunner, PRAGUE_TZ


class IntradayForecastRunner(BaseForecastRunner):
    """Intraday (A40) generation forecast runner."""

    RUNNER_NAME = "ENTSO-E ID Forecast Runner (A40)"
    TABLE_NAME = "entsoe_generation_forecast_intraday"
    PROCESS_TYPE = "A40"

    def run(self) -> bool:
        self.print_header()

        if not self._init_client():
            return False

        try:
            if self.is_backfill:
                total_records = self._run_backfill()
            else:
                # Target: today in Prague timezone
                now_prague = datetime.now(PRAGUE_TZ)
                target_date = now_prague.date()
                self.logger.debug(f"Target date: {target_date} (today)")
                total_records = self._run_with_availability_check(target_date)

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
    IntradayForecastRunner.main()
