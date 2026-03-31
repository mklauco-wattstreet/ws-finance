#!/usr/bin/env python3
"""
ENTSO-E Day-Ahead Generation Forecast Runner (A69 / A01).

Fetches day-ahead wind/solar generation forecasts for all active areas.
Normal mode: checks today + tomorrow + backfills up to 7 days if service was down.
Backfill mode: fetches all chunks without availability check.

Usage:
    python3 -m runners.entsoe_unified_forecast_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_unified_forecast_runner --start 2024-12-01 --end 2024-12-22
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.forecast_runner_base import BaseForecastRunner, PRAGUE_TZ


class DayAheadForecastRunner(BaseForecastRunner):
    """Day-ahead (A01) generation forecast runner."""

    RUNNER_NAME = "ENTSO-E DA Forecast Runner (A01)"
    TABLE_NAME = "entsoe_generation_forecast"
    PROCESS_TYPE = "A01"

    def run(self) -> bool:
        self.print_header()

        if not self._init_client():
            return False

        try:
            if self.is_backfill:
                total_records = self._run_backfill()
            else:
                # Check today + tomorrow, and backfill up to 7 days if service was down
                total_records = 0
                today = datetime.now(PRAGUE_TZ).date()
                for day_offset in range(-7, 2):
                    target_date = today + timedelta(days=day_offset)
                    total_records += self._run_with_availability_check(target_date)

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
    DayAheadForecastRunner.main()
