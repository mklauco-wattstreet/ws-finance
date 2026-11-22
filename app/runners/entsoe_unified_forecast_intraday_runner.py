#!/usr/bin/env python3
"""
ENTSO-E Intraday Generation Forecast Runner (A69 / A40).

Fetches intraday wind/solar generation forecasts for all active areas.

The intraday forecast is REVISED through the day, so the first published
version must not be the one that stays in the database.

Normal (cron) mode: on EVERY run, fetches TODAY and TOMORROW (Prague local
days) for ALL active areas, with no skip-if-exists check, and upserts them.
- Rows whose values changed are updated; unchanged rows are not rewritten
  (``skip_unchanged``), so ``updated_at`` only moves when a value really
  changed. ``updated_at`` is set explicitly by ``BaseRunner.bulk_upsert``
  (the table has no trigger).
- A fetch that returns fewer rows than are stored (partial document) never
  deletes anything: this is upsert only.
- "No matching data" (not published yet) is logged at INFO and is not an
  area failure.
- Area order follows ``ACTIVE_GENERATION_AREAS`` (CZ first). The loop is
  area-major (CZ today, CZ tomorrow, next area ...) so CZ data lands first
  even when the run is cut short.
- Request count: one ENTSO-E request per area per day = 8 areas x 2 days =
  16 requests per run (about 80-130 s at 5-8 s each). The DB connection is
  never inside an open transaction while an HTTP request is in flight
  (``bulk_upsert`` commits after every area).

Backfill mode (--start/--end): unchanged, all chunks without availability
check.

Usage:
    python3 -m runners.entsoe_unified_forecast_intraday_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_unified_forecast_intraday_runner --start 2024-12-01 --end 2024-12-22
"""

import sys
from contextlib import nullcontext
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.forecast_runner_base import BaseForecastRunner, PRAGUE_TZ


class IntradayForecastRunner(BaseForecastRunner):
    """Intraday (A40) generation forecast runner."""

    RUNNER_NAME = "ENTSO-E ID Forecast Runner (A40)"
    TABLE_NAME = "entsoe_generation_forecast_intraday"
    PROCESS_TYPE = "A40"

    def bulk_upsert(self, *args, **kwargs):
        """Always guard the UPDATE so unchanged rows keep their updated_at.

        The base forecast path calls bulk_upsert without skip_unchanged; since
        cron mode re-fetches the same days every run, force it on there.
        Backfill keeps the base behaviour.
        """
        if not self.is_backfill:
            kwargs.setdefault("skip_unchanged", True)
        return super().bulk_upsert(*args, **kwargs)

    @staticmethod
    def _day_window_utc(target_date):
        """Prague-local calendar day -> (period_start, period_end) in UTC."""
        start_prague = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=PRAGUE_TZ)
        end_prague = start_prague + timedelta(days=1)
        return start_prague.astimezone(timezone.utc), end_prague.astimezone(timezone.utc)

    def _run_refresh(self) -> int:
        """Cron mode: re-fetch today and tomorrow for every area, every run."""
        today = datetime.now(PRAGUE_TZ).date()
        windows = [self._day_window_utc(d) for d in (today, today + timedelta(days=1))]

        total_records = 0
        # No DB read precedes the fetches, and bulk_upsert commits per area,
        # so the connection is never idle-in-transaction during an HTTP call.
        db_ctx = nullcontext() if self.dry_run else self.database_connection()
        with db_ctx as conn:
            for area_id, area_code, display_label, country_code in self.ACTIVE_AREAS:
                for period_start, period_end in windows:
                    if self.deadline_exceeded():
                        return total_records
                    total_records += self._process_area(
                        period_start, period_end,
                        area_id, area_code, display_label, country_code,
                        conn
                    )
        return total_records

    def run(self) -> bool:
        self.print_header()

        if not self._init_client():
            return False

        try:
            if self.is_backfill:
                total_records = self._run_backfill()
            else:
                total_records = self._run_refresh()

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
