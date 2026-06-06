#!/usr/bin/env python3
"""
Weather Forecast Runner.

Fetches D+1 weather forecast from Open-Meteo for central Czechia.
Normal mode: 15-min resolution forecast for tomorrow.
Backfill mode: hourly historical forecasts via Previous Runs API.

Usage:
    python3 -m runners.weather_forecast_runner [--debug] [--dry-run]
    python3 -m runners.weather_forecast_runner --start 2026-01-01 --end 2026-04-05 --debug
"""

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from weather.client import OpenMeteoClient
from weather.parsers import parse_forecast_15min, parse_previous_day_forecast

# Previous Runs API allows larger ranges than ENTSO-E
BACKFILL_CHUNK_DAYS = 30

# Self-heal / outer-retry tunables for _run_daily.
# Self-heal scans for any trade_date in [today - SELF_HEAL_LOOKBACK_DAYS, today]
# that has no row in weather_forecast and refills it via the Previous Runs API.
# Outer retry guards against multi-minute Open-Meteo outages that exceed the
# urllib3 per-request budget (~62s with the new client defaults).
SELF_HEAL_LOOKBACK_DAYS = 7
OUTER_RETRY_ATTEMPTS = 3
OUTER_RETRY_WAIT_S = 120  # doubles per attempt: 120s, 240s


class WeatherForecastRunner(BaseRunner):
    """Runner for Open-Meteo D+1 weather forecasts."""

    RUNNER_NAME = "Weather Forecast Runner"
    TABLE_NAME = "weather_forecast"
    COLUMNS = [
        "trade_date", "time_interval", "forecast_made_at",
        "temperature_2m_degc", "shortwave_radiation_wm2", "direct_radiation_wm2",
        "cloud_cover_pct", "wind_speed_10m_kmh",
    ]
    CONFLICT_COLUMNS = ["trade_date", "time_interval", "forecast_made_at"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = None

    def _backfill_chunks(self):
        """Yield (start, end) date pairs in BACKFILL_CHUNK_DAYS windows."""
        end = self.end_date or datetime.now(PRAGUE_TZ).date()
        start = self.start_date or end - timedelta(days=7)
        current = start
        while current <= end:
            chunk_end = min(current + timedelta(days=BACKFILL_CHUNK_DAYS - 1), end)
            yield current, chunk_end
            current = chunk_end + timedelta(days=1)

    def run(self) -> bool:
        """Execute the weather forecast pipeline."""
        self.client = OpenMeteoClient()

        try:
            if self.is_backfill:
                return self._run_backfill()
            else:
                return self._run_daily()

        except Exception as e:
            self.logger.error(f"{self.RUNNER_NAME} failed: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return False

    def _run_daily(self) -> bool:
        """Fetch tomorrow's D+1 forecast at 15-min resolution.

        Two safety nets beyond the urllib3 retry budget on the HTTP client:

        1. **Self-heal**: before the D+1 fetch, scan the DB for any trade_date
           in the recent past (SELF_HEAL_LOOKBACK_DAYS) that has no rows in
           weather_forecast. Refill those gaps via the Previous Runs API
           (same path as the explicit backfill mode). If a previous day's run
           was lost — e.g. a 502 like 2026-06-04 15:14 — the next day's run
           self-corrects without manual intervention.

        2. **Outer retry**: the D+1 fetch retries up to OUTER_RETRY_ATTEMPTS
           with exponentially-spaced sleeps. Combined with the client's
           urllib3 retries (~62s per attempt), total worst-case patience is
           several minutes — long enough for typical Open-Meteo hiccups.
        """
        self._self_heal_recent_gaps()

        now = datetime.now(PRAGUE_TZ)
        tomorrow = (now + timedelta(days=1)).date()
        forecast_made_at = now

        self.logger.debug(f"Fetching D+1 forecast for {tomorrow}")

        records = self._fetch_d1_with_outer_retry(tomorrow, forecast_made_at)

        if not records:
            self.logger.warning(f"{self.RUNNER_NAME}: no data parsed for {tomorrow}")
            return True

        self.logger.debug(f"Parsed {len(records)} records for {tomorrow}")

        with self.database_connection() as conn:
            count = self.bulk_upsert(
                conn, self.TABLE_NAME, self.COLUMNS,
                records, self.CONFLICT_COLUMNS,
            )
            self.logger.info(f"{self.RUNNER_NAME}: {count} records upserted for {tomorrow}")

        return True

    def _fetch_d1_with_outer_retry(self, tomorrow, forecast_made_at):
        """Fetch D+1 with up to OUTER_RETRY_ATTEMPTS attempts, exponential sleep."""
        wait = OUTER_RETRY_WAIT_S
        for attempt in range(1, OUTER_RETRY_ATTEMPTS + 1):
            try:
                data = self.client.fetch_forecast_15min(tomorrow)
                return parse_forecast_15min(data, forecast_made_at)
            except Exception as e:
                if attempt < OUTER_RETRY_ATTEMPTS:
                    self.logger.warning(
                        f"D+1 fetch attempt {attempt}/{OUTER_RETRY_ATTEMPTS} failed: {e}. "
                        f"Sleeping {wait}s before retry."
                    )
                    time.sleep(wait)
                    wait *= 2
                else:
                    self.logger.error(
                        f"D+1 fetch failed after {OUTER_RETRY_ATTEMPTS} attempts: {e}. "
                        f"The next daily run will self-heal this gap via Previous Runs API."
                    )
                    raise

    def _self_heal_recent_gaps(self):
        """Detect missing trade_dates in the recent past and refill via Previous Runs API.

        Idempotent: rows that already exist hit ON CONFLICT DO UPDATE and are
        rewritten with identical content. Skips today and tomorrow (those are
        not yet eligible for Previous Runs data — they're the live forecast).
        """
        today = datetime.now(PRAGUE_TZ).date()
        lookback_start = today - timedelta(days=SELF_HEAL_LOOKBACK_DAYS)
        # Previous Runs forecasts are only available for dates that are already
        # "previous" — yesterday and older. Don't try to self-heal today or
        # later via this path; that's the D+1 live fetch's job.
        lookback_end = today - timedelta(days=1)

        if lookback_end < lookback_start:
            return

        with self.database_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT d::date
                    FROM generate_series(%s::date, %s::date, '1 day'::interval) d
                    LEFT JOIN finance.weather_forecast f
                      ON f.trade_date = d::date
                    WHERE f.trade_date IS NULL
                    GROUP BY d::date
                    ORDER BY d::date
                    """,
                    (lookback_start, lookback_end),
                )
                missing = [r[0] for r in cur.fetchall()]

        if not missing:
            self.logger.debug(
                f"Self-heal: no gaps in [{lookback_start}, {lookback_end}]"
            )
            return

        gap_start = missing[0]
        gap_end = missing[-1]
        self.logger.warning(
            f"Self-heal: weather_forecast missing {len(missing)} trade_dates in "
            f"[{lookback_start}, {lookback_end}] — refilling via Previous Runs API: {missing}"
        )

        try:
            data = self.client.fetch_previous_day_forecast(gap_start, gap_end)
            records = parse_previous_day_forecast(data)
        except Exception as e:
            self.logger.error(
                f"Self-heal: Previous Runs API call failed for {gap_start}..{gap_end}: {e}. "
                f"Will retry on the next daily run."
            )
            return

        if not records:
            self.logger.warning(
                f"Self-heal: Previous Runs API returned no records for {gap_start}..{gap_end}"
            )
            return

        with self.database_connection() as conn:
            count = self.bulk_upsert(
                conn, self.TABLE_NAME, self.COLUMNS,
                records, self.CONFLICT_COLUMNS,
            )
            self.logger.info(
                f"Self-heal: {count} records upserted for trade_dates in "
                f"{gap_start}..{gap_end}"
            )

    def _run_backfill(self) -> bool:
        """Backfill historical forecasts via Previous Runs API (hourly)."""
        total = 0

        with self.database_connection() as conn:
            for chunk_start, chunk_end in self._backfill_chunks():
                self.logger.debug(f"Backfill chunk: {chunk_start} to {chunk_end}")

                data = self.client.fetch_previous_day_forecast(chunk_start, chunk_end)
                records = parse_previous_day_forecast(data)

                if not records:
                    self.logger.debug(f"No data for {chunk_start} to {chunk_end}")
                    continue

                count = self.bulk_upsert(
                    conn, self.TABLE_NAME, self.COLUMNS,
                    records, self.CONFLICT_COLUMNS,
                )
                total += count
                self.logger.debug(f"Chunk {chunk_start}-{chunk_end}: {count} records")

        self.logger.info(f"{self.RUNNER_NAME}: backfill complete, {total} records upserted")
        return True


if __name__ == '__main__':
    WeatherForecastRunner.main()
