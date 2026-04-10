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
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from weather.client import OpenMeteoClient
from weather.parsers import parse_forecast_15min, parse_previous_day_forecast

# Previous Runs API allows larger ranges than ENTSO-E
BACKFILL_CHUNK_DAYS = 30


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
        """Fetch tomorrow's D+1 forecast at 15-min resolution."""
        now = datetime.now(PRAGUE_TZ)
        tomorrow = (now + timedelta(days=1)).date()
        forecast_made_at = now

        self.logger.debug(f"Fetching D+1 forecast for {tomorrow}")

        data = self.client.fetch_forecast_15min(tomorrow)
        records = parse_forecast_15min(data, forecast_made_at)

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
