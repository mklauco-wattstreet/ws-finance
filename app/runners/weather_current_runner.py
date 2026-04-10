#!/usr/bin/env python3
"""
Weather Current Conditions Runner.

Fetches current weather snapshot from Open-Meteo for central Czechia
and stores it in weather_current table.

Normal mode: fetches latest snapshot (single row).
Backfill mode: fetches ERA5 archive true values (hourly, ~5-day lag).

Usage:
    python3 -m runners.weather_current_runner [--debug] [--dry-run]
    python3 -m runners.weather_current_runner --start 2026-01-01 --end 2026-04-05 --debug
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from weather.client import OpenMeteoClient
from weather.parsers import parse_current, parse_archive_hourly

BACKFILL_CHUNK_DAYS = 90


class WeatherCurrentRunner(BaseRunner):
    """Runner for Open-Meteo current weather conditions."""

    RUNNER_NAME = "Weather Current Runner"
    TABLE_NAME = "weather_current"
    COLUMNS = [
        "trade_date", "time_interval",
        "temperature_2m_degc", "shortwave_radiation_wm2", "direct_radiation_wm2",
        "cloud_cover_pct", "wind_speed_10m_kmh",
    ]
    CONFLICT_COLUMNS = ["trade_date", "time_interval"]

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
        """Fetch current weather or backfill from ERA5 archive."""
        self.client = OpenMeteoClient()

        try:
            if self.is_backfill:
                return self._run_backfill()
            else:
                return self._run_current()

        except Exception as e:
            self.logger.error(f"{self.RUNNER_NAME} failed: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return False

    def _run_current(self) -> bool:
        """Fetch latest snapshot and upsert single row."""
        data = self.client.fetch_current()
        records = parse_current(data)

        if not records:
            self.logger.warning(f"{self.RUNNER_NAME}: no data parsed")
            return True

        r = records[0]
        self.logger.debug(
            f"Parsed {len(records)} record: "
            f"date={r[0]} interval={r[1]} "
            f"temp={r[2]} sw_rad={r[3]} dir_rad={r[4]} "
            f"cloud={r[5]} wind={r[6]}"
        )

        with self.database_connection() as conn:
            count = self.bulk_upsert(
                conn, self.TABLE_NAME, self.COLUMNS,
                records, self.CONFLICT_COLUMNS,
            )
            self.logger.info(f"{self.RUNNER_NAME}: {count} record upserted")

        return True

    def _run_backfill(self) -> bool:
        """Backfill true values from ERA5 archive (hourly resolution)."""
        total = 0

        with self.database_connection() as conn:
            for chunk_start, chunk_end in self._backfill_chunks():
                self.logger.debug(f"ERA5 backfill chunk: {chunk_start} to {chunk_end}")

                data = self.client.fetch_archive_hourly(chunk_start, chunk_end)
                records = parse_archive_hourly(data)

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
    WeatherCurrentRunner.main()
