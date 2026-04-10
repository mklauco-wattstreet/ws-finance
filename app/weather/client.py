"""Open-Meteo HTTP client with retry/backoff."""

import logging
from datetime import date
from typing import Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from weather.constants import (
    LATITUDE, LONGITUDE,
    FORECAST_URL, ARCHIVE_URL, PREVIOUS_RUNS_URL,
    WEATHER_VARIABLES, WEATHER_VARIABLES_PREVIOUS_DAY1,
)

logger = logging.getLogger(__name__)


class OpenMeteoClient:
    """Client for Open-Meteo forecast and previous-runs APIs."""

    def __init__(self, max_retries: int = 3, backoff_factor: float = 1.0):
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _get(self, url: str, params: dict) -> Dict[str, Any]:
        """Execute GET request and return parsed JSON."""
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if "error" in data and data["error"]:
            raise ValueError(f"Open-Meteo API error: {data.get('reason', data)}")
        return data

    def fetch_forecast_15min(self, target_date: date) -> Dict[str, Any]:
        """Fetch D+1 forecast at 15-minute resolution for a single day.

        Args:
            target_date: The date to get the forecast for (typically tomorrow).

        Returns:
            Raw JSON response with 'minutely_15' key containing time series.
        """
        date_str = target_date.strftime("%Y-%m-%d")
        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "minutely_15": ",".join(WEATHER_VARIABLES),
            "start_date": date_str,
            "end_date": date_str,
            "timezone": "Europe/Prague",
        }
        return self._get(FORECAST_URL, params)

    def fetch_current(self) -> Dict[str, Any]:
        """Fetch current conditions snapshot.

        Returns:
            Raw JSON response with 'current' key containing a single observation.
        """
        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "current": ",".join(WEATHER_VARIABLES),
            "timezone": "Europe/Prague",
        }
        return self._get(FORECAST_URL, params)

    def fetch_archive_hourly(
        self, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """Fetch historical true values from ERA5 archive (hourly resolution).

        Archive lags ~5 days behind today.

        Args:
            start_date: Start of backfill range.
            end_date: End of backfill range.

        Returns:
            Raw JSON response with 'hourly' key containing time series.
        """
        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "hourly": ",".join(WEATHER_VARIABLES),
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "timezone": "Europe/Prague",
        }
        return self._get(ARCHIVE_URL, params)

    def fetch_previous_day_forecast(
        self, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """Fetch historical D+1 forecasts (what the model predicted ~24h ahead).

        Uses the Previous Runs API with _previous_day1 suffix.
        Available from January 2024 onwards, hourly resolution.

        Args:
            start_date: Start of backfill range.
            end_date: End of backfill range.

        Returns:
            Raw JSON response with 'hourly' key containing time series.
        """
        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "hourly": ",".join(WEATHER_VARIABLES_PREVIOUS_DAY1),
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "timezone": "Europe/Prague",
        }
        return self._get(PREVIOUS_RUNS_URL, params)
