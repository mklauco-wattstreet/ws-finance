"""Parsers for Open-Meteo JSON responses → DB-ready tuples."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Tuple, Optional
import zoneinfo

from weather.constants import WEATHER_VARIABLES, WEATHER_VARIABLES_PREVIOUS_DAY1

PRAGUE_TZ = zoneinfo.ZoneInfo("Europe/Prague")


def _to_decimal(value, precision: int = 2) -> Optional[Decimal]:
    """Convert a numeric value to Decimal, returning None for missing data."""
    if value is None:
        return None
    return round(Decimal(str(value)), precision)


def _time_interval_15min(time_str: str) -> str:
    """Convert '2026-04-11T00:00' to '00:00-00:15' interval string."""
    dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
    end = dt + timedelta(minutes=15)
    return f"{dt.strftime('%H:%M')}-{end.strftime('%H:%M')}"


def _expand_hourly_to_15min(time_str: str) -> List[str]:
    """Expand one hourly timestamp into 4 x 15-min interval strings.

    '2026-04-11T14:00' → ['14:00-14:15', '14:15-14:30', '14:30-14:45', '14:45-15:00']
    """
    dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
    intervals = []
    for offset in (0, 15, 30, 45):
        start = dt + timedelta(minutes=offset)
        end = start + timedelta(minutes=15)
        intervals.append(f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}")
    return intervals


def _trade_date_from_time_str(time_str: str):
    """Extract trade_date (date object) from ISO time string."""
    return datetime.strptime(time_str[:10], "%Y-%m-%d").date()


def parse_forecast_15min(
    data: dict, forecast_made_at: datetime
) -> List[Tuple]:
    """Parse 15-min forecast response into DB tuples.

    Args:
        data: Raw JSON from fetch_forecast_15min().
        forecast_made_at: Timezone-aware datetime when the forecast was retrieved.

    Returns:
        List of (trade_date, time_interval, forecast_made_at,
                 temperature_2m, shortwave_radiation, direct_radiation,
                 cloud_cover, wind_speed_10m) tuples.
    """
    m15 = data["minutely_15"]
    times = m15["time"]
    records = []

    for i, t in enumerate(times):
        records.append((
            _trade_date_from_time_str(t),
            _time_interval_15min(t),
            forecast_made_at,
            _to_decimal(m15["temperature_2m"][i]),
            _to_decimal(m15["shortwave_radiation"][i]),
            _to_decimal(m15["direct_radiation"][i]),
            _to_decimal(m15["cloud_cover"][i]),
            _to_decimal(m15["wind_speed_10m"][i]),
        ))

    return records


def parse_current(data: dict) -> List[Tuple]:
    """Parse current conditions response into a single DB tuple.

    Returns:
        List with one (trade_date, time_interval,
                       temperature_2m, shortwave_radiation, direct_radiation,
                       cloud_cover, wind_speed_10m) tuple.
    """
    cur = data["current"]
    time_str = cur["time"]
    dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
    end = dt + timedelta(minutes=15)
    time_interval = f"{dt.strftime('%H:%M')}-{end.strftime('%H:%M')}"

    return [(
        dt.date(),
        time_interval,
        _to_decimal(cur["temperature_2m"]),
        _to_decimal(cur["shortwave_radiation"]),
        _to_decimal(cur["direct_radiation"]),
        _to_decimal(cur["cloud_cover"]),
        _to_decimal(cur["wind_speed_10m"]),
    )]


def parse_archive_hourly(data: dict) -> List[Tuple]:
    """Parse ERA5 archive response (hourly) into weather_current DB tuples.

    Each hourly value is expanded into 4 identical 15-min rows so that
    resolution matches live current snapshots.

    Returns:
        List of (trade_date, time_interval,
                 temperature_2m, shortwave_radiation, direct_radiation,
                 cloud_cover, wind_speed_10m) tuples.
    """
    hourly = data["hourly"]
    times = hourly["time"]
    records = []

    for i, t in enumerate(times):
        trade_date = _trade_date_from_time_str(t)
        values = (
            _to_decimal(hourly["temperature_2m"][i]),
            _to_decimal(hourly["shortwave_radiation"][i]),
            _to_decimal(hourly["direct_radiation"][i]),
            _to_decimal(hourly["cloud_cover"][i]),
            _to_decimal(hourly["wind_speed_10m"][i]),
        )
        for interval in _expand_hourly_to_15min(t):
            records.append((trade_date, interval, *values))

    return records


def parse_previous_day_forecast(
    data: dict,
) -> List[Tuple]:
    """Parse Previous Runs API response (hourly, _previous_day1 suffix) into DB tuples.

    The forecast_made_at is set to the previous day at 15:00 Prague time
    (the ~12z model run arrives around 14:00 CEST).

    Returns:
        List of (trade_date, time_interval, forecast_made_at,
                 temperature_2m, shortwave_radiation, direct_radiation,
                 cloud_cover, wind_speed_10m) tuples.
    """
    hourly = data["hourly"]
    times = hourly["time"]
    records = []

    for i, t in enumerate(times):
        trade_date = _trade_date_from_time_str(t)
        # forecast_made_at = previous day 15:00 Prague time
        forecast_made_at = datetime(
            trade_date.year, trade_date.month, trade_date.day,
            15, 0, tzinfo=PRAGUE_TZ
        ) - timedelta(days=1)

        values = (
            _to_decimal(hourly[WEATHER_VARIABLES_PREVIOUS_DAY1[0]][i]),
            _to_decimal(hourly[WEATHER_VARIABLES_PREVIOUS_DAY1[1]][i]),
            _to_decimal(hourly[WEATHER_VARIABLES_PREVIOUS_DAY1[2]][i]),
            _to_decimal(hourly[WEATHER_VARIABLES_PREVIOUS_DAY1[3]][i]),
            _to_decimal(hourly[WEATHER_VARIABLES_PREVIOUS_DAY1[4]][i]),
        )
        for interval in _expand_hourly_to_15min(t):
            records.append((trade_date, interval, forecast_made_at, *values))

    return records
