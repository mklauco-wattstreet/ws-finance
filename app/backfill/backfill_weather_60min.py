"""Backfill weather_current_60min and weather_forecast_60min from 15-min sources.

Per docs/60min_tables_plan.md §4.3:
- All numeric variables: mean
- weather_current_60min: GROUP BY (trade_date, hour)
- weather_forecast_60min: GROUP BY (trade_date, hour, forecast_made_at)

Usage:
    python3 -m backfill.backfill_weather_60min YYYY-MM-DD YYYY-MM-DD [--debug] [--dry-run]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backfill._common import (
    HOUR_COMPLETE_HAVING,
    HOUR_GROUP_SQL,
    HOUR_INTERVAL_SQL,
    parse_args,
    run_backfill,
    setup_logging,
)


CURRENT_SQL = f"""
INSERT INTO weather_current_60min (
    trade_date, time_interval,
    temperature_2m_degc, shortwave_radiation_wm2, direct_radiation_wm2,
    cloud_cover_pct, wind_speed_10m_kmh
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    AVG(temperature_2m_degc),
    AVG(shortwave_radiation_wm2),
    AVG(direct_radiation_wm2),
    AVG(cloud_cover_pct),
    AVG(wind_speed_10m_kmh)
FROM weather_current
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
{HOUR_COMPLETE_HAVING}
ON CONFLICT (trade_date, time_interval) DO UPDATE SET
    temperature_2m_degc = EXCLUDED.temperature_2m_degc,
    shortwave_radiation_wm2 = EXCLUDED.shortwave_radiation_wm2,
    direct_radiation_wm2 = EXCLUDED.direct_radiation_wm2,
    cloud_cover_pct = EXCLUDED.cloud_cover_pct,
    wind_speed_10m_kmh = EXCLUDED.wind_speed_10m_kmh,
    updated_at = CURRENT_TIMESTAMP
"""


FORECAST_SQL = f"""
INSERT INTO weather_forecast_60min (
    trade_date, time_interval, forecast_made_at,
    temperature_2m_degc, shortwave_radiation_wm2, direct_radiation_wm2,
    cloud_cover_pct, wind_speed_10m_kmh
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    forecast_made_at,
    AVG(temperature_2m_degc),
    AVG(shortwave_radiation_wm2),
    AVG(direct_radiation_wm2),
    AVG(cloud_cover_pct),
    AVG(wind_speed_10m_kmh)
FROM weather_forecast
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}, forecast_made_at
{HOUR_COMPLETE_HAVING}
ON CONFLICT (trade_date, time_interval, forecast_made_at) DO UPDATE SET
    temperature_2m_degc = EXCLUDED.temperature_2m_degc,
    shortwave_radiation_wm2 = EXCLUDED.shortwave_radiation_wm2,
    direct_radiation_wm2 = EXCLUDED.direct_radiation_wm2,
    cloud_cover_pct = EXCLUDED.cloud_cover_pct,
    wind_speed_10m_kmh = EXCLUDED.wind_speed_10m_kmh,
    updated_at = CURRENT_TIMESTAMP
"""


def main():
    args = parse_args("Weather")
    logger = setup_logging("backfill_weather_60min", args.debug)
    run_backfill(
        label="Weather",
        queries=[
            ("weather_current_60min", CURRENT_SQL),
            ("weather_forecast_60min", FORECAST_SQL),
        ],
        args=args,
        logger=logger,
    )


if __name__ == "__main__":
    main()
