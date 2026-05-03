# Weather Data Pipeline

Open-Meteo weather data for central Czechia, used as exogenous input for renewable generation forecasting and imbalance prediction.

## Overview

This module fetches three weather data streams from Open-Meteo public APIs and stores them in PostgreSQL:

- **D+1 forecast** (15-min resolution) — what the model predicted for tomorrow.
- **Current snapshot** — observed values, polled every 15 minutes.
- **Historical truth** (ERA5 archive) and **historical forecasts** (Previous Runs API) — used for backfill and forecast-error feature engineering.

The point sampled is a single coordinate near the centroid of Czechia (49.80 N, 15.47 E). All times are stored in `Europe/Prague` and aligned to OTE's 96-period settlement structure.

## Philosophy

Wind and solar generation drive a large share of intraday imbalance variability, but the forecast that the TSO planned around (D+1) is what the imbalance signal is actually a *deviation from*. Storing both the live observation and the snapshot of the forecast at the time it was made enables computing a forecast-error feature that mirrors how dispatchers and balancing markets see the system.

**Design principles:**
- **Forecast vs. truth split** — `weather_forecast` retains every D+1 prediction (PK includes `forecast_made_at` so we can keep multiple model runs). `weather_current` stores observed/archive truth.
- **Resolution alignment** — Live forecasts arrive at 15-min resolution and are stored as-is. Hourly archive and Previous Runs data are expanded into 4 identical 15-min rows so all downstream joins use the same `(trade_date, time_interval)` grain.
- **Free, no-auth API** — Open-Meteo requires no key, so the pipeline has no secrets and no rate-limit pressure beyond polite use.
- **Idempotent upserts** — All writes use `ON CONFLICT DO UPDATE`. Re-running a backfill is safe.

## Variables

The same five variables are pulled from every endpoint:

| Variable | Unit | Description |
|----------|------|-------------|
| `temperature_2m` | degC | Air temperature at 2 m |
| `shortwave_radiation` | W/m^2 | Total shortwave (direct + diffuse) — solar generation driver |
| `direct_radiation` | W/m^2 | Direct beam component |
| `cloud_cover` | % | Total cloud cover fraction |
| `wind_speed_10m` | km/h | Wind speed at 10 m — wind generation driver |

## Directory Structure

```
app/
├── runners/
│   ├── weather_forecast_runner.py    # D+1 forecast (live + Previous Runs backfill)
│   └── weather_current_runner.py     # Current snapshot (live + ERA5 archive backfill)
│
└── weather/
    ├── client.py                     # OpenMeteoClient (forecast / archive / previous-runs)
    ├── parsers.py                    # JSON -> DB tuples, hourly-to-15min expansion
    └── constants.py                  # Coordinates, endpoints, variable list
```

## Configuration

No environment variables required — Open-Meteo is a public API.

## Usage

### Cron (Automatic)

```
14    15    * * *   weather_forecast_runner    # D+1 forecast, once daily after 12z model run
14,29,44,59 * * * * weather_current_runner     # Current snapshot every 15 min
```

The forecast runs once per day at 15:14 Prague (after the ~14:00 CEST 12z model arrival). The current runner is aligned with the other 14/29/44/59 pipelines so its output is ready before the consumer at :01,:16,:31,:46.

### Manual Execution

```bash
# Live D+1 forecast for tomorrow
docker compose exec entsoe-ote-data-uploader python3 -m runners.weather_forecast_runner --debug

# Live current snapshot
docker compose exec entsoe-ote-data-uploader python3 -m runners.weather_current_runner --debug

# Backfill historical forecasts (Previous Runs API, hourly, expanded to 15-min)
docker compose exec entsoe-ote-data-uploader python3 -m runners.weather_forecast_runner --start 2024-01-01 --end 2026-04-30

# Backfill historical truth from ERA5 archive (~5-day lag, hourly, expanded to 15-min)
docker compose exec entsoe-ote-data-uploader python3 -m runners.weather_current_runner --start 2024-01-01 --end 2026-04-30

# Dry run
docker compose exec entsoe-ote-data-uploader python3 -m runners.weather_current_runner --dry-run --debug
```

### Command-Line Arguments

| Argument | Description |
|----------|-------------|
| `--start YYYY-MM-DD` | Backfill start date |
| `--end YYYY-MM-DD` | Backfill end date (defaults to today) |
| `--debug` | Verbose logging |
| `--dry-run` | Fetch and parse but skip database upload |

Backfill auto-chunks: forecast runner uses 30-day windows, current runner uses 90-day windows.

---

## Database Schema

All tables in the `finance` schema. Timestamps for `trade_date` are local Prague dates; `time_interval` is the standard `"HH:MM-HH:MM"` 15-min string aligned with OTE/CEPS/ENTSO-E. `forecast_made_at` is timezone-aware (Prague).

### weather_forecast

D+1 forecast snapshots. PK includes `forecast_made_at` so multiple model runs for the same `(trade_date, time_interval)` are retained.

| Column | Type | Description |
|--------|------|-------------|
| `trade_date` | DATE | Delivery day (the day the forecast is for) |
| `time_interval` | VARCHAR(11) | 15-min interval (e.g. `"14:00-14:15"`) |
| `forecast_made_at` | TIMESTAMPTZ | When the forecast was retrieved (live mode) or imputed prev-day 15:00 Prague (Previous Runs backfill) |
| `temperature_2m_degc` | NUMERIC(6,2) | Air temperature at 2 m |
| `shortwave_radiation_wm2` | NUMERIC(8,2) | Total shortwave radiation |
| `direct_radiation_wm2` | NUMERIC(8,2) | Direct beam radiation |
| `cloud_cover_pct` | NUMERIC(5,2) | Cloud cover fraction |
| `wind_speed_10m_kmh` | NUMERIC(6,2) | Wind speed at 10 m |
| `created_at` / `updated_at` | TIMESTAMPTZ | Audit columns |

PK: `(trade_date, time_interval, forecast_made_at)`. Index on `trade_date`.

### weather_current

Observed conditions (live snapshots) and ERA5 archive truth. Single row per 15-min interval — only the latest write wins on conflict.

| Column | Type | Description |
|--------|------|-------------|
| `trade_date` | DATE | Delivery day |
| `time_interval` | VARCHAR(11) | 15-min interval |
| `temperature_2m_degc` | NUMERIC(6,2) | Air temperature at 2 m |
| `shortwave_radiation_wm2` | NUMERIC(8,2) | Total shortwave radiation |
| `direct_radiation_wm2` | NUMERIC(8,2) | Direct beam radiation |
| `cloud_cover_pct` | NUMERIC(5,2) | Cloud cover fraction |
| `wind_speed_10m_kmh` | NUMERIC(6,2) | Wind speed at 10 m |
| `created_at` / `updated_at` | TIMESTAMPTZ | Audit columns |

PK: `(trade_date, time_interval)`. Index on `trade_date`.

### Expected Data Rates

- `weather_forecast`: 96 rows/day per model run (1 run/day cron + multiple Previous-Runs backfill rows allowed).
- `weather_current`: 96 rows/day in steady state. Live mode writes one row per 15-min cron tick (the API returns a single observation snapshot, which is upserted into its containing 15-min interval).

---

## Architecture

```
Open-Meteo (public APIs, no auth)
    ├── api.open-meteo.com/v1/forecast            (live D+1 + current)
    ├── archive-api.open-meteo.com/v1/archive     (ERA5 truth, ~5-day lag, hourly)
    └── previous-runs-api.open-meteo.com/v1/forecast  (historical D+1 forecasts, hourly)
       |
       v
+------------------+
|  OpenMeteoClient |  3x retry + exp. backoff on 429/5xx, 30s timeout
+--------+---------+
         |
         v
+------------------+
|  Parsers         |  JSON -> tuples
|                  |  hourly -> 4x 15-min expansion for archive/previous-runs
+--------+---------+
         |
         v
+------------------+
|  Bulk Upsert     |  ON CONFLICT DO UPDATE via base_runner.bulk_upsert
+--------+---------+
         |
         v
+------------------+
|  PostgreSQL      |  finance.weather_forecast / finance.weather_current
+------------------+
```

## Forecast-Error Feature Notes

When joining `weather_forecast` to `weather_current` to derive a forecast error:

- The Previous Runs API exposes a `_previous_day1` variant of each variable, meaning "the value the forecast made yesterday predicted for today." This is what `parse_previous_day_forecast` reads, with `forecast_made_at` imputed as `(trade_date - 1 day) 15:00 Prague`.
- For live forecasts (the daily cron), `forecast_made_at` is the actual wall-clock time of the fetch, so it is roughly `(trade_date - 1 day) 15:14 Prague`.
- ERA5 archive lags ~5 days; for the most recent days of `weather_current`, only live snapshot rows exist.

## Migration History

| Rev | Date | Description |
|-----|------|-------------|
| 053 | 2026-04-10 | Create `weather_current` and `weather_forecast` tables |
| 054 | 2026-04-10 | Rename columns to add unit suffixes (`_degc`, `_wm2`, `_pct`, `_kmh`) |

## Troubleshooting

### Connection errors

1. Verify network access to `https://api.open-meteo.com`, `https://archive-api.open-meteo.com`, `https://previous-runs-api.open-meteo.com`.
2. Run with `--debug` to see request URLs and full responses.

### Empty results

1. Open-Meteo may not have published the requested period yet (especially same-day archive — there is a ~5-day lag).
2. The Previous Runs API only goes back to **January 2024**; earlier `--start` dates will return empty hourly arrays.

### View logs

```bash
docker compose exec entsoe-ote-data-uploader tail -100 /var/log/weather_forecast.log
docker compose exec entsoe-ote-data-uploader tail -100 /var/log/weather_current.log
```

## API Reference

- Open-Meteo docs: https://open-meteo.com/en/docs
- ERA5 archive docs: https://open-meteo.com/en/docs/historical-weather-api
- Previous Runs API docs: https://open-meteo.com/en/docs/previous-runs-api
