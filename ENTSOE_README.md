# ENTSO-E Data Pipeline

Pan-European electricity market data from the ENTSO-E Transparency Platform.

## Overview

This module fetches real-time and historical data from the ENTSO-E API, parses XML responses, and uploads to PostgreSQL using idempotent upserts. All tables are partitioned by `country_code` for efficient multi-area querying.

## Philosophy

ENTSO-E provides the **pan-European view** of the electricity grid. While CEPS gives faster Czech-specific data (~5 min delay vs hours), ENTSO-E is the only source for cross-border context: what neighboring countries generate, consume, and trade. This context is critical for imbalance prediction because Czech grid stress often originates from German wind/solar variability or cross-border flow imbalances.

**Design principles:**
- **Wide-format tables** — Generation and flow data are pivoted into one row per period rather than normalized by fuel/border. This avoids expensive JOINs for ML feature engineering.
- **Partition by country_code** — Enables partition pruning on the most common filter. German TSOs (4 areas) share the `DE` partition.
- **Idempotent upserts** — All writes use `ON CONFLICT DO UPDATE`, making re-runs and backfills safe.
- **UTC-to-Prague conversion** — API returns UTC. Parsers convert to Europe/Prague, compute `trade_date` (local date) and `period` (1-96), aligning with OTE's settlement structure.

## Data Types

| Runner | Doc Type | Table | Active Areas | Schedule |
|--------|----------|-------|-------------|----------|
| `entsoe_unified_gen_runner` | A75 | `entsoe_generation_actual` | CZ, DE(4 TSOs), AT, PL, SK | `*/15 * * * *` |
| `entsoe_unified_load_runner` | A65 | `entsoe_load` | CZ, DE, AT, PL, SK | `*/15 * * * *` |
| `entsoe_unified_forecast_runner` | A69 | `entsoe_generation_forecast` | CZ, DE, AT, PL, SK | `*/15 * * * *` |
| `entsoe_unified_scheduled_runner` | A71 | `entsoe_generation_scheduled` | CZ, DE, AT, PL, SK | `*/15 * * * *` |
| `entsoe_unified_flow_runner` | A11 | `entsoe_cross_border_flows` | CZ (4 borders) | `*/15 * * * *` |
| `entsoe_unified_sched_flow_runner` | A09 | `entsoe_scheduled_cross_border_flows` | CZ (4 borders) | `*/15 * * * *` |
| `entsoe_unified_balancing_runner` | A84 | `entsoe_balancing_energy` | CZ, DE, AT, PL, SK | `*/15 * * * *` |
| `entsoe_unified_imbalance_runner` | A85/A86 | `entsoe_imbalance_prices` | CZ, DE, AT, PL, SK, HU | `*/15 * * * *` |
| `entsoe_unified_day_ahead_prices_runner` | A44 | `entsoe_day_ahead_prices` | HU, DE-LU, AT | `0 14 * * *` |

## Directory Structure

```
app/
├── runners/                           # Pipeline runners
│   ├── base_runner.py                 # Shared infrastructure (DB, upsert, backfill)
│   ├── entsoe_unified_*.py            # 9 data pipelines
│   ├── entsoe_consistency_check.py    # Data validation utility
│   └── ENTSOE_SCHEMA.md              # Detailed schema reference
│
└── entsoe/                            # ENTSO-E module
    ├── client.py                      # API client with retry logic
    ├── parsers.py                     # XML parsing for all doc types
    ├── constants.py                   # Area codes, active areas config
    └── xml_definitions/               # XSD schemas, XML-to-DB mapping docs

downloads/entsoe/                      # XML file storage (auto-cleaned daily)
└── YYYY/MM/*.xml
```

## Configuration

Add to `.env`:

```bash
ENTSOE_SECURITY_TOKEN=your_token_here
ENTSOE_CONTROL_AREA_DOMAIN=10YCZ-CEPS-----N
ENTSOE_BASE_URL=https://web-api.tp.entsoe.eu/api
```

## Usage

### Cron (Automatic)

Most runners execute every 15 minutes via crontab, fetching the last 3 hours of data. Day-ahead prices run once daily at 14:00 (after ~13:00 publication).

### Manual Execution

```bash
# Normal mode - fetch last 3 hours
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner

# Backfill mode - historical data (auto-chunks into 7-day blocks)
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner --start 2024-12-01 --end 2024-12-31

# Dry run - fetch and parse without uploading
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner --dry-run --debug
```

### Command-Line Arguments

| Argument | Description |
|----------|-------------|
| `--debug` | Enable verbose logging |
| `--dry-run` | Fetch and parse but skip database upload |
| `--start YYYY-MM-DD` | Start date for backfill |
| `--end YYYY-MM-DD` | End date for backfill (defaults to today) |

## Backfill

Backfill automatically chunks requests into 7-day blocks (ENTSO-E API limit):

```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_load_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_gen_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_flow_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_forecast_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_balancing_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_scheduled_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_sched_flow_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_day_ahead_prices_runner --start 2024-12-01
```

## Data Consistency Check

Verify data completeness across all datasets and countries:

```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_consistency_check
```

---

## Database Schema

All tables in `finance` schema. All partitioned by `LIST (country_code)` with partitions: `_cz`, `_de`, `_at`, `_pl`, `_sk` (plus `_hu` where applicable).

**Common structure:** PK `(trade_date, period, area_id, country_code)`, 15-min granularity (96 periods/day).

### entsoe_areas (Static Lookup)

Central reference for EIC codes and area metadata.

| id | code | country_name | country_code |
|----|------|--------------|--------------|
| 1 | 10YCZ-CEPS-----N | Czech Republic | CZ |
| 2 | 10YDE-EON------1 | Germany (TenneT) | DE |
| 3 | 10YAT-APG------L | Austria | AT |
| 4 | 10YPL-AREA-----S | Poland | PL |
| 5 | 10YSK-SEPS-----K | Slovakia | SK |
| 6 | 10YDE-VE-------2 | Germany (50Hertz) | DE |
| 7 | 10YDE-RWENET---I | Germany (Amprion) | DE |
| 8 | 10YDE-ENBW-----N | Germany (TransnetBW) | DE |
| 9 | 10YHU-MAVIR----U | Hungary | HU |
| 10 | 10Y1001A1001A82H | Germany-Luxembourg (BZ) | DE |

### entsoe_generation_actual (A75)

Actual generation per production type in wide format. 9 fuel columns per row.

| Column | Type | Description |
|--------|------|-------------|
| `gen_nuclear_mw` | NUMERIC(12,3) | B14: Nuclear |
| `gen_coal_mw` | NUMERIC(12,3) | B02+B05: Brown coal + Hard coal |
| `gen_gas_mw` | NUMERIC(12,3) | B04: Fossil Gas |
| `gen_solar_mw` | NUMERIC(12,3) | B16: Solar |
| `gen_wind_mw` | NUMERIC(12,3) | B19: Wind Onshore |
| `gen_wind_offshore_mw` | NUMERIC(12,3) | B18: Wind Offshore |
| `gen_hydro_pumped_mw` | NUMERIC(12,3) | B10: Hydro Pumped Storage |
| `gen_biomass_mw` | NUMERIC(12,3) | B01: Biomass |
| `gen_hydro_other_mw` | NUMERIC(12,3) | B11+B12: Run-of-river + Reservoir |

All fuel columns nullable (NULL when unavailable for a country/area).

### entsoe_load (A65)

Actual total load and day-ahead forecast.

| Column | Type | Description |
|--------|------|-------------|
| `actual_load_mw` | NUMERIC(12,3) | Actual total load |
| `forecast_load_mw` | NUMERIC(12,3) | Day-ahead forecast load |

### entsoe_generation_forecast (A69)

Day-ahead generation forecasts for renewable sources.

| Column | Type | Description |
|--------|------|-------------|
| `forecast_solar_mw` | NUMERIC(12,3) | Solar forecast |
| `forecast_wind_mw` | NUMERIC(12,3) | Wind Onshore forecast |
| `forecast_wind_offshore_mw` | NUMERIC(12,3) | Wind Offshore forecast |

### entsoe_generation_scheduled (A71)

Day-ahead scheduled total generation.

| Column | Type | Description |
|--------|------|-------------|
| `scheduled_total_mw` | NUMERIC(12,3) | Total scheduled generation |

### entsoe_cross_border_flows (A11)

Physical cross-border flows for CZ borders. Wide format with one column per border.

| Column | Type | Description |
|--------|------|-------------|
| `delivery_datetime` | TIMESTAMP | Full delivery timestamp |
| `flow_de_mw` | NUMERIC(12,3) | Flow to/from Germany (+import/-export) |
| `flow_at_mw` | NUMERIC(12,3) | Flow to/from Austria |
| `flow_pl_mw` | NUMERIC(12,3) | Flow to/from Poland |
| `flow_sk_mw` | NUMERIC(12,3) | Flow to/from Slovakia |
| `flow_total_net_mw` | NUMERIC(12,3) | Sum of all border flows |

### entsoe_scheduled_cross_border_flows (A09)

Day-ahead scheduled commercial exchanges for CZ borders.

| Column | Type | Description |
|--------|------|-------------|
| `scheduled_de_mw` | NUMERIC(12,3) | Scheduled exchange with Germany |
| `scheduled_at_mw` | NUMERIC(12,3) | Scheduled exchange with Austria |
| `scheduled_pl_mw` | NUMERIC(12,3) | Scheduled exchange with Poland |
| `scheduled_sk_mw` | NUMERIC(12,3) | Scheduled exchange with Slovakia |
| `scheduled_total_net_mw` | NUMERIC(12,3) | Sum of all scheduled exchanges |

### entsoe_balancing_energy (A84)

Activated balancing energy prices for aFRR and mFRR reserves.

| Column | Type | Description |
|--------|------|-------------|
| `afrr_up_price_eur` | NUMERIC(12,3) | aFRR upward activation price (EUR/MWh) |
| `afrr_down_price_eur` | NUMERIC(12,3) | aFRR downward activation price |
| `mfrr_up_price_eur` | NUMERIC(12,3) | mFRR upward activation price |
| `mfrr_down_price_eur` | NUMERIC(12,3) | mFRR downward activation price |

### entsoe_imbalance_prices (A85/A86)

Imbalance prices with financial components and volumes. Currency is CZK for CZ, EUR for all others (column names say `czk_mwh` but store the local currency).

| Column | Type | Description |
|--------|------|-------------|
| `pos_imb_price_czk_mwh` | NUMERIC(15,3) | Positive imbalance price |
| `pos_imb_scarcity_czk_mwh` | NUMERIC(15,3) | Positive imbalance scarcity component |
| `pos_imb_incentive_czk_mwh` | NUMERIC(15,3) | Positive imbalance incentive component |
| `pos_imb_financial_neutrality_czk_mwh` | NUMERIC(15,3) | Positive financial neutrality |
| `neg_imb_price_czk_mwh` | NUMERIC(15,3) | Negative imbalance price |
| `neg_imb_scarcity_czk_mwh` | NUMERIC(15,3) | Negative imbalance scarcity component |
| `neg_imb_incentive_czk_mwh` | NUMERIC(15,3) | Negative imbalance incentive component |
| `neg_imb_financial_neutrality_czk_mwh` | NUMERIC(15,3) | Negative financial neutrality |
| `imbalance_mwh` | NUMERIC(12,5) | Total imbalance volume |
| `difference_mwh` | NUMERIC(12,5) | Difference volume |
| `situation` | VARCHAR | Market situation |
| `status` | VARCHAR | Data status |
| `currency` | VARCHAR | CZK or EUR |
| `delivery_datetime` | TIMESTAMP | Full delivery timestamp |

### entsoe_day_ahead_prices (A44)

Day-ahead market clearing prices. HU, DE-LU (Germany-Luxembourg bidding zone), and AT. CZ prices come from OTE.

| Column | Type | Description |
|--------|------|-------------|
| `price_eur_mwh` | NUMERIC(12,3) | Day-ahead clearing price in EUR/MWh |

---

## Period to Time Mapping

| Period | Time (CET) | Period | Time (CET) | Period | Time (CET) | Period | Time (CET) |
|--------|------------|--------|------------|--------|------------|--------|------------|
| 1 | 00:00 | 25 | 06:00 | 49 | 12:00 | 73 | 18:00 |
| 4 | 00:45 | 28 | 06:45 | 52 | 12:45 | 76 | 18:45 |
| 8 | 01:45 | 32 | 07:45 | 56 | 13:45 | 80 | 19:45 |
| 12 | 02:45 | 36 | 08:45 | 60 | 14:45 | 84 | 20:45 |
| 16 | 03:45 | 40 | 09:45 | 64 | 15:45 | 88 | 21:45 |
| 20 | 04:45 | 44 | 10:45 | 68 | 16:45 | 92 | 22:45 |
| 24 | 05:45 | 48 | 11:45 | 72 | 17:45 | 96 | 23:45 |

**Formula:** `Period = (Hour * 4) + (Minute / 15) + 1`

## Query Examples

### Total German Generation (All 4 TSOs)

```sql
SELECT trade_date, period, time_interval,
       SUM(gen_wind_mw) as total_wind_onshore,
       SUM(gen_wind_offshore_mw) as total_wind_offshore,
       SUM(gen_solar_mw) as total_solar
FROM finance.entsoe_generation_actual
WHERE country_code = 'DE'
  AND trade_date = '2025-12-15'
GROUP BY trade_date, period, time_interval
ORDER BY period;
```

### Forecast vs Actual Error

```sql
SELECT g.trade_date, g.period,
       f.forecast_solar_mw - g.gen_solar_mw as solar_error_mw,
       f.forecast_wind_mw - g.gen_wind_mw as wind_error_mw
FROM finance.entsoe_generation_actual g
JOIN finance.entsoe_generation_forecast f
  ON g.trade_date = f.trade_date
  AND g.period = f.period
  AND g.area_id = f.area_id
  AND g.country_code = f.country_code
WHERE g.country_code = 'CZ'
ORDER BY g.trade_date, g.period;
```

### Cross-Country Load Comparison

```sql
SELECT trade_date, period, country_code,
       actual_load_mw,
       forecast_load_mw,
       actual_load_mw - forecast_load_mw as load_forecast_error
FROM finance.entsoe_load
WHERE trade_date = '2025-12-15'
ORDER BY country_code, period;
```

### Imbalance Prices with Currency

```sql
SELECT trade_date, period, country_code,
       CASE
           WHEN country_code = 'CZ' THEN ROUND(pos_imb_price_czk_mwh / 24.5, 2)
           ELSE ROUND(pos_imb_price_czk_mwh, 2)
       END AS imb_price_eur_mwh
FROM finance.entsoe_imbalance_prices
WHERE trade_date = '2025-12-15'
ORDER BY country_code, period;
```

## Migration History

| Rev | Date | Description |
|-----|------|-------------|
| 012 | 2024-12-22 | Create `entsoe_areas` lookup table |
| 013 | 2024-12-22 | Create partitioned `entsoe_generation_actual` |
| 014 | 2024-12-22 | Migrate data from legacy tables |
| 015 | 2024-12-22 | Drop legacy generation tables |
| 016 | 2025-12-22 | Add German TSO areas (50Hertz, Amprion, TransnetBW) |
| 017 | 2025-12-22 | Consolidate German partitions into single `_de` partition |
| 018 | 2025-12-23 | Restructure partitioning by `country_code` instead of `area_id` |
| 019 | 2025-12-23 | Partition `entsoe_load` by country_code |
| 020 | 2025-12-23 | Partition `entsoe_generation_forecast` by country_code |
| 021 | 2025-12-23 | Partition `entsoe_generation_scheduled` by country_code |
| 022 | 2025-12-23 | Partition `entsoe_balancing_energy` by country_code |
| 023 | 2025-12-23 | Partition `entsoe_imbalance_prices` by country_code |
| 024 | 2025-12-23 | Partition `entsoe_cross_border_flows` by country_code |
| 025 | 2025-12-23 | Partition `entsoe_scheduled_cross_border_flows` by country_code |
| 036 | 2026-01-23 | Add `entsoe_day_ahead_prices` partitioned table (HU) |
| 040 | 2026-03-07 | Add DE-LU bidding zone (area_id=10) and DE/AT partitions for day-ahead prices |

---

## Architecture

```
ENTSO-E API (REST)
       |
       v
+------------------+
|  EntsoeClient    |  Fetch XML with 3x retry + exponential backoff
|  (client.py)     |  Auto-unzip ZIP responses, validate date ranges
+--------+---------+
         |
         v
+------------------+
|  Save to disk    |  downloads/entsoe/YYYY/MM/*.xml
+--------+---------+  (auto-cleaned after 1 day by cron at 02:00)
         |
         v
+------------------+
|  XML Parsers     |  UTC -> Prague timezone, period calc (1-96)
|  (parsers.py)    |  PSR type aggregation, resolution priority (15m > 60m)
+--------+---------+
         |
         v
+------------------+
|  Bulk Upsert     |  psycopg2.extras.execute_values
|  (base_runner)   |  ON CONFLICT DO UPDATE (idempotent)
+--------+---------+
         |
         v
+------------------+
|  PostgreSQL      |  Partitioned by country_code (LIST)
+------------------+
```

## File Storage

XML files are stored in `downloads/entsoe/YYYY/MM/` and automatically cleaned after 1 day (cron at 02:00).

- Container path: `/app/downloads/entsoe`
- Host path: `./downloads/entsoe`

## Troubleshooting

### Connection Errors

1. Verify `ENTSOE_SECURITY_TOKEN` is valid
2. Check network access to `https://web-api.tp.entsoe.eu`
3. Run with `--debug` to see full API responses

### Empty Results

1. Data may not be available for the requested period
2. Verify control area domain is correct
3. Check ENTSO-E platform status

### View Logs

```bash
docker compose exec entsoe-ote-data-uploader tail -100 /var/log/entsoe_imbalance.log
docker compose exec entsoe-ote-data-uploader tail -100 /var/log/entsoe_*.log
```

## API Reference

- ENTSO-E Transparency Platform: https://transparency.entsoe.eu
- API Documentation: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
