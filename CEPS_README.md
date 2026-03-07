# CEPS Data Pipeline

Czech grid operator (CEPS) electricity data via SOAP API.

## Overview

This module fetches real-time and historical data from the CEPS SOAP API, parses XML responses, and uploads to PostgreSQL using idempotent upserts. CEPS is the Czech TSO (Transmission System Operator) and provides data faster than ENTSO-E (~5 min delay vs hours).

## Philosophy

CEPS is the **real-time Czech grid signal**. While ENTSO-E provides pan-European context with multi-hour delay, CEPS delivers 1-minute granularity data within minutes of physical delivery. This makes CEPS data essential for near-real-time imbalance monitoring and short-horizon prediction.

**Design principles:**
- **Dual granularity** — Five datasets arrive at 1-minute resolution and are automatically aggregated to 15-minute intervals (mean, median, last-at-interval). Three datasets arrive natively at 15-minute resolution. The 15-min tables align with OTE's settlement periods for direct feature joins.
- **Year-based partitioning** — Unlike ENTSO-E's country-code partitions, CEPS is CZ-only, so tables partition by year (2024-2028) on `delivery_timestamp` or `trade_date`.
- **Three aggregation statistics** — Mean captures average behavior, median resists outliers, and last-at-interval captures the value closest to settlement time. ML models can choose the most predictive statistic per feature.
- **Idempotent upserts** — All writes use `ON CONFLICT DO UPDATE`. Re-aggregation of 15-min data is triggered automatically when 1-min data is upserted.
- **Derived feature table** — `ceps_1min_features_15min` stores distributional, threshold, and temporal trend features computed from the three 1-min source tables. Automatically recomputed on each upsert of imbalance, RE price, or export/import SVR data. Can also be backfilled standalone from existing data.

## Datasets

| Dataset | SOAP Operation | Granularity | Key Columns |
|---------|---------------|-------------|-------------|
| `imbalance` | AktualniSystemovaOdchylkaCR | 1-min -> 15-min | `load_mw` |
| `re_price` | AktualniCenaRE | 1-min -> 15-min | `price_afrr_plus/minus`, `price_mfrr_plus/minus/5` (EUR/MWh) |
| `svr_activation` | AktivaceSVRvCR | 1-min -> 15-min | `afrr_plus/minus_mw`, `mfrr_plus/minus/5_mw` |
| `export_import_svr` | ExportImportSVR | 1-min -> 15-min | `imbalance_netting_mw`, `mari_mfrr_mw`, `picasso_afrr_mw` |
| `generation_res` | GenerationRES | 1-min -> 15-min | `wind_mw`, `solar_mw` |
| `generation` | Generation | 15-min native | 9 plant types: `tpp`, `ccgt`, `npp`, `hpp`, `pspp`, `altpp`, `appp`, `wpp`, `pvpp` |
| `generation_plan` | GenerationPlan | 15-min native | `total_mw` |
| `estimated_imbalance_price` | OdhadovanaCenaOdchylky | 15-min native | `estimated_price_czk_mwh` |

## Directory Structure

```
app/ceps/
├── ceps_soap_pipeline.py      # Main pipeline (download + parse + upload)
├── ceps_soap_xml_parser.py    # XML parsing for SOAP responses
├── ceps_soap_uploader.py      # Database upsert + automatic 1min->15min aggregation
├── preprocess_ceps_data.py    # 1-min feature aggregation + standalone backfill
├── ceps_consistency_check.py  # Data validation utility
└── constants.py               # SOAP operation names, Czech month mappings

downloads/ceps/
└── soap/YYYY/MM/*.xml         # XML file storage (auto-cleaned daily)
```

## Usage

### Cron (Automatic)

Pipeline runs every 15 minutes (at :12, :27, :42, :57), fetching today's data for all datasets.

### Manual Execution

```bash
# All datasets - today's data
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_soap_pipeline --dataset all

# Single dataset
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_soap_pipeline --dataset imbalance

# Specific date
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_soap_pipeline --dataset all --start 2026-01-15

# Date range backfill (auto-chunks into 7-day blocks)
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_soap_pipeline --dataset all --start 2025-12-01 --end 2025-12-31

# Dry run
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_soap_pipeline --dataset all --dry-run --debug

# Backfill 1-min features (no API calls, computes from existing DB data)
docker compose exec entsoe-ote-data-uploader python /app/scripts/ceps/preprocess_ceps_data.py --start 2024-12-01 --end 2026-03-07
```

### Command-Line Arguments

| Argument | Description |
|----------|-------------|
| `--dataset` | Required: `imbalance`, `re_price`, `svr_activation`, `export_import_svr`, `generation_res`, `generation`, `generation_plan`, `estimated_imbalance_price`, or `all` |
| `--start YYYY-MM-DD` | Start date (defaults to today) |
| `--end YYYY-MM-DD` | End date (defaults to start date) |
| `--debug` | Enable verbose logging |
| `--dry-run` | Fetch and parse but skip database upload |

## Data Consistency Check

```bash
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_consistency_check
```

---

## Database Schema

All tables in `finance` schema. Timestamps are naive (Europe/Prague local time). All partitioned by RANGE on year (2024-2028).

### 1-Minute Raw Tables

Five datasets store raw 1-minute data. Each row has `delivery_timestamp` (UNIQUE) and dataset-specific columns.

**ceps_actual_imbalance_1min**
| Column | Type | Description |
|--------|------|-------------|
| `delivery_timestamp` | TIMESTAMP (UNIQUE) | Minute-level timestamp (Prague) |
| `load_mw` | NUMERIC(12,5) | System imbalance in MW |

**ceps_actual_re_price_1min**
| Column | Type | Description |
|--------|------|-------------|
| `delivery_timestamp` | TIMESTAMP (UNIQUE) | Minute-level timestamp |
| `price_afrr_plus_eur_mwh` | NUMERIC(15,3) | aFRR upward price |
| `price_afrr_minus_eur_mwh` | NUMERIC(15,3) | aFRR downward price |
| `price_mfrr_plus_eur_mwh` | NUMERIC(15,3) | mFRR upward price |
| `price_mfrr_minus_eur_mwh` | NUMERIC(15,3) | mFRR downward price |
| `price_mfrr_5_eur_mwh` | NUMERIC(15,3) | mFRR 5-minute price |

**ceps_svr_activation_1min**
| Column | Type | Description |
|--------|------|-------------|
| `delivery_timestamp` | TIMESTAMP (UNIQUE) | Minute-level timestamp |
| `afrr_plus_mw` | NUMERIC(15,3) | aFRR upward activation volume |
| `afrr_minus_mw` | NUMERIC(15,3) | aFRR downward activation volume |
| `mfrr_plus_mw` | NUMERIC(15,3) | mFRR upward activation volume |
| `mfrr_minus_mw` | NUMERIC(15,3) | mFRR downward activation volume |
| `mfrr_5_mw` | NUMERIC(15,3) | mFRR 5-minute activation volume |

**ceps_export_import_svr_1min**
| Column | Type | Description |
|--------|------|-------------|
| `delivery_timestamp` | TIMESTAMP (UNIQUE) | Minute-level timestamp |
| `imbalance_netting_mw` | NUMERIC(15,5) | Imbalance netting flow |
| `mari_mfrr_mw` | NUMERIC(15,5) | MARI platform mFRR flow |
| `picasso_afrr_mw` | NUMERIC(15,5) | PICASSO platform aFRR flow |
| `sum_exchange_european_platforms_mw` | NUMERIC(15,5) | Total cross-platform exchange |

**ceps_generation_res_1min**
| Column | Type | Description |
|--------|------|-------------|
| `delivery_timestamp` | TIMESTAMP (UNIQUE) | Minute-level timestamp |
| `wind_mw` | NUMERIC(12,3) | Wind generation (VTE) |
| `solar_mw` | NUMERIC(12,3) | Solar generation (FVE) |

### 15-Minute Aggregated Tables

For each 1-min dataset, the uploader automatically aggregates to 15-min intervals with three statistics per column: `_mean`, `_median`, `_last_at_interval`.

Each table has composite unique key `(trade_date, time_interval)` where `time_interval` is `"HH:MM-HH:MM"` format.

**ceps_actual_imbalance_15min** — `load_mean_mw`, `load_median_mw`, `last_load_at_interval_mw`

**ceps_actual_re_price_15min** — 15 columns (5 price types x 3 stats each)

**ceps_svr_activation_15min** — 15 columns (5 activation types x 3 stats each)

**ceps_export_import_svr_15min** — 12 columns (4 flow types x 3 stats each)

**ceps_generation_res_15min** — 6 columns (wind + solar x 3 stats each)

### Derived Feature Table

**ceps_1min_features_15min** — Distributional, threshold, and temporal features computed from the three 1-min source tables (RE prices, imbalance, export/import SVR). Automatically triggered on each upsert cycle; can also be backfilled standalone.

| Column | Type | Description |
|--------|------|-------------|
| `trade_date` | DATE | Trade date |
| `time_interval` | VARCHAR(11) | 15-min interval (e.g. `"14:00-14:15"`) |
| `minute_count` | SMALLINT | Number of 1-min data points (features NULLed if < 12) |
| `afrr_plus_min/max/std_eur` | NUMERIC | aFRR+ price distribution |
| `afrr_plus_skew` | NUMERIC(10,5) | Adjusted Fisher-Pearson skewness (NULL if n < 3 or zero stddev) |
| `afrr_minus_min/max/std_eur` | NUMERIC | aFRR- price distribution |
| `afrr_minus_skew` | NUMERIC(10,5) | Skewness of aFRR- prices |
| `mfrr_plus_min/max/std_eur` | NUMERIC | mFRR+ price distribution |
| `mfrr_plus_skew` | NUMERIC(10,5) | Skewness of mFRR+ prices |
| `mfrr_minus_min/max/std_eur` | NUMERIC | mFRR- price distribution |
| `mfrr_minus_skew` | NUMERIC(10,5) | Skewness of mFRR- prices |
| `imbalance_range_mw` | NUMERIC(12,5) | max(load_mw) - min(load_mw) within interval |
| `imbalance_std_mw` | NUMERIC(12,5) | Sample stddev of load_mw |
| `imbalance_slope` | NUMERIC(12,8) | REGR_SLOPE with normalized x (0..14), MW/min trend |
| `minutes_at_floor` | SMALLINT | Count where aFRR- price <= -500 EUR/MWh |
| `minutes_near_peak` | SMALLINT | Count where aFRR+ price >= 500 EUR/MWh |
| `saturation_count` | SMALLINT | Count where \|picasso_afrr_mw\| >= 95% of daily 95th percentile |

**Key design decisions:**
- Skewness uses raw moment expansion (`SUM(x^3) - 3*AVG(x)*SUM(x^2) + 2*n*AVG(x)^3`) to avoid LATERAL joins
- Saturation threshold is dynamic: 95th percentile of `|picasso_afrr_mw|` per trade_date (not a static MW constant)
- REGR_SLOPE uses minute index 0..14 instead of epoch to avoid floating-point precision issues
- Thresholds (-500/+500 EUR) defined as constants in `preprocess_ceps_data.py`

**ceps_derived_features_15min** — Rolling memory and forecast surprise features computed from existing 15-min tables. Triggered on upsert of imbalance, generation, generation_res, and generation_plan data.

| Column | Type | Description |
|--------|------|-------------|
| `trade_date` | DATE | Trade date |
| `time_interval` | VARCHAR(11) | 15-min interval |
| `imb_roll_2h` | NUMERIC(12,5) | Mean of load_mean_mw over trailing 8 intervals (2 hours) |
| `imb_roll_4h` | NUMERIC(12,5) | Mean of load_mean_mw over trailing 16 intervals (4 hours) |
| `imb_integral_4h` | NUMERIC(15,5) | Cumulative sum of load_mean_mw over trailing 16 intervals |
| `solar_error_mw` | NUMERIC(12,3) | pvpp_mw (actual) - solar_mean_mw (RES forecast) |
| `wind_error_mw` | NUMERIC(12,3) | wpp_mw (actual) - wind_mean_mw (RES forecast) |
| `gen_total_error_mw` | NUMERIC(12,3) | Actual total generation - planned total_mw |

**Key insight:** `GenerationRES` SOAP operation ("Odhad vyroby obnovitelnych zdroju") provides the **RES forecast**, not actuals. Actual solar/wind come from `Generation` (`pvpp_mw`, `wpp_mw`). This enables direct forecast error computation.

### Native 15-Minute Tables

Three datasets arrive at 15-minute resolution directly from the API. No aggregation needed.

**ceps_generation_15min**
| Column | Type | Description |
|--------|------|-------------|
| `tpp_mw` | NUMERIC(12,3) | Thermal Power Plants |
| `ccgt_mw` | NUMERIC(12,3) | Combined Cycle Gas Turbine |
| `npp_mw` | NUMERIC(12,3) | Nuclear Power Plants |
| `hpp_mw` | NUMERIC(12,3) | Hydro Power Plants |
| `pspp_mw` | NUMERIC(12,3) | Pumped-Storage |
| `altpp_mw` | NUMERIC(12,3) | Alternative Power Plants |
| `appp_mw` | NUMERIC(12,3) | Autoproducer (canceled Oct 2014) |
| `wpp_mw` | NUMERIC(12,3) | Wind Power Plants |
| `pvpp_mw` | NUMERIC(12,3) | Photovoltaic Power Plants |

**ceps_generation_plan_15min**
| Column | Type | Description |
|--------|------|-------------|
| `total_mw` | NUMERIC(12,3) | Planned total generation |

**ceps_estimated_imbalance_price_15min**
| Column | Type | Description |
|--------|------|-------------|
| `estimated_price_czk_mwh` | NUMERIC(12,3) | Estimated deviation price in CZK/MWh |

### Expected Data Rates

- 1-minute tables: 1,440 records/day (24h x 60min)
- 15-minute tables: 96 records/day (24h x 4 intervals)

---

## Architecture

```
CEPS SOAP API (https://www.ceps.cz/_layouts/CepsData.asmx)
       |
       v
+------------------+
|  Build SOAP      |  Operation-specific XML envelope
|  Envelope        |  7-day max chunk size
+--------+---------+
         |
         v
+------------------+
|  HTTP POST       |  60s timeout, ~7s response time
|  to CEPS         |  No authentication required (public API)
+--------+---------+
         |
         v
+------------------+
|  Save to disk    |  downloads/ceps/soap/YYYY/MM/
+--------+---------+  (auto-cleaned after 1 day by cron at 02:05)
         |
         v
+------------------+
|  XML Parser      |  Dataset-specific parsing (8 parsers)
|  + Deduplicate   |  Remove duplicate timestamps in-memory
+--------+---------+
         |
         v
+------------------+
|  Bulk Upsert     |  1-min: INSERT ... ON CONFLICT DO UPDATE
|  + Aggregate     |  15-min: automatic re-aggregation (mean/median/last)
+--------+---------+
         |
         v
+------------------+
|  Feature Agg     |  preprocess_ceps_data.py
|  (1min -> 15min) |  distributional/threshold/trend features
+--------+---------+  triggered after imbalance, re_price, export_import_svr
         |
         v
+------------------+
|  PostgreSQL      |  Year-partitioned tables (2024-2028)
+------------------+
```

## File Storage

XML files stored in `downloads/ceps/soap/YYYY/MM/` and auto-cleaned after 1 day (cron at 02:05).

- Container path: `/app/downloads/ceps/soap`
- Host path: `./downloads/ceps/soap`

## Migration History

| Rev | Date | Description |
|-----|------|-------------|
| 027 | 2026-01-07 | Create imbalance tables (1min + 15min) |
| 028 | 2026-01-07 | Add last_load_at_interval_mw |
| 029 | 2026-01-07 | Convert TIMESTAMPTZ to TIMESTAMP (naive) |
| 030 | 2026-01-08 | Create RE price tables |
| 031 | 2026-01-08 | Create SVR activation tables |
| 032 | 2026-01-09 | Create export/import SVR tables |
| 033 | 2026-01-16 | Create generation RES tables |
| 034 | 2026-01-16 | Create generation + generation plan tables |
| 035 | 2026-01-17 | Create estimated imbalance price table |
| 042 | 2026-03-07 | Create ceps_1min_features_15min table (distributional/volatility features) |
| 043 | 2026-03-07 | Create ceps_derived_features_15min table (rolling memory + forecast surprise) |

## Troubleshooting

### Connection Errors

1. Verify network access to `https://www.ceps.cz`
2. Run with `--debug` to see full responses

### Empty Results

1. CEPS may not have data for the requested period
2. Check CEPS website for service status

### View Logs

```bash
docker compose exec entsoe-ote-data-uploader tail -100 /var/log/ceps.log
```
