# ENTSO-E Database Schema

Documentation of all `entsoe_*` tables in the `finance` schema.

**Last Updated:** 2025-12-23 (Migration 018: Partition by country_code)

---

## Table Overview

| Table | Description | Doc Type | Resolution | Partitioned |
|-------|-------------|----------|------------|-------------|
| `entsoe_areas` | Area/TSO lookup table | - | - | No |
| `entsoe_generation_actual` | Actual generation by fuel type | A75 | 15-min | Yes (by country) |
| `entsoe_generation_forecast` | Day-ahead wind/solar forecasts | A69 | 15-min | No |
| `entsoe_generation_scheduled` | Day-ahead scheduled generation | A71 | 15-min | No |
| `entsoe_load` | Actual load & DA forecast | A65 | 15-min | No |
| `entsoe_cross_border_flows` | Physical cross-border flows | A11 | 15-min | No |
| `entsoe_scheduled_cross_border_flows` | Scheduled cross-border exchanges | A09 | 15-min | No |
| `entsoe_balancing_energy` | Activated aFRR/mFRR prices | A84 | 15-min | No |
| `entsoe_imbalance_prices` | Imbalance prices & volumes | A85/A86 | 15-min | No |

---

## entsoe_areas

**Purpose:** Central lookup table for delivery area metadata (EIC codes).

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Area ID (used for partitioning) |
| `code` | VARCHAR(20) UNIQUE | EIC code (e.g., '10YCZ-CEPS-----N') |
| `country_name` | VARCHAR(100) | Full name (e.g., 'Czech Republic') |
| `country_code` | VARCHAR(5) | ISO code (e.g., 'CZ') |
| `is_active` | BOOLEAN | Whether area is actively fetched |

**Pre-populated Data:**

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

---

## entsoe_generation_actual

**Purpose:** Actual generation per production type (wide format).

**Partitioning:** LIST by `country_code`, partitioned by country:
- `entsoe_generation_actual_cz` → country_code IN ('CZ')
- `entsoe_generation_actual_de` → country_code IN ('DE')
- `entsoe_generation_actual_at` → country_code IN ('AT')
- `entsoe_generation_actual_pl` → country_code IN ('PL')
- `entsoe_generation_actual_sk` → country_code IN ('SK')

This structure allows new TSOs/bidding zones to be added without modifying partition DDL.
New TSOs automatically route to their country partition based on `country_code`.

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Auto-increment ID |
| `trade_date` | DATE | Delivery date (Europe/Prague) |
| `period` | INTEGER | 15-min period (1-96) |
| `area_id` | INTEGER | FK to entsoe_areas.id |
| `country_code` | VARCHAR(5) | Country code for partition routing (e.g., 'DE') |
| `time_interval` | VARCHAR(11) | Time slot (e.g., '08:00-08:15') |
| `gen_nuclear_mw` | NUMERIC(12,3) | B14: Nuclear |
| `gen_coal_mw` | NUMERIC(12,3) | B02+B05: Brown coal + Hard coal |
| `gen_gas_mw` | NUMERIC(12,3) | B04: Fossil Gas |
| `gen_solar_mw` | NUMERIC(12,3) | B16: Solar |
| `gen_wind_mw` | NUMERIC(12,3) | B19: Wind Onshore |
| `gen_wind_offshore_mw` | NUMERIC(12,3) | B18: Wind Offshore |
| `gen_hydro_pumped_mw` | NUMERIC(12,3) | B10: Hydro Pumped Storage |
| `gen_biomass_mw` | NUMERIC(12,3) | B01: Biomass |
| `gen_hydro_other_mw` | NUMERIC(12,3) | B11+B12: Run-of-river + Reservoir |
| `created_at` | TIMESTAMP | Record creation time |

**Primary Key:** `(trade_date, period, area_id, country_code)`

**PSR Type Mapping:**

| PSR Code | Description | DB Column |
|----------|-------------|-----------|
| B01 | Biomass | gen_biomass_mw |
| B02 | Fossil Brown coal/Lignite | gen_coal_mw |
| B04 | Fossil Gas | gen_gas_mw |
| B05 | Fossil Hard coal | gen_coal_mw |
| B10 | Hydro Pumped Storage | gen_hydro_pumped_mw |
| B11 | Hydro Run-of-river | gen_hydro_other_mw |
| B12 | Hydro Water Reservoir | gen_hydro_other_mw |
| B14 | Nuclear | gen_nuclear_mw |
| B16 | Solar | gen_solar_mw |
| B18 | Wind Offshore | gen_wind_offshore_mw |
| B19 | Wind Onshore | gen_wind_mw |

---

## entsoe_generation_forecast

**Purpose:** Day-ahead generation forecasts for renewable sources (A69).

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment ID |
| `trade_date` | DATE | Delivery date |
| `period` | INTEGER | 15-min period (1-96) |
| `time_interval` | VARCHAR(11) | Time slot |
| `forecast_solar_mw` | NUMERIC(12,3) | B16: Solar forecast |
| `forecast_wind_mw` | NUMERIC(12,3) | B19: Wind Onshore forecast |
| `forecast_wind_offshore_mw` | NUMERIC(12,3) | B18: Wind Offshore forecast |
| `created_at` | TIMESTAMP | Record creation time |

**Unique Constraints:** `(trade_date, period)`, `(trade_date, time_interval)`

---

## entsoe_generation_scheduled

**Purpose:** Day-ahead scheduled total generation (A71).

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment ID |
| `trade_date` | DATE | Delivery date |
| `period` | INTEGER | 15-min period (1-96) |
| `time_interval` | VARCHAR(11) | Time slot |
| `scheduled_total_mw` | NUMERIC(12,3) | Total scheduled generation |
| `created_at` | TIMESTAMP | Record creation time |

**Unique Constraints:** `(trade_date, period)`, `(trade_date, time_interval)`

---

## entsoe_load

**Purpose:** Actual total load and day-ahead forecast (A65).

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment ID |
| `trade_date` | DATE | Delivery date |
| `period` | INTEGER | 15-min period (1-96) |
| `time_interval` | VARCHAR(11) | Time slot |
| `actual_load_mw` | NUMERIC(12,3) | Actual total load |
| `forecast_load_mw` | NUMERIC(12,3) | Day-ahead forecast load |
| `created_at` | TIMESTAMP | Record creation time |

**Unique Constraints:** `(trade_date, period)`, `(trade_date, time_interval)`

---

## entsoe_cross_border_flows

**Purpose:** Physical cross-border flows for CZ borders (A11).

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment ID |
| `trade_date` | DATE | Delivery date |
| `period` | INTEGER | 15-min period (1-96) |
| `time_interval` | VARCHAR(11) | Time slot |
| `delivery_datetime` | TIMESTAMP | Full delivery timestamp |
| `area_id` | VARCHAR(20) | EIC code of reference area |
| `flow_de_mw` | NUMERIC(12,3) | Flow to/from Germany (+import/-export) |
| `flow_at_mw` | NUMERIC(12,3) | Flow to/from Austria |
| `flow_pl_mw` | NUMERIC(12,3) | Flow to/from Poland |
| `flow_sk_mw` | NUMERIC(12,3) | Flow to/from Slovakia |
| `flow_total_net_mw` | NUMERIC(12,3) | Sum of all border flows |
| `created_at` | TIMESTAMP | Record creation time |

**Unique Constraints:** `(delivery_datetime, area_id)`, `(trade_date, period, area_id)`

---

## entsoe_scheduled_cross_border_flows

**Purpose:** Day-ahead scheduled commercial exchanges for CZ borders (A09).

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment ID |
| `trade_date` | DATE | Delivery date |
| `period` | INTEGER | 15-min period (1-96) |
| `time_interval` | VARCHAR(11) | Time slot |
| `scheduled_de_mw` | NUMERIC(12,3) | Scheduled exchange with Germany |
| `scheduled_at_mw` | NUMERIC(12,3) | Scheduled exchange with Austria |
| `scheduled_pl_mw` | NUMERIC(12,3) | Scheduled exchange with Poland |
| `scheduled_sk_mw` | NUMERIC(12,3) | Scheduled exchange with Slovakia |
| `scheduled_total_net_mw` | NUMERIC(12,3) | Sum of all scheduled exchanges |
| `created_at` | TIMESTAMP | Record creation time |

**Unique Constraints:** `(trade_date, period)`, `(trade_date, time_interval)`

---

## entsoe_balancing_energy

**Purpose:** Activated balancing energy prices (A84) - aFRR/mFRR.

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment ID |
| `trade_date` | DATE | Delivery date |
| `period` | INTEGER | 15-min period (1-96) |
| `time_interval` | VARCHAR(11) | Time slot |
| `afrr_up_price_eur` | NUMERIC(12,3) | aFRR upward activation price (EUR/MWh) |
| `afrr_down_price_eur` | NUMERIC(12,3) | aFRR downward activation price |
| `mfrr_up_price_eur` | NUMERIC(12,3) | mFRR upward activation price |
| `mfrr_down_price_eur` | NUMERIC(12,3) | mFRR downward activation price |
| `created_at` | TIMESTAMP | Record creation time |

**Unique Constraints:** `(trade_date, period)`, `(trade_date, time_interval)`

**Business Types:** A95 (aFRR), A96 (mFRR)

---

## entsoe_imbalance_prices

**Purpose:** Imbalance prices and volumes (A85/A86).

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment ID |
| `trade_date` | DATE | Delivery date |
| `period` | INTEGER | 15-min period (1-96) |
| `time_interval` | VARCHAR(11) | Time slot |
| `pos_imb_price_czk_mwh` | NUMERIC(15,3) | Positive imbalance price |
| `pos_imb_scarcity_czk_mwh` | NUMERIC(15,3) | Positive imbalance scarcity |
| `pos_imb_incentive_czk_mwh` | NUMERIC(15,3) | Positive imbalance incentive |
| `pos_imb_financial_neutrality_czk_mwh` | NUMERIC(15,3) | Positive financial neutrality |
| `neg_imb_price_czk_mwh` | NUMERIC(15,3) | Negative imbalance price |
| `neg_imb_scarcity_czk_mwh` | NUMERIC(15,3) | Negative imbalance scarcity |
| `neg_imb_incentive_czk_mwh` | NUMERIC(15,3) | Negative imbalance incentive |
| `neg_imb_financial_neutrality_czk_mwh` | NUMERIC(15,3) | Negative financial neutrality |
| `imbalance_mwh` | NUMERIC(12,5) | Total imbalance volume |
| `difference_mwh` | NUMERIC(12,5) | Difference volume |
| `situation` | VARCHAR | Market situation |
| `status` | VARCHAR | Data status |
| `delivery_datetime` | TIMESTAMP WITH TZ | Full delivery timestamp |
| `created_at` | TIMESTAMP | Record creation time |

**Unique Constraints:** `(trade_date, period)`, `(trade_date, time_interval)`

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

**Formula:** `Period = (Hour × 4) + (Minute ÷ 15) + 1`

---

## Query Examples

### Total German Generation (All 4 TSOs)

```sql
SELECT trade_date, period, time_interval,
       SUM(gen_wind_mw) as total_wind_onshore,
       SUM(gen_wind_offshore_mw) as total_wind_offshore,
       SUM(gen_solar_mw) as total_solar
FROM finance.entsoe_generation_actual
WHERE country_code = 'DE'  -- All German TSOs via partition pruning
  AND trade_date = '2025-12-15'
GROUP BY trade_date, period, time_interval
ORDER BY period;
```

### Generation by Country

```sql
SELECT country_code,
       SUM(gen_nuclear_mw) as nuclear,
       SUM(gen_coal_mw) as coal,
       SUM(gen_gas_mw) as gas,
       SUM(gen_solar_mw) as solar,
       SUM(gen_wind_mw) as wind
FROM finance.entsoe_generation_actual
WHERE trade_date = '2025-12-15' AND period = 48
GROUP BY country_code
ORDER BY country_code;
```

### Forecast vs Actual Error

```sql
SELECT g.trade_date, g.period,
       f.forecast_solar_mw - g.gen_solar_mw as solar_error_mw,
       f.forecast_wind_mw - g.gen_wind_mw as wind_error_mw
FROM finance.entsoe_generation_actual g
JOIN finance.entsoe_generation_forecast f
  ON g.trade_date = f.trade_date AND g.period = f.period
WHERE g.country_code = 'CZ' AND g.area_id = 1  -- CZ only
ORDER BY g.trade_date, g.period;
```

### Count Records by Country and Area

```sql
SELECT country_code, area_id, COUNT(*) as records,
       MIN(trade_date) as first_date, MAX(trade_date) as last_date
FROM finance.entsoe_generation_actual
GROUP BY country_code, area_id
ORDER BY country_code, area_id;
```

---

## Indexes

| Table | Index | Columns |
|-------|-------|---------|
| `entsoe_generation_actual` | `ix_entsoe_generation_actual_trade_date` | `trade_date` |
| `entsoe_cross_border_flows` | (unique constraint) | `delivery_datetime, area_id` |
| `entsoe_cross_border_flows` | (unique constraint) | `trade_date, period, area_id` |

---

## Data Runners

| Runner | Table | Schedule | Description |
|--------|-------|----------|-------------|
| `entsoe_unified_gen_runner.py` | `entsoe_generation_actual` | Every 15 min | Fetches A75 data for all 8 areas (CZ, DE×4, AT, PL, SK) |
| `entsoe_flow_runner.py` | `entsoe_cross_border_flows` | Every 15 min | Fetches A11 physical flows for CZ borders |
| `entsoe_balancing_runner.py` | `entsoe_balancing_energy` | Every 15 min | Fetches A84 aFRR/mFRR prices |

**Runner Usage:**
```bash
# Normal run (last 3 hours)
python3 /app/scripts/runners/entsoe_unified_gen_runner.py

# Dry run (no database writes)
python3 /app/scripts/runners/entsoe_unified_gen_runner.py --dry-run

# Backfill historical data
python3 /app/scripts/runners/entsoe_unified_gen_runner.py --start 2025-01-01 --end 2025-12-01
```

---

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
