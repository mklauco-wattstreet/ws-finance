---
name: Schema Patterns
description: Table inventory, partitioning strategy, column type choices, driver details
type: project
---

## Driver & Library
- psycopg2 (sync), version pinned in Dockerfile (not asyncpg)
- psycopg2.extras.execute_values with page_size=1000 for bulk upserts
- SQLAlchemy ORM only used for model definitions and Alembic migrations; all runtime queries are raw SQL
- pgbouncer in production (DNS name "james"); connection strings go to pgbouncer port

## Table Inventory (finance schema)
~25+ tables. Key groups:

### Partitioned by country_code (LIST)
- entsoe_generation_actual (CZ, DE, AT, PL, SK)
- entsoe_imbalance_prices (CZ, DE, AT, PL, SK, HU)
- entsoe_day_ahead_prices (HU, DE, AT)
- entsoe_generation_forecast_intraday (by country_code)
- entsoe_generation_forecast_current (by country_code)

### Partitioned by year RANGE (CEPS tables)
- ceps_actual_imbalance_1min / _15min (2024–2028)
- ceps_actual_re_price_1min / _15min (2024–2028)
- ceps_svr_activation_1min / _15min (2024–2028)
- ceps_export_import_svr_1min / _15min (2024–2028)
- ceps_generation_res_1min / _15min (2024–2028)
- ceps_1min_features_15min (2024–2028)
- ceps_derived_features_15min (year range)

### Flat tables (no partitioning)
- ote_prices_day_ahead, ote_prices_day_ahead_60min, ote_prices_imbalance
- ote_prices_intraday_market, ote_trade_balance, ote_daily_payments, ote_prices_ida
- entsoe_load, entsoe_balancing_energy, entsoe_generation_scheduled
- entsoe_cross_border_flows, entsoe_scheduled_cross_border_flows
- entsoe_generation_forecast (CZ-only, no partition)
- da_bid, da_period_summary, da_curve_depth
- cnb_exchange_rate, entsoe_areas

## Key Data Volumes (estimated)
- CEPS 1-min tables: ~1440 rows/day per dataset; ~525k rows/year
- OTE 15-min tables: 96 rows/day; ~35k rows/year
- ENTSOE partitioned: 96 rows/day × 5-6 countries; up to ~210k rows/year per table
- da_bid: ~17,000 rows/day; ~6.2M rows/year

## PK Strategy
- Partitioned tables: composite PK includes country_code (required for list partitioning)
- CEPS year-partitioned: composite PK includes delivery_timestamp or (trade_date, time_interval)
- da_bid: PK = (delivery_date, period, side, price, order_resolution)
- Most flat tables: surrogate id PK + unique constraint on natural key

## search_path Approach
SET search_path TO finance is issued per-connection as a session-level setting.
In production this is safe because pgbouncer uses transaction pooling — search_path is NOT reliable across borrowed connections unless SET is done per-transaction. This is a known risk.
