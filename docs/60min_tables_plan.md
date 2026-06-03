# 60-Minute Tables — Foundations & Column Inventory

**Status:** Plan, awaiting architect sign-off. No migrations written yet.
**Scope:** This repo (`ws-finance`) — DDL only. Aggregator runners and cron wiring are deferred to a follow-up.

---

## 1. Why

The IDC trades both 15-min and 60-min products. `ta-feature-api` needs to serve hourly contracts using **native 60-min rows from the DB**, not Python aggregation on read. Architectural rule: ws-finance materializes source-level 60-min aggregations; the API computes any derived/recomputed feature on top of those.

This document lists every new `_60min` table, its primary key, and every column with its aggregation rule. Once accepted, it becomes the spec for the migrations and the eventual aggregator runners.

---

## 2. Scope

### In scope (this doc)

| Source family | New tables |
|---|---|
| §3.1 Day-ahead analytics | 2 |
| §3.2 IDA | 1 (provisional) |
| §3.3 Weather | 2 |
| §3.4 CEPS | 10 |
| §3.5 ENTSO-E | 7 |
| **Total** | **22** |

### Explicitly out

- All **derived** features the spec marked `recompute` — they stay in the API (ratios, spreads, surprises, slopes-of-aggregates, intensities, drifts, asymmetries, instabilities, trends, etc.).
- Tables already native at 60-min: `ote_prices_day_ahead_60min`, `idc_*_60min`, `da_bid` (uses `order_resolution` column).
- Cron schedule and runner implementation — separate document/PR.
- §3.6 IDC and §3.7 computed/contract-timing from the upstream spec — handled outside this repo.

---

## 3. Conventions (apply to every table below)

| Aspect | Rule |
|---|---|
| **Naming** | `<source>_60min` |
| **Schema** | `finance` |
| **Column types** | Identical to the 15-min source, so `INSERT … SELECT` works without casts |
| **`created_at` / `updated_at`** | `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` (timezone-aware where the source is) |
| **Temporal key** | `time_interval` only. The 15-min `period` column is **not** carried over to any 60-min table — `time_interval` is sufficient and avoids a DST-aware integer ordinal. |
| **`time_interval`** | `VARCHAR(11)`, format `'HH:MM-HH:MM'` (e.g., `'09:00-10:00'`) |
| **Partitioning** | If the source is LIST-partitioned by `country_code`, the 60-min variant uses the **same** partition scheme and partition list |
| **Indexes** | Replicate any non-PK index on `(trade_date)` that the source has |

### Aggregation rule legend

- **mean** — arithmetic mean of the four 15-min values
- **sum** — sum of the four 15-min values (volumes/energies)
- **vwap** — volume-weighted mean (price weighted by interval volume)
- **last** — value of the last quarter (`HH:45-HH+1:00`)
- **native** — re-aggregated directly from a higher-resolution source (not from 15-min)

---

## 4. Table catalogue

### 4.1 Day-ahead — §3.1

#### `finance.da_period_summary_60min`

**PK:** `(delivery_date, time_interval)`

| Column | Type | Rule |
|---|---|---|
| `delivery_date` | `DATE NOT NULL` | key |
| `time_interval` | `VARCHAR(11) NOT NULL` | key |
| `clearing_price` | `NUMERIC(10,2)` | mean |
| `clearing_volume` | `NUMERIC(12,3)` | sum |
| `supply_next_price` | `NUMERIC(10,2)` | mean |
| `supply_next_volume` | `NUMERIC(12,3)` | sum |
| `supply_price_gap` | `NUMERIC(10,2)` | mean |
| `supply_volume_gap` | `NUMERIC(12,3)` | sum |
| `demand_next_price` | `NUMERIC(10,2)` | mean |
| `demand_next_volume` | `NUMERIC(12,3)` | sum |
| `demand_price_gap` | `NUMERIC(10,2)` | mean |
| `demand_volume_gap` | `NUMERIC(12,3)` | sum |
| `created_at` | `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` | — |

#### `finance.da_curve_depth_60min`

**PK:** `(delivery_date, time_interval)`

| Column | Type | Rule |
|---|---|---|
| `delivery_date` | `DATE NOT NULL` | key |
| `time_interval` | `VARCHAR(11) NOT NULL` | key |
| `clearing_price` | `NUMERIC(10,2) NOT NULL` | mean |
| `supply_mw_from_clearing` | `NUMERIC(12,3)` | mean |
| `supply_price_from_clearing` | `NUMERIC(10,2)` | mean |
| `supply_slope` | `NUMERIC(10,4)` | mean |
| `supply_matched_mw_from_clearing` | `NUMERIC(12,3)` | mean |
| `supply_matched_price_from_clearing` | `NUMERIC(10,2)` | mean |
| `supply_matched_slope` | `NUMERIC(10,4)` | mean |
| `demand_mw_from_clearing` | `NUMERIC(12,3)` | mean |
| `demand_price_from_clearing` | `NUMERIC(10,2)` | mean |
| `demand_slope` | `NUMERIC(10,4)` | mean |
| `demand_matched_mw_from_clearing` | `NUMERIC(12,3)` | mean |
| `demand_matched_price_from_clearing` | `NUMERIC(10,2)` | mean |
| `demand_matched_slope` | `NUMERIC(10,4)` | mean |
| `created_at` | `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` | — |

> **Default chosen — source of aggregation:** aggregate from the four 15-min `da_period_summary` / `da_curve_depth` rows. The alternative — recompute directly from `da_bid WHERE order_resolution='60min'` — is more correct in principle and we can switch to it later **without changing the DDL above**. The DDL is independent of the source.

---

### 4.2 IDA — §3.2 (provisional)

#### `finance.ote_prices_ida_60min`

**PK:** `(id)` with `UNIQUE (trade_date, time_interval, ida_idx)`

| Column | Type | Rule |
|---|---|---|
| `id` | `SERIAL` | autoincrement |
| `trade_date` | `DATE NOT NULL` | key |
| `time_interval` | `VARCHAR(11) NOT NULL` | key |
| `ida_idx` | `INTEGER NOT NULL` | key, 1/2/3 |
| `price_eur_mwh` | `NUMERIC(10,2)` | vwap (by `volume_mwh`) |
| `volume_mwh` | `NUMERIC(12,3)` | sum |
| `saldo_dm_mwh` | `NUMERIC(12,3)` | sum |
| `export_mwh` | `NUMERIC(12,3)` | sum |
| `import_mwh` | `NUMERIC(12,3)` | sum |
| `created_at`, `updated_at` | `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` | — |

> **Default chosen — provisional table.** If IDA1/2/3 publish hourly products natively upstream, this table will be populated as a native source (no aggregation). Otherwise it's filled by aggregating four 15-min rows. **Verification against OTE source files is required before the aggregator is implemented.** The DDL above is identical either way.

---

### 4.3 Weather — §3.3

#### `finance.weather_current_60min`

**PK:** `(trade_date, time_interval)`

| Column | Type | Rule |
|---|---|---|
| `trade_date` | `DATE NOT NULL` | key |
| `time_interval` | `VARCHAR(11) NOT NULL` | key |
| `temperature_2m_degc` | `NUMERIC(6,2)` | mean |
| `shortwave_radiation_wm2` | `NUMERIC(8,2)` | mean |
| `direct_radiation_wm2` | `NUMERIC(8,2)` | mean |
| `cloud_cover_pct` | `NUMERIC(5,2)` | mean |
| `wind_speed_10m_kmh` | `NUMERIC(6,2)` | mean |
| `created_at`, `updated_at` | `TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP` | — |

#### `finance.weather_forecast_60min`

**PK:** `(trade_date, time_interval, forecast_made_at)`

Same column set as `weather_current_60min`, plus:

| Column | Type | Rule |
|---|---|---|
| `forecast_made_at` | `TIMESTAMPTZ NOT NULL` | key |

Aggregation: mean across the four 15-min rows that share the same `forecast_made_at`.

---

### 4.4 CEPS — §3.4 (10 tables)

**Common shape for every CEPS 60-min table:**
- PK: `(trade_date, time_interval, id)` with `UNIQUE (trade_date, time_interval)`
- `id` is `SERIAL`
- Partitioned LIST by year on `trade_date` — same `2024…2028` partitions as source
- Columns below are the *non-key* source columns + their rule

#### `finance.ceps_actual_imbalance_60min`

| Column | Type | Rule |
|---|---|---|
| `load_mean_mw` | `NUMERIC(12,5)` | mean |
| `load_median_mw` | `NUMERIC(12,5)` | mean |

#### `finance.ceps_estimated_imbalance_price_60min`

| Column | Type | Rule |
|---|---|---|
| `estimated_price_czk_mwh` | `NUMERIC(12,3)` | mean |

#### `finance.ceps_actual_re_price_60min`

All columns `NUMERIC(15,3)`. Rule: `_mean_` → mean, `_median_` → mean, `_last_at_interval_` → last.

| Group | Columns |
|---|---|
| aFRR+ | `price_afrr_plus_mean_eur_mwh`, `price_afrr_plus_median_eur_mwh`, `price_afrr_plus_last_at_interval_eur_mwh` |
| aFRR− | `price_afrr_minus_mean_eur_mwh`, `price_afrr_minus_median_eur_mwh`, `price_afrr_minus_last_at_interval_eur_mwh` |
| mFRR+ | `price_mfrr_plus_mean_eur_mwh`, `price_mfrr_plus_median_eur_mwh`, `price_mfrr_plus_last_at_interval_eur_mwh` |
| mFRR− | `price_mfrr_minus_mean_eur_mwh`, `price_mfrr_minus_median_eur_mwh`, `price_mfrr_minus_last_at_interval_eur_mwh` |
| mFRR-5 | `price_mfrr_5_mean_eur_mwh`, `price_mfrr_5_median_eur_mwh`, `price_mfrr_5_last_at_interval_eur_mwh` |

#### `finance.ceps_svr_activation_60min`

All columns `NUMERIC(15,3)`. Rule: same as above (`_mean_` → mean, `_median_` → mean, `_last_at_interval_` → last).

| Group | Columns |
|---|---|
| aFRR+ | `afrr_plus_mean_mw`, `afrr_plus_median_mw`, `afrr_plus_last_at_interval_mw` |
| aFRR− | `afrr_minus_mean_mw`, `afrr_minus_median_mw`, `afrr_minus_last_at_interval_mw` |
| mFRR+ | `mfrr_plus_mean_mw`, `mfrr_plus_median_mw`, `mfrr_plus_last_at_interval_mw` |
| mFRR− | `mfrr_minus_mean_mw`, `mfrr_minus_median_mw`, `mfrr_minus_last_at_interval_mw` |

#### `finance.ceps_export_import_svr_60min`

All columns `NUMERIC(15,5)`. Rule: `_mean_` → mean, `_median_` → mean, `_last_at_interval_` → last.

| Group | Columns |
|---|---|
| Imbalance netting | `imbalance_netting_mean_mw`, `imbalance_netting_median_mw`, `imbalance_netting_last_at_interval_mw` |
| MARI (mFRR) | `mari_mfrr_mean_mw`, `mari_mfrr_median_mw`, `mari_mfrr_last_at_interval_mw` |
| PICASSO (aFRR) | `picasso_afrr_mean_mw`, `picasso_afrr_median_mw`, `picasso_afrr_last_at_interval_mw` |
| Sum exchange | `sum_exchange_mean_mw`, `sum_exchange_median_mw`, `sum_exchange_last_at_interval_mw` |

#### `finance.ceps_generation_60min`

All `NUMERIC(12,3)`, all rule **mean**:
`tpp_mw`, `ccgt_mw`, `npp_mw`, `hpp_mw`, `pspp_mw`, `altpp_mw`, `appp_mw`, `wpp_mw`, `pvpp_mw`

#### `finance.ceps_generation_plan_60min`

| Column | Type | Rule |
|---|---|---|
| `total_mw` | `NUMERIC(12,3)` | mean |

#### `finance.ceps_generation_res_60min`

All `NUMERIC(12,3)`. Rule: `_mean_` → mean, `_median_` → mean, `_last_at_interval_` → last.

| Group | Columns |
|---|---|
| Wind | `wind_mean_mw`, `wind_median_mw`, `wind_last_at_interval_mw` |
| Solar | `solar_mean_mw`, `solar_median_mw`, `solar_last_at_interval_mw` |

#### `finance.ceps_1min_features_60min`

> **Default chosen — populated via re-aggregation from the 1-min source**, NOT from the 15-min features.
>
> Reason: aggregating distributional stats (min/max/std/skew/threshold counts) from already-aggregated 15-min stats is mathematically wrong. The aggregator for this table reads the underlying 1-min tables over a 60-min window and computes the same statistics natively. Rule for every column below is therefore **native**.

Same column set as `ceps_1min_features_15min` (30+ columns covering aFRR/mFRR distribution, imbalance distribution, threshold counts, total activation, spreads). Column names and types identical to source. Reproduced for reference:

| Group | Columns (all native re-aggregation) |
|---|---|
| Meta | `minute_count` |
| aFRR+ distribution | `afrr_plus_min_eur`, `afrr_plus_max_eur`, `afrr_plus_std_eur`, `afrr_plus_skew` |
| aFRR− distribution | `afrr_minus_min_eur`, `afrr_minus_max_eur`, `afrr_minus_std_eur`, `afrr_minus_skew` |
| mFRR+ distribution | `mfrr_plus_min_eur`, `mfrr_plus_max_eur`, `mfrr_plus_std_eur`, `mfrr_plus_skew` |
| mFRR− distribution | `mfrr_minus_min_eur`, `mfrr_minus_max_eur`, `mfrr_minus_std_eur`, `mfrr_minus_skew` |
| Imbalance distribution | `imbalance_range_mw`, `imbalance_std_mw`, `imbalance_slope` |
| Threshold counts | `minutes_at_floor`, `minutes_near_peak`, `saturation_count` |
| Golden Trio | `total_active_mean_mw`, `total_active_std_mw`, `platform_active_count`, `afrr_mfrr_plus_spread_mean_eur`, `afrr_mfrr_plus_spread_std_eur`, `afrr_mfrr_minus_spread_mean_eur`, `afrr_mfrr_minus_spread_std_eur` |

Types match `ceps_1min_features_15min` 1:1.

#### `finance.ceps_derived_features_60min`

> **Default chosen — rule `last` for every column.**
>
> Reason: the rolling fields (`imb_roll_2h`, `imb_roll_4h`, `imb_integral_4h`) already smear over multi-hour windows; the hour's "last quarter" value is the most representative single value. The error fields (`solar_error_mw`, `wind_error_mw`, `gen_total_error_mw`) are differences of two already-aggregated means at quarter-hour resolution; persisting `last` keeps them in lockstep with the rolling fields. If the API needs the cleanly-aggregated form, it can recompute from `ceps_generation_60min` and `ceps_generation_res_60min`.

| Column | Type | Rule |
|---|---|---|
| `imb_roll_2h` | `NUMERIC(12,5)` | last |
| `imb_roll_4h` | `NUMERIC(12,5)` | last |
| `imb_integral_4h` | `NUMERIC(15,5)` | last |
| `solar_error_mw` | `NUMERIC(12,3)` | last |
| `wind_error_mw` | `NUMERIC(12,3)` | last |
| `gen_total_error_mw` | `NUMERIC(12,3)` | last |

---

### 4.5 ENTSO-E — §3.5 (7 tables)

> **Default chosen — 7 tables, not 6.** Adds `entsoe_imbalance_prices_60min` to the spec's set. Rationale: the imbalance price table is consumed by the imbalance predictor and the liquidator in this repo. Having a 60-min view is consistent with the rest and avoids a future second migration.

**Common shape**
- Non-partitioned: PK `(id)` with `UNIQUE (trade_date, time_interval)`; `id SERIAL`.
- Partitioned by `country_code`: PK `(trade_date, time_interval, area_id, country_code)`; same partition list as source (CZ/DE/AT/PL/SK/HU as applicable).

#### `finance.entsoe_load_60min` (single CZ table)

| Column | Type | Rule |
|---|---|---|
| `actual_load_mw` | `NUMERIC(12,3)` | mean |
| `forecast_load_mw` | `NUMERIC(12,3)` | mean |

#### `finance.entsoe_generation_forecast_60min` (single CZ table)

| Column | Type | Rule |
|---|---|---|
| `forecast_solar_mw` | `NUMERIC(12,3)` | mean |
| `forecast_wind_mw` | `NUMERIC(12,3)` | mean |
| `forecast_wind_offshore_mw` | `NUMERIC(12,3)` | mean |

#### `finance.entsoe_generation_actual_60min` — **partitioned by `country_code`** (CZ, DE, AT, PL, SK)

| Column | Type | Rule |
|---|---|---|
| `gen_nuclear_mw` | `NUMERIC(12,3)` | mean |
| `gen_coal_mw` | `NUMERIC(12,3)` | mean |
| `gen_gas_mw` | `NUMERIC(12,3)` | mean |
| `gen_solar_mw` | `NUMERIC(12,3)` | mean |
| `gen_wind_mw` | `NUMERIC(12,3)` | mean |
| `gen_wind_offshore_mw` | `NUMERIC(12,3)` | mean |
| `gen_hydro_pumped_mw` | `NUMERIC(12,3)` | mean |
| `gen_biomass_mw` | `NUMERIC(12,3)` | mean |
| `gen_hydro_other_mw` | `NUMERIC(12,3)` | mean |

#### `finance.entsoe_cross_border_flows_60min` (single CZ-centric table)

| Column | Type | Rule |
|---|---|---|
| `delivery_datetime` | `TIMESTAMP NOT NULL` | hour start |
| `area_id` | `VARCHAR(20) NOT NULL` | key |
| `flow_de_mw` | `NUMERIC(12,3)` | mean |
| `flow_at_mw` | `NUMERIC(12,3)` | mean |
| `flow_pl_mw` | `NUMERIC(12,3)` | mean |
| `flow_sk_mw` | `NUMERIC(12,3)` | mean |
| `flow_total_net_mw` | `NUMERIC(12,3)` | mean |

UNIQUE `(delivery_datetime, area_id)` and `(trade_date, time_interval, area_id)` — mirroring source minus the `period` column.

#### `finance.entsoe_scheduled_cross_border_flows_60min` (single CZ-centric table)

| Column | Type | Rule |
|---|---|---|
| `scheduled_de_mw` | `NUMERIC(12,3)` | mean |
| `scheduled_at_mw` | `NUMERIC(12,3)` | mean |
| `scheduled_pl_mw` | `NUMERIC(12,3)` | mean |
| `scheduled_sk_mw` | `NUMERIC(12,3)` | mean |
| `scheduled_total_net_mw` | `NUMERIC(12,3)` | mean |

#### `finance.entsoe_day_ahead_prices_60min` — **partitioned by `country_code`** (DE, AT, HU)

| Column | Type | Rule |
|---|---|---|
| `price_eur_mwh` | `NUMERIC(12,3)` | mean |

#### `finance.entsoe_imbalance_prices_60min` — **partitioned by `country_code`** (CZ, DE, AT, PL, SK, HU)

> Added beyond the upstream spec — see rationale above.

| Column | Type | Rule |
|---|---|---|
| `pos_imb_price_mwh` | `NUMERIC(15,3)` | mean |
| `pos_imb_scarcity_mwh` | `NUMERIC(15,3)` | mean |
| `pos_imb_incentive_mwh` | `NUMERIC(15,3)` | mean |
| `pos_imb_financial_neutrality_mwh` | `NUMERIC(15,3)` | mean |
| `neg_imb_price_mwh` | `NUMERIC(15,3)` | mean |
| `neg_imb_scarcity_mwh` | `NUMERIC(15,3)` | mean |
| `neg_imb_incentive_mwh` | `NUMERIC(15,3)` | mean |
| `neg_imb_financial_neutrality_mwh` | `NUMERIC(15,3)` | mean |
| `imbalance_mwh` | `NUMERIC(12,5)` | sum |
| `difference_mwh` | `NUMERIC(12,5)` | sum |
| `situation` | `TEXT` | last |
| `status` | `TEXT` | last |
| `currency` | `VARCHAR(3) NOT NULL` | propagate (always identical across the four quarters of an hour for the same country) |
| `delivery_datetime` | `TIMESTAMPTZ` | hour start |

---

### 4.7 OTE-CR domestic imbalance settlement (added 2026-06-02, migration 063)

#### `finance.ote_prices_imbalance_60min`

**Source**: `finance.ote_prices_imbalance` (CZ-only, CZK/MWh — distinct from `entsoe_imbalance_prices_60min` which is per-country EUR/MWh).
**PK**: `(id)` with `UNIQUE (trade_date, time_interval)`

| Column | Type | Rule |
|---|---|---|
| `id` | `SERIAL` | autoincrement |
| `trade_date` | `DATE NOT NULL` | key |
| `time_interval` | `VARCHAR(11) NOT NULL` | key |
| `system_imbalance_mwh` | `NUMERIC(12,5)` | sum |
| `absolute_imbalance_sum_mwh` | `NUMERIC(12,5)` | sum |
| `positive_imbalance_mwh` | `NUMERIC(12,5)` | sum |
| `negative_imbalance_mwh` | `NUMERIC(12,5)` | sum |
| `rounded_imbalance_mwh` | `NUMERIC(12,5)` | sum |
| `cost_of_be_czk` | `NUMERIC(15,3)` | sum |
| `cost_of_imbalance_czk` | `NUMERIC(15,3)` | sum |
| `settlement_price_imbalance_czk_mwh` | `NUMERIC(15,3)` | mean |
| `settlement_price_counter_imbalance_czk_mwh` | `NUMERIC(15,3)` | mean |
| `price_protective_be_component_czk_mwh` | `NUMERIC(15,3)` | mean |
| `price_be_component_czk_mwh` | `NUMERIC(15,3)` | mean |
| `price_im_component_czk_mwh` | `NUMERIC(15,3)` | mean |
| `price_si_component_czk_mwh` | `NUMERIC(15,3)` | mean |
| `price_not_performed_activation_czk_mwh` | `NUMERIC(15,3)` | mean |
| `created_at`, `updated_at` | `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` | — |

> Note on settlement-price rule: kept as `mean` for consistency with the rest of the 60-min set. If a future API consumer needs the realized hour-level price for settlement-style logic, switch the two `settlement_price_*` columns to **VWAP by `rounded_imbalance_mwh`** — DDL is the same.

---

## 5. Defaults summary (overridable)

These are the choices I'm committing to so the migrations can be written. Each is structurally orthogonal to the DDL — flipping any of them later does not change the table shapes, only the aggregator runner.

| # | Decision | Default | Reverse by |
|---|---|---|---|
| 1 | DA 60-min source | Aggregate from 15-min `da_period_summary`/`da_curve_depth` | Switch the aggregator to read `da_bid WHERE order_resolution='60min'` |
| 2 | IDA hourly product | Provisional aggregator using §3.2 rules | Verify OTE files; if native hourly exists, swap aggregator for an ingester |
| 3 | `ceps_1min_features_60min` source | Re-aggregate from 1-min source | (Don't reverse — aggregating-stats-of-stats is mathematically wrong) |
| 4 | `ceps_derived_features_60min` rule | `last` for every column | Switch specific columns to recompute in the aggregator |
| 5 | `entsoe_imbalance_prices_60min` | Included (7th ENTSO-E table) | Drop the migration if you'd rather not maintain it |

---

## 6. Done criteria for the migration PR

- [ ] One Alembic migration per source family — 5 migrations total (DA, IDA, Weather, CEPS, ENTSO-E). Each is DDL-only, no DML.
- [ ] Each new table mirrors the source's PK shape, types, partitioning, and `created_at`/`updated_at` columns.
- [ ] `models.py` gains the 22 corresponding SQLAlchemy classes so `alembic autogenerate` stays clean for future changes.
- [ ] `models.py` updates do not affect any existing class.
- [ ] No code outside the migration uses the new tables yet (the aggregator runners are a separate PR).
- [ ] CLAUDE.md gets a one-line note linking to this doc and stating that all `_60min` tables are populated by aggregator runners (TBD).

---

## 7. Explicitly deferred

| Item | Where it lives |
|---|---|
| Aggregator runners (one per source family) | Follow-up PR |
| Cron schedule for the aggregators | Follow-up PR (likely at `:14,:29,:44,:59 + N` — chosen after we measure 15-min runner completion) |
| Backfill scripts for existing 15-min history | Follow-up PR |
| `ta-feature-api`-side query updates | `ta-feature-api` repo |
| `da_bid order_resolution='60min'` coverage verification | Prerequisite for switching Default #1 |
| OTE IDA native hourly product verification | Prerequisite for confirming Default #2 |

---

## 8. Sign-off

Architect approval required on:
1. The 22 tables in §4.
2. The 5 defaults in §5.
3. The done criteria in §6.

After sign-off: I write the 5 migrations + the 22 ORM classes, run them locally, confirm `alembic autogenerate` produces no diff, and open the PR.

---

## 9. Live aggregator runners (added 2026-06-02)

Closes the "Aggregator runners" deferral in §7. All `_60min` tables are populated by reusing the existing backfill scripts in live mode — the same scripts that did the historical fill, given a new `--auto` flag.

### Hard rules

1. **Hour alignment.** Every 60-min row keys on `time_interval ∈ {'00:00-01:00', '01:00-02:00', …, '23:00-00:00'}`. Always full hours, never sliding windows.
2. **Completeness gate.** A 60-min row for hour H may be written ONLY if the 15-min source has all four exact quarters of H: `'HH:00-HH:15'`, `'HH:15-HH:30'`, `'HH:30-HH:45'`, `'HH:45-HH+1:00'`. Partial hours produce **no** row.
3. **Fire often, no delay logic.** Aggregators fire every 15 minutes regardless of source state. Empty firings are the normal case most of the time. No sleeps, retry queues, source-side notifications, or "wait until source is ready" checks. Just stateless cron + idempotent SQL.

### SQL guard (single pattern)

Every 15-min-source aggregation query ends with:

```sql
GROUP BY trade_date, SUBSTRING(time_interval, 1, 2) [, …extra_partition_keys]
HAVING COUNT(DISTINCT time_interval) = 4
```

Because GROUP BY buckets by hour, the 4 distinct `time_interval` values within an hour group can only be the 4 expected quarters. The constant `HOUR_COMPLETE_HAVING` in `app/backfill/_common.py` is the single source of truth.

For partitioned tables (e.g. `entsoe_imbalance_prices_60min`, `entsoe_load_60min`), the GROUP BY also includes `area_id, country_code` and the HAVING applies per-partition: each country × area × hour needs its own four quarters.

For `weather_forecast_60min`, the GROUP BY includes `forecast_made_at`; HAVING applies per forecast snapshot × hour.

For `ceps_1min_features_60min` (native re-aggregation from the 1-min source), the equivalent guard is `HAVING COUNT(*) = 60` inside the `agg` CTE — full 60 distinct minutes required.

### CLI: `--auto`

Every backfill script now accepts `--auto`, which replaces the required `start end` positional args with a trailing 6-hour window:
- `args.start_date = (NOW(Europe/Prague) - 6h).date()`
- `args.end_date   = NOW(Europe/Prague).date()`

The script's existing day-by-day driver iterates 1–2 days. Reprocessing whole days is safe because all queries use `ON CONFLICT DO UPDATE` and the HAVING gate keeps partial hours out.

### Cron schedule

All 7 aggregators fire at `15,30,45,0 * * * *` (every 15 min, 1 minute after the source `:14,:29,:44,:59` runners). The schedule is uniform: OTE imbalance / DA tables don't update every 15 min upstream, but their aggregators safely no-op most of the time and pick up new hours when they appear.

### One-time cleanup

Existing 60-min rows from the bootstrap backfill predate the HAVING gate and include partial-hour aggregations. Run once before/during the cutover to live aggregators:

```bash
docker compose exec entsoe-ote-data-uploader python3 -m backfill.cleanup_partial_60min_rows --dry-run
docker compose exec entsoe-ote-data-uploader python3 -m backfill.cleanup_partial_60min_rows
```

For each 60-min table, the cleanup deletes rows whose corresponding hour in the source is incomplete (matched on all partition keys). Re-run each backfill script over its full source range afterwards — the HAVING gate ensures only complete hours are re-emitted.
