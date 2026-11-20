# Cross-border data: implementation plan

Status: drafted 2026-11-20. Local development only. Nothing here is committed, pushed or run on production.

Goal: store every cross-border signal for the CZ borders (level 0) in tables that take the borders of CZ's neighbours (level 1) by adding rows, not columns.

## Ground rules

- The existing tables and runners that the prediction service reads keep their meaning. `flow_de_mw` and `scheduled_de_mw` stay TenneT-only. The full CZ-DE border goes into the new tables.
- No table or column is dropped.
- All schema changes are Alembic migrations. Models in `app/models.py` are updated in lockstep.
- `app/entsoe/client.py` (1505 lines) and `app/entsoe/parsers.py` (2154 lines) are over the 1500-line cap. New code goes into new modules; these two files only get bug fixes.
- New runners subclass `BaseRunner` and keep the shared CLI (`--debug`, `--dry-run`, `--start`, `--end`).
- "No matching data" from ENTSO-E is a normal empty result, never an error.
- Local test command pattern (the long-running container is not needed, and its cron is not started):
  `docker compose run --rm --no-deps --entrypoint python3 entsoe-ote-data-uploader -m runners.<name> --start 2026-11-12 --end 2026-11-19 --debug`
- Local tests cover at most 7 days.

## Phase 1: fix the current runners and tables

| # | Item | Action |
|---|---|---|
| 1.1 | Congestion income is empty on production after 2026-08-21 (one backfill on that day, one single day written on 2026-10-25). ENTSO-E returns data for November 2026. | Reproduce the cron-mode run locally, find why it writes nothing, fix. |
| 1.2 | `scheduled_total_net_mw` becomes NULL when the sum is exactly 0.0 (`app/entsoe/parsers.py`, `ScheduledExchangesParser`). | Store 0.0 when at least one border has a value; NULL only when all borders are missing. Check the same pattern in `CrossBorderFlowsParser`. |
| 1.3 | Holes in physical flows (about 320 intervals in Aug-Sep 2026) and scheduled exchanges. | Find out whether the holes are missing at the source or a parsing defect (compare one holed day against the API). Fix if it is ours; the refill happens in phase 3. |
| 1.4 | `app/runners/README.md` lists only two of the cross-border runners. | Leave for the documentation step at the end. |

Not defects, no action: `published_at` is empty for the three intraday-auction products because those documents carry no update time; the intraday-capacity runner already uses the DE-LU zone.

## Phase 2: tables and runners for all level 0 data

### 2.1 Schema (migration 090)

Border register:

`entsoe_borders`: `border_id` smallint PK, `border_code` text unique (for example `CZ-DE_LU`), `area_a_id`, `area_b_id` (FK to `entsoe_areas`), `kind` (`bidding_zone` or `control_area`), `level` smallint, `capacity_region` text, `is_active` boolean.

Seed rows, active: CZ-DE_LU (1,10), CZ-DE_TENNET (1,2), CZ-DE_50HERTZ (1,6), CZ-AT (1,3), CZ-PL (1,4), CZ-SK (1,5), all level 0, region Core.
Seed rows, inactive, level 1, to prove the design: DE_LU-AT (10,3), DE_LU-PL (10,4), AT-HU (3,9), SK-HU (5,9), PL-SK (4,5).

Data tables. Every row is directed: energy or capacity goes from `from_area_id` to `to_area_id` (ENTSO-E `out_Domain` to `in_Domain`). None of these is partitioned.

| Table | Key | Value columns |
|---|---|---|
| `entsoe_border_flows` (15 min) | `delivery_datetime`, `from_area_id`, `to_area_id` | `trade_date`, `period`, `time_interval`, `physical_mw`, `scheduled_da_mw`, `scheduled_total_mw`, `created_at`, `updated_at` |
| `entsoe_border_capacity` (mixed resolution) | `period_start`, `from_area_id`, `to_area_id`, `item`, `horizon` | `period_end`, `resolution`, `value`, `unit`, `created_at`, `updated_at` |
| `entsoe_net_positions` (15 min, LIST-partitioned by `country_code`, CZ partition) | `delivery_datetime`, `area_id`, `country_code` | `trade_date`, `period`, `time_interval`, `net_position_da_mw`, `net_position_intraday_mw`, `sched_net_position_da_mw`, `sched_net_position_total_mw`, audit columns |
| `entsoe_transmission_outages` (events) | `doc_mrid`, `revision_number`, `timeseries_mrid` | `from_area_id`, `to_area_id`, `business_type`, `doc_status`, `created_datetime`, asset mrid, name, location and type (nullable), `unavail_start`, `unavail_end`, `quantity_unit`, `curve_type`, `min_capacity_mw`, `reason_code`, `reason_text`, audit columns |
| `entsoe_transmission_outage_points` | outage row id, `point_start` | `point_end`, `capacity_mw` |
| `entsoe_congestion_costs` (monthly) | `period_start`, `area_id`, `cost_type` | `period_end`, `amount_eur`, audit columns |
| `jao_core_max_net_position` (hourly) | `delivery_datetime`, `hub` | `min_mw`, `max_mw` |
| `jao_core_max_exchange` (hourly) | `delivery_datetime`, `from_hub`, `to_hub` | `max_bex_mw` |
| `jao_core_net_position` (15 min) | `delivery_datetime`, `hub`, `market` | `net_position_mw` |
| `jao_core_scheduled_exchange` (15 min) | `delivery_datetime`, `from_hub`, `to_hub`, `market` | `scheduled_mw` |
| `jao_core_price_spread` (15 min) | `delivery_datetime`, `from_hub`, `to_hub`, `market` | `spread_eur` |
| `jao_core_intraday_atc` (15 min) | `delivery_datetime`, `from_hub`, `to_hub`, `calc_round` | `atc_mw`, `ntc_mw` |
| `jao_core_allocation_constraint` (15 min) | `delivery_datetime`, `hub`, `market` | `limit_up_mw`, `limit_down_mw` |

`entsoe_border_capacity.item` values: `ntc_forecast`, `offered_explicit`, `offered_intraday`, `already_allocated`, `nominated`, `use_requested`, `use_allocated`, `auction_price`, `auction_revenue`. `horizon` values: `day`, `week`, `month`, `year`, `idct`, `ida1`, `ida2`, `ida3`.

JAO `market` values: `da`, `ida1`, `ida2`, `ida3`. JAO `calc_round`: `A` to `E`. JAO tables use JAO's own hub codes as text, and store every Core hub and border that a response contains, so level 1 comes for free.

All JAO tables carry `created_at`, `updated_at`.

Shared code written with the schema:
- `app/entsoe/borders.py`: loads active borders from `entsoe_borders`, with area EIC codes.
- `app/entsoe/query.py`: one generic `fetch_document(client, params)` that sends any parameter set through the client's session and retry logic, handles ZIP responses, and returns `None` on "No matching data".

### 2.2 Runners

| Runner | Source items | Table |
|---|---|---|
| `entsoe_border_flow_runner` | Physical flows (A11); scheduled exchanges day-ahead (A09, A01) and total (A09, A05); every active border, both directions | `entsoe_border_flows` |
| `entsoe_border_capacity_runner` | Capacity forecast week, month, year (A61); offered explicit month and year (A31, A02); intraday offered (A31, A07); already allocated (A26, A29); nominated (A26, B08); use of capacity (A25, B05); auction revenue (A25, B07) | `entsoe_border_capacity` |
| `entsoe_net_position_runner` | Net positions day-ahead and intraday (A25, B09); schedule net positions (A09, B09); monthly congestion costs (A92) | `entsoe_net_positions`, `entsoe_congestion_costs` |
| `entsoe_transmission_outages_runner` | Transmission unavailability (A78), per border, both directions | `entsoe_transmission_outages`, `entsoe_transmission_outage_points` |
| `jao_core_runner` | JAO Publication Tool: `maxNetPos`, `maxExchanges`, `netPos`, `scheduledExchanges`, `priceSpread`, `allocationConstraint`, intraday `intradayAtc` and `intradayNtc` for rounds A to E, intraday-auction `netPos`, `scheduledExchanges`, `priceSpread` | the seven `jao_core_*` tables |
| CEPS pipeline, new datasets | `CrossborderPowerFlows`, `EmergencyExchange`, `ExportImportVykonu`; columns to be read from a live response first | new `ceps_*` tables, migration 091 |

Confirmed empty at the source for CZ borders, so no runner: day-ahead capacity forecast, day-ahead offered capacity, redispatching, countertrading, flow-based parameters on ENTSO-E, per-border congestion income.

Added after the live check: `entsoe_border_balancing_runner` (migration 092) stores balancing capacity limits per border, exchanged mFRR volumes per border, and netted and exchanged volumes for the CZ area, in `entsoe_border_balancing` and `entsoe_area_balancing_exchange`.

Deferred, named so they are not forgotten: the JAO flow-based domain (about 20,000 rows per hour), JAO long-term auction results (needs a token), the aFRR cross-border marginal price (4-second) and area control error (1-minute), 60-minute mirror tables for the new tables, cron lines.

Known defect, not fixed here: `entsoe_cross_border_flows` has stored the import value only (exports lost) since 2025-12-24, because `CrossBorderFlowsParser` writes both directions into one column and the last one parsed wins. `entsoe_border_flows` stores each direction separately and is correct.

## Phase 3: backfill from 2026-06-01

Local verification: each runner for 2026-11-12 to 2026-11-19, then row counts per border and direction.

Production backfill (to be run by hand after deploy; each is longer than 30 seconds):

```
docker compose -f docker-compose.yml -f docker-compose.prod.yml exec entsoe-ote-data-uploader python3 -m runners.<runner> --start 2026-06-01 --end 2026-11-21
```

for each of: `entsoe_congestion_income_runner`, `entsoe_unified_flow_runner`, `entsoe_unified_sched_flow_runner`, and the five new runners. The CEPS command follows the existing pipeline CLI.

## Open decisions

1. Switch `flow_de_mw` and `scheduled_de_mw` to the full CZ-DE border, or keep TenneT-only. It changes a model input.
2. Cron schedules for the new runners.
3. 60-minute mirrors for the new tables.
