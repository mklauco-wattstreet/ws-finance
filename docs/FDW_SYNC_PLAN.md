# Production-to-Dev Database Sync via postgres_fdw

## Context
Development database is lagging behind production. Data is append-only time-series (electricity market data). A permanent FDW link from dev DB to prod DB (reachable via Tailscale) allows dev to be synced on demand with a single SQL script.

## Architecture

```
┌──────────────┐   Tailscale   ┌──────────────┐
│   DEV DB     │◄──────────────│   PROD DB    │
│              │               │              │
│ finance.*    │  postgres_fdw │ finance.*    │
│ prod_fdw.*   │───────────────│              │
└──────────────┘               └──────────────┘
```

- `finance.*` — local dev tables (your working data)
- `prod_fdw.*` — foreign tables pointing to production (read-only via `user_finance_readonly`)

## Production Side (one-time)
- `pg_hba.conf`: allow dev's Tailscale IP
- `postgresql.conf`: `listen_addresses = '*'`
- User: `user_finance_readonly` (already exists, has SELECT on `finance.*`)

## Scripts

### `scripts/fdw_setup.sql` — Run once on dev DB
1. Creates `postgres_fdw` extension
2. Creates foreign server `prod_server` pointing to prod via Tailscale IP
3. Maps local dev user to `user_finance_readonly` on prod
4. Creates `prod_fdw` schema
5. Imports all tables from prod's `finance` schema into `prod_fdw`
6. Includes verification queries

**Placeholders to fill in:**
- `<TAILSCALE_IP>` — Production Tailscale IP
- `<DEV_DB_USER>` — Your local dev DB username
- `<READONLY_PASSWORD>` — Password for `user_finance_readonly`

### `scripts/fdw_sync.sql` — Run anytime to catch up
Dynamic PL/pgSQL script that syncs all tables automatically:

| Section | Tables | Method |
|---------|--------|--------|
| Lookup | `entsoe_areas` | TRUNCATE + full INSERT (keeps prod IDs) |
| Non-partitioned (11) | OTE + ENTSO-E tables | Delta by MAX(datetime_col) |
| Country-partitioned (3) | `entsoe_imbalance_prices`, `entsoe_generation_actual`, `entsoe_day_ahead_prices` | Per-country delta via child partitions (CZ/DE/AT/PL/SK/HU) |
| CEPS 1-min (5) | `ceps_actual_imbalance_1min`, `ceps_actual_re_price_1min`, `ceps_svr_activation_1min`, `ceps_export_import_svr_1min`, `ceps_generation_res_1min` | Delta via year partitions (2024-2028) |
| CEPS 15-min (8) | `ceps_actual_imbalance_15min`, `ceps_actual_re_price_15min`, `ceps_svr_activation_15min`, `ceps_export_import_svr_15min`, `ceps_generation_res_15min`, `ceps_generation_15min`, `ceps_generation_plan_15min`, `ceps_estimated_imbalance_price_15min` | Delta via year partitions (2024-2028) |

**Features:**
- Auto-discovers columns via `information_schema` (excludes `id`)
- Handles missing partitions gracefully (skips with `EXCEPTION WHEN undefined_table`)
- RAISE NOTICE for every table with row counts
- Total summary at the end
- Idempotent — safe to run multiple times

## Verification
1. Run `fdw_setup.sql` on dev DB (via psql or DataGrip)
2. Test: `SELECT COUNT(*) FROM prod_fdw.ote_prices_day_ahead;`
3. Run `fdw_sync.sql` — check NOTICE messages for row counts
4. Verify: `SELECT MAX(trade_date) FROM finance.ote_prices_day_ahead;` — should match prod
5. Run consistency checks to confirm:
   ```bash
   docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_consistency_check.py
   docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_consistency_check.py
   ```

## Notes
- FDW connection is permanent — `prod_fdw` schema stays for future syncs
- To re-import after prod schema changes: `DROP SCHEMA prod_fdw CASCADE;` then re-run steps 4+5 from setup
- Delta sync uses strict `>` comparison — at most 1 day of partial data won't be re-synced (run consistency checks to verify)
- All data is append-only, so MAX(datetime)-based delta is safe
