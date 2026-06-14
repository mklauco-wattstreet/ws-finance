"""Convert ENTSO-E audit columns (created_at/updated_at) to timestamptz.

Revision ID: 070
Revises: 069
Create Date: 2026-06-14

CONTEXT
-------
Every ENTSO-E table's audit columns (created_at / updated_at) were declared
`timestamp without time zone` (naive) and filled by CURRENT_TIMESTAMP via the
column DEFAULT and by BaseRunner.bulk_upsert (`updated_at = CURRENT_TIMESTAMP`).
A naive value written by CURRENT_TIMESTAMP means a different absolute instant
depending on the Postgres session timezone: local-dev Postgres defaults to GMT
(so the value is UTC wall-clock), production defaults to Europe/Prague (so it is
Prague wall-clock). The interpretation of created_at/updated_at therefore differs
per environment — the exact ambiguity already reconciled for CEPS in 068/069.

This migration makes the audit columns honest instants by reinterpreting each
stored naive value in the SESSION timezone it was written in — a relabel, NOT a
shift. The wall-clock reading in that same session is unchanged; only the (now
explicit and correct) zone is attached. It does NOT touch any business timestamp
(trade_date, period, delivery_datetime) — those stay exactly as they are.

SCOPE
-----
* All `entsoe_%` BASE / partitioned-parent tables, audit columns only. Country
  partitions are NOT altered directly — ALTER on a partitioned parent cascades to
  every partition atomically (and altering a child partition's column type
  independently would error). Child partitions are excluded via pg_inherits.
* `entsoe_areas` is EXCLUDED on purpose: it is a static bidding-zone lookup, not a
  pipeline data table (and only has updated_at). Left naive.
* Columns already `timestamptz` are skipped by the type filter (idempotent).
  Business timestamps (delivery_datetime on imbalance_prices/outages, the naive
  delivery_datetime on the flow tables) are out of scope — only created_at /
  updated_at are touched.

DISCOVERY USES pg_catalog, NOT information_schema (IMPORTANT)
-----------------------------------------------------------
The first draft selected tables from information_schema.columns, which is
PRIVILEGE-FILTERED: it omits tables the executing role cannot access. Several
ENTSO-E tables (entsoe_generation_forecast_intraday / _current and their
partitions) are owned by `postgres` (a latent defect from migration 045), so the
alembic role (user_finance) could not see them and they were SILENTLY SKIPPED.
This revision discovers targets via pg_catalog (pg_class/pg_attribute), which is
not privilege-filtered, and ends with an assertion that FAILS if any ENTSO-E audit
column is still naive — so an un-converted table can never pass unnoticed again.

PRE-REQUISITE (must hold on every environment BEFORE running):
  Every `entsoe_%` table must be OWNED BY the alembic role (user_finance). ALTER
  COLUMN TYPE requires ownership; a postgres-owned table will make this migration
  fail (loudly, by design). Fix first as a superuser:
    DO $$ DECLARE r record; BEGIN
      FOR r IN SELECT c.relname FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='finance'
        WHERE c.relkind IN ('r','p')
          AND (c.relname LIKE 'entsoe_generation_forecast_intraday%'
            OR c.relname LIKE 'entsoe_generation_forecast_current%')
      LOOP EXECUTE format('ALTER TABLE finance.%I OWNER TO user_finance', r.relname); END LOOP;
    END $$;

WHY THE RELABEL IS CORRECT (verified pre-flight, 2026-06-14)
-----------------------------------------------------------
The conversion zone is current_setting('TimeZone') — the zone the naive values
were written in (dev=GMT, prod=Europe/Prague). This assumes that zone has been
STABLE across each table's history. Verified on prod via the flows canary:
created_at - delivery_datetime is a stable small positive lag (~29 min) in the
live regime with NO +/-2h step-change. A hardcoded zone would corrupt whichever
environment it does not match — current_setting is correct for both.

REVERSIBILITY
-------------
Every entsoe created_at/updated_at was naive before this migration (verified), so
the downgrade — a symmetric reinterpret back to naive in the same session zone,
restricted to the same tables/columns — restores the exact original values. The
whole migration is transactional (PostgreSQL transactional DDL): any mid-run
failure (including the ownership pre-req not being met) rolls back with the
database untouched.

OPERATIONAL
-----------
ALTER COLUMN TYPE timestamp -> timestamptz under a non-UTC session forces a full
table rewrite under ACCESS EXCLUSIVE. The large DE generation/load partitions are
the long pole; run with the ENTSO-E cron paused or inside a quiet 15-min window.
Run via the app/pgbouncer connection and confirm SHOW timezone = Europe/Prague
on prod BEFORE executing.
"""

from alembic import op

revision = '070'
down_revision = '069'
branch_labels = None
depends_on = None


# Target selector: entsoe_% parent/standalone tables (NOT child partitions, NOT
# entsoe_areas), audit columns only. Discovery via pg_catalog so it is NOT filtered
# by the executing role's privileges. atttypid regtype match is precision-agnostic.
_SELECT = """
        SELECT c.relname AS table_name, a.attname AS column_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'finance'
        JOIN pg_attribute a ON a.attrelid = c.oid
             AND a.attname IN ('created_at', 'updated_at')
             AND a.attnum > 0 AND NOT a.attisdropped
        WHERE c.relname LIKE 'entsoe\\_%' ESCAPE '\\'
          AND c.relkind IN ('r', 'p')                                       -- ordinary + partitioned parent
          AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid)  -- exclude child partitions
          AND c.relname <> 'entsoe_areas'                                   -- static lookup, left naive
          AND a.atttypid = '{from_type}'::regtype
        ORDER BY c.relname, a.attname
"""


def _convert(target_type: str, from_type: str) -> None:
    op.execute(f"""
    DO $$
    DECLARE r record;
    BEGIN
      FOR r IN
        {_SELECT.format(from_type=from_type)}
      LOOP
        -- Relabel using the SESSION timezone — that IS the zone these naive values
        -- were written in (CURRENT_TIMESTAMP under the app/alembic connection).
        EXECUTE format(
          'ALTER TABLE finance.%I ALTER COLUMN %I TYPE {target_type} USING (%I AT TIME ZONE current_setting(''TimeZone''))',
          r.table_name, r.column_name, r.column_name);
        RAISE NOTICE 'converted finance.%.% -> {target_type} using session tz %',
          r.table_name, r.column_name, current_setting('TimeZone');
      END LOOP;
    END $$;
    """)


def _assert_none_naive() -> None:
    # Fail loudly if ANY entsoe audit column (parent OR partition, excluding the
    # intentionally-skipped entsoe_areas) is still naive — catches privilege/
    # ownership skips that a privilege-filtered query would hide.
    op.execute("""
    DO $$
    DECLARE n int; bad text;
    BEGIN
      SELECT count(*), string_agg(c.relname || '.' || a.attname, ', ')
      INTO n, bad
      FROM pg_class c
      JOIN pg_namespace nsp ON nsp.oid = c.relnamespace AND nsp.nspname = 'finance'
      JOIN pg_attribute a ON a.attrelid = c.oid
           AND a.attname IN ('created_at', 'updated_at')
           AND a.attnum > 0 AND NOT a.attisdropped
      WHERE c.relname LIKE 'entsoe\\_%' ESCAPE '\\'
        AND c.relkind IN ('r', 'p')
        AND c.relname <> 'entsoe_areas'
        AND a.atttypid = 'timestamp without time zone'::regtype;
      IF n > 0 THEN
        RAISE EXCEPTION 'migration 070 incomplete: % entsoe audit column(s) still naive '
          '(table not owned by current_user? see ownership pre-req in docstring): %', n, bad;
      END IF;
    END $$;
    """)


def upgrade() -> None:
    _convert(target_type='timestamptz', from_type='timestamp without time zone')
    _assert_none_naive()


def downgrade() -> None:
    _convert(target_type='timestamp', from_type='timestamp with time zone')
