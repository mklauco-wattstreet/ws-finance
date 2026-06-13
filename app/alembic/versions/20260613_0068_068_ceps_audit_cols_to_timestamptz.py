"""Convert CEPS audit columns (created_at/updated_at) to timestamptz — 15min/60min tier.

Revision ID: 068
Revises: 067
Create Date: 2026-06-13

CONTEXT
-------
CEPS audit columns were declared `timestamp without time zone` (naive) but filled
by DEFAULT CURRENT_TIMESTAMP while the Postgres session timezone is GMT — so they
hold a *UTC wall-clock with no zone label*. `delivery_timestamp`, by contrast, is
a naive *Prague* wall-clock (migration 029). Mixing the two zones makes a plain
`created_at - delivery_timestamp` return a nonsense (~ -2h) value.

This migration makes the audit columns honest instants by reinterpreting the
stored naive value AS UTC (it IS UTC) — a relabel, NOT a shift. Verified by exact
round-trip on live data: `x = (x AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'`.
UTC has no DST, so there is no ambiguous/non-existent hour: the conversion is
provably lossless.

SCOPE
-----
This revision converts the audit columns (created_at / updated_at) on the SMALL
CEPS tables only: *_15min and *_60min. The five *_1min tables are handled by
revision 069, which in ONE ALTER per table both converts their audit columns and
adds the `delivery_instant` generated column — so each large minute-table is
rewritten exactly once. `delivery_timestamp` stays naive Prague everywhere.

Audit columns are NOT the partition key and NOT used for local bucketing, so a
plain in-place ALTER is safe (none of the insert/aggregation hazards that made the
delivery_timestamp conversion infeasible).

Columns already `timestamptz` (e.g. several *_60min, ceps_actual_imbalance_15min)
are skipped by the data_type filter — this migration is idempotent.

PRE-FLIGHT (must hold or the ALTER fails):
  * Tables must be owned by the migration role (user_finance). Older CEPS tables
    may be owned by `postgres`; reassign ownership first (see migration 067 note).
  * Verify the conversion zone is correct for THIS environment: confirm
    `SHOW timezone` is GMT/UTC and that a sample created_at matches a known UTC
    ingest time. The `AT TIME ZONE 'UTC'` relabel is only correct if the stored
    naive audit values are UTC wall-clock (true in dev; confirm in prod).
  * ALTER COLUMN TYPE takes ACCESS EXCLUSIVE and rewrites partitions. The 15min/
    60min tables are tiny (96/24 rows/day) so this is fast; still run with the
    CEPS cron paused.
"""

from alembic import op

revision = '068'
down_revision = '067'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    DO $$
    DECLARE r record;
    BEGIN
      FOR r IN
        SELECT c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_schema = c.table_schema AND t.table_name = c.table_name
        WHERE c.table_schema = 'finance'
          AND t.table_type = 'BASE TABLE'
          AND c.table_name LIKE 'ceps_%'
          AND c.table_name NOT LIKE '%\\_1min' ESCAPE '\\'          -- 1min handled by rev 069 (single rewrite)
          AND c.table_name NOT SIMILAR TO '%_(2024|2025|2026|2027|2028|2029)'  -- parents only
          AND c.column_name IN ('created_at', 'updated_at')
          AND c.data_type = 'timestamp without time zone'           -- skip already-tz
        ORDER BY c.table_name, c.column_name
      LOOP
        EXECUTE format(
          'ALTER TABLE finance.%I ALTER COLUMN %I TYPE timestamptz USING (%I AT TIME ZONE ''UTC'')',
          r.table_name, r.column_name, r.column_name);
        RAISE NOTICE 'tz-converted finance.%.%', r.table_name, r.column_name;
      END LOOP;
    END $$;
    """)


def downgrade() -> None:
    # NOT supported on purpose: a blind timestamptz->naive sweep cannot tell columns
    # this migration converted apart from those that were already timestamptz before
    # it ran (e.g. the *_60min audit columns), and would silently corrupt the latter.
    # To roll back, restore from backup or revert specific columns by hand.
    raise NotImplementedError("Downgrade not supported — see migration docstring.")
