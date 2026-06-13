"""CEPS *_1min: audit columns -> timestamptz, plus a `delivery_instant` generated column.

Revision ID: 069
Revises: 068
Create Date: 2026-06-13

PATH 1 (chosen). We deliberately do NOT convert the naive `delivery_timestamp`
to timestamptz — that is load-bearing for every CEPS insert and local-bucket
aggregation and would require a coordinated session-timezone rollout (see the
deferred Path-2 rebuild in docs/deferred_migrations/). Instead:

  1. Convert the *_1min audit columns (created_at / updated_at) to timestamptz,
     relabeling the stored naive value AS UTC (it IS UTC; a relabel, not a shift).
  2. Add a STORED generated column `delivery_instant timestamptz` =
     delivery_timestamp AT TIME ZONE 'Europe/Prague'. This exposes the true instant
     of the delivery minute, computed and maintained BY THE DATABASE, so a plain

         created_at - delivery_instant            -- ~ +2 min, session-independent

     gives the real publication lag with no AT TIME ZONE in any application code.
     `delivery_timestamp` itself is untouched, so all existing bucketing/inserts
     keep working exactly as before.

Both subcommands for each table are issued in a SINGLE `ALTER TABLE`, so each
large minute-table is rewritten exactly ONCE.

WHY THIS IS SAFE FOR THE DATA
  * Audit relabel verified by exact round-trip on live rows; UTC has no DST, so it
    is provably lossless.
  * `delivery_instant` is purely derived from the unchanged delivery_timestamp;
    it adds information, never mutates the source. The naive->tz literal-zone
    expression is IMMUTABLE (verified) so it is valid in a STORED generated column.
  * `created_at`/`updated_at` default CURRENT_TIMESTAMP stays correct once the
    column is timestamptz (records the true instant going forward).

PRE-FLIGHT (HEAVY — run manually, CEPS cron paused, maintenance window):
  * Tables must be owned by user_finance (reassign from postgres first; see 067).
  * Adding a STORED generated column and ALTER TYPE both force a full table
    rewrite under ACCESS EXCLUSIVE; the *_1min tables hold millions of rows.
  * Confirm `SHOW timezone` is GMT/UTC and a sample created_at matches a known UTC
    ingest time before trusting the `AT TIME ZONE 'UTC'` relabel in production.
"""

from alembic import op

revision = '069'
down_revision = '068'
branch_labels = None
depends_on = None

ONE_MIN_TABLES = [
    "ceps_actual_imbalance_1min",
    "ceps_actual_re_price_1min",
    "ceps_export_import_svr_1min",
    "ceps_generation_res_1min",
    "ceps_svr_activation_1min",
]


def upgrade() -> None:
    for tbl in ONE_MIN_TABLES:
        # Single ALTER TABLE -> single rewrite: relabel audit cols as UTC instants
        # and add the derived Prague-instant column.
        op.execute(f"""
            ALTER TABLE finance.{tbl}
                ALTER COLUMN created_at TYPE timestamptz USING (created_at AT TIME ZONE 'UTC'),
                ALTER COLUMN updated_at TYPE timestamptz USING (updated_at AT TIME ZONE 'UTC'),
                ADD COLUMN delivery_instant timestamptz
                    GENERATED ALWAYS AS (delivery_timestamp AT TIME ZONE 'Europe/Prague') STORED;
        """)
        op.execute(f"""
            COMMENT ON COLUMN finance.{tbl}.delivery_instant IS
            'True instant of the delivery minute = delivery_timestamp (naive Prague) AT TIME ZONE Europe/Prague. '
            'Derived/read-only; use for instant comparisons e.g. created_at - delivery_instant.';
        """)


def downgrade() -> None:
    for tbl in ONE_MIN_TABLES:
        op.execute(f"ALTER TABLE finance.{tbl} DROP COLUMN delivery_instant;")
        # audit columns back to naive UTC wall-clock
        op.execute(f"""
            ALTER TABLE finance.{tbl}
                ALTER COLUMN created_at TYPE timestamp USING (created_at AT TIME ZONE 'UTC'),
                ALTER COLUMN updated_at TYPE timestamp USING (updated_at AT TIME ZONE 'UTC');
        """)
