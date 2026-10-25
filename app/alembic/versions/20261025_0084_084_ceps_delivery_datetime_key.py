"""CEPS tables: key on delivery_datetime (UTC instant) instead of local time.

Revision ID: 084
Revises: 083
Create Date: 2026-10-25

Why: CEPS publishes every value with its UTC offset, so on the fall-back day
(last Sunday of October) both passes of 02:00-03:00 arrive (1500 minutes,
100 quarter-hours). The parser dropped the offset, so the 1-min tables keyed
on naive local delivery_timestamp, and the 15/60-min tables keyed on
(trade_date, time_interval), let the second pass overwrite the first. As in
migrations 082 (OTE) and 083 (ENTSO-E), a TIMESTAMPTZ delivery_datetime (UTC
start of the minute / interval) becomes the key. delivery_timestamp,
trade_date and time_interval keep their meaning as descriptive local values.

All CEPS tables are RANGE-partitioned (1-min by delivery_timestamp, the rest
by trade_date) and a unique key on a partitioned table must contain the
partition column, so the new keys are:
  1-min:         UNIQUE (delivery_datetime, delivery_timestamp)
  15/60-min:     UNIQUE (delivery_datetime, trade_date)
Both are unique on delivery_datetime alone in practice (the second column is
a function of it). The PRIMARY KEY (trade_date, time_interval, id) present on
some 15/60-min tables is left as it is (id keeps it unique).

Scope - exactly these 25 tables in schema finance, nothing else:
  1-min:   ceps_actual_imbalance_1min, ceps_actual_re_price_1min,
           ceps_export_import_svr_1min, ceps_generation_res_1min,
           ceps_svr_activation_1min
  15-min:  ceps_1min_features_15min, ceps_actual_imbalance_15min,
           ceps_actual_re_price_15min, ceps_derived_features_15min,
           ceps_estimated_imbalance_price_15min, ceps_export_import_svr_15min,
           ceps_generation_15min, ceps_generation_plan_15min,
           ceps_generation_res_15min, ceps_svr_activation_15min
  60-min:  the ten matching ceps_*_60min tables

Steps (one transaction; any failure rolls everything back):
 1. ADD COLUMN delivery_datetime TIMESTAMPTZ.
 2. Fill from local time AT TIME ZONE 'Europe/Prague':
      1-min:      delivery_timestamp
      15/60-min:  trade_date + start of time_interval
    An ambiguous 02:xx on a past fall-back day resolves to the second (CET)
    pass, which is the pass the old key kept; re-fetching those days
    afterwards writes the first pass too. The one exception is a label that
    ends before it starts ("02:45-02:00", how CEPS names the last quarter of
    the first pass): it is the first pass, one hour earlier.
 3. SET NOT NULL.
 4. Drop the local-time UNIQUE key (found by column list) and add the
    delivery_datetime one.

No column or table is dropped. Every statement names finance.<table>
explicitly and the session pins search_path to finance.
"""

from alembic import op
from sqlalchemy import text


revision = '084'
down_revision = '083'
branch_labels = None
depends_on = None

SCHEMA = 'finance'
TZ = 'Europe/Prague'

MINUTE_TABLES = [
    'ceps_actual_imbalance_1min',
    'ceps_actual_re_price_1min',
    'ceps_export_import_svr_1min',
    'ceps_generation_res_1min',
    'ceps_svr_activation_1min',
]

_INTERVAL_BASES = [
    'ceps_1min_features',
    'ceps_actual_imbalance',
    'ceps_actual_re_price',
    'ceps_derived_features',
    'ceps_estimated_imbalance_price',
    'ceps_export_import_svr',
    'ceps_generation',
    'ceps_generation_plan',
    'ceps_generation_res',
    'ceps_svr_activation',
]
INTERVAL_TABLES = [f"{b}_{r}" for r in ('15min', '60min') for b in _INTERVAL_BASES]

MINUTE_OLD_KEY = ['delivery_timestamp']
MINUTE_NEW_KEY = ['delivery_datetime', 'delivery_timestamp']
INTERVAL_OLD_KEY = ['trade_date', 'time_interval']
INTERVAL_NEW_KEY = ['delivery_datetime', 'trade_date']

# CEPS labels the last quarter of the first fall-back pass "02:45-02:00"
# (end before start, not midnight). AT TIME ZONE resolves 02:45 to the second
# (CET) pass, so such a label is moved back one hour to the first pass.
FIRST_PASS_SHIFT = (
    "- CASE WHEN substring(time_interval, 7, 5) < substring(time_interval, 1, 5) "
    "AND substring(time_interval, 7, 5) <> '00:00' THEN interval '1 hour' ELSE interval '0' END"
)


def _constraint_name(bind, table, columns, contype):
    """Name of the single constraint of `contype` on finance.<table> over exactly `columns`."""
    rows = bind.execute(text("""
        SELECT c.conname
        FROM pg_constraint c
        WHERE c.conrelid = to_regclass(:tbl)
          AND c.contype = :contype
          AND (SELECT array_agg(a.attname::text ORDER BY a.attname::text COLLATE "C")
               FROM unnest(c.conkey) AS k(attnum)
               JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum)
              = CAST(:cols AS text[])
    """), {'tbl': f'{SCHEMA}.{table}', 'contype': contype, 'cols': sorted(columns)}).fetchall()
    if len(rows) != 1:
        raise RuntimeError(
            f"expected exactly one {contype!r} constraint on {SCHEMA}.{table} {columns}, found {len(rows)}"
        )
    return rows[0][0]


def _new_key_name(table):
    return f"{table}_delivery_datetime_key"


def upgrade() -> None:
    bind = op.get_bind()
    op.execute(f"SET LOCAL search_path TO {SCHEMA}")
    op.execute("SET LOCAL lock_timeout = '5s'")

    # 1. Column.
    for table in MINUTE_TABLES + INTERVAL_TABLES:
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ADD COLUMN delivery_datetime TIMESTAMPTZ")

    # 2. Fill.
    for table in MINUTE_TABLES:
        op.execute(
            f"UPDATE {SCHEMA}.{table} SET delivery_datetime = delivery_timestamp AT TIME ZONE '{TZ}'"
        )
    for table in INTERVAL_TABLES:
        op.execute(
            f"UPDATE {SCHEMA}.{table} SET delivery_datetime = "
            f"(trade_date + substring(time_interval, 1, 5)::time) AT TIME ZONE '{TZ}' {FIRST_PASS_SHIFT}"
        )

    # 3. NOT NULL (fails the whole migration if any row was left unfilled).
    for table in MINUTE_TABLES + INTERVAL_TABLES:
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ALTER COLUMN delivery_datetime SET NOT NULL")

    # 4. Swap keys.
    for tables, old_cols, new_cols in ((MINUTE_TABLES, MINUTE_OLD_KEY, MINUTE_NEW_KEY),
                                       (INTERVAL_TABLES, INTERVAL_OLD_KEY, INTERVAL_NEW_KEY)):
        for table in tables:
            old_name = _constraint_name(bind, table, old_cols, 'u')
            op.execute(f'ALTER TABLE {SCHEMA}.{table} DROP CONSTRAINT "{old_name}"')
            op.execute(
                f'ALTER TABLE {SCHEMA}.{table} ADD CONSTRAINT "{_new_key_name(table)}" '
                f'UNIQUE ({", ".join(new_cols)})'
            )


def downgrade() -> None:
    """Restore the local-time UNIQUE keys and drop delivery_datetime.

    Fails if a fall-back day has been loaded since the upgrade (both passes
    share the local key). The original constraint names are not restored.
    """
    op.execute(f"SET LOCAL search_path TO {SCHEMA}")
    op.execute("SET LOCAL lock_timeout = '5s'")
    for tables, old_cols in ((MINUTE_TABLES, MINUTE_OLD_KEY), (INTERVAL_TABLES, INTERVAL_OLD_KEY)):
        for table in tables:
            op.execute(f'ALTER TABLE {SCHEMA}.{table} DROP CONSTRAINT "{_new_key_name(table)}"')
            op.execute(
                f'ALTER TABLE {SCHEMA}.{table} ADD CONSTRAINT "{table}_{"_".join(old_cols)}_key"'
                f' UNIQUE ({", ".join(old_cols)})'
            )
            op.execute(f"ALTER TABLE {SCHEMA}.{table} DROP COLUMN delivery_datetime")
