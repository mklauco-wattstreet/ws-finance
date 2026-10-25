"""ENTSO-E tables: key on delivery_datetime (UTC instant) instead of local labels.

Revision ID: 083
Revises: 082
Create Date: 2026-10-25

Why: on the fall-back day (last Sunday of October) 02:00-03:00 local happens
twice. The ENTSO-E parsers numbered periods by wall clock (hour*4 + minute/15
+ 1), so both passes shared one key and the second pass overwrote (or, in the
summing parsers, was added to) the first; the 60-min tables are keyed on the
repeated local label; the outages and procured-capacity aggregations failed
outright on the duplicate key. As in migration 082 for OTE, a TIMESTAMPTZ
delivery_datetime (UTC start of the interval) becomes the key, and period
switches to the OTE numbering counted from local midnight (1..92/96/100), so
it lines up with the OTE tables again. trade_date / period / time_interval
stay as descriptive columns.

Scope - exactly these 30 tables in schema finance, nothing else:
  A. 15-min, wall-clock period, no delivery_datetime yet (ADD + fill):
     entsoe_balancing_energy, entsoe_day_ahead_prices, entsoe_generation_actual,
     entsoe_generation_forecast, entsoe_generation_forecast_current,
     entsoe_generation_forecast_intraday, entsoe_generation_scheduled,
     entsoe_load, entsoe_scheduled_cross_border_flows
     + entsoe_imbalance_prices (column exists as TIMESTAMPTZ, always NULL: fill)
  B. 15-min with a naive local delivery_datetime (ALTER TYPE -> TIMESTAMPTZ):
     entsoe_cross_border_flows, entsoe_congestion_income,
     entsoe_intraday_transfer_capacity
  C. 60-min, label-keyed:
     ADD + fill: entsoe_day_ahead_prices_60min, entsoe_generation_actual_60min,
                 entsoe_generation_forecast_60min, entsoe_load_60min,
                 entsoe_scheduled_cross_border_flows_60min
     fill only:  entsoe_imbalance_prices_60min (TIMESTAMPTZ, partly NULL)
     ALTER TYPE: entsoe_congestion_income_60min, entsoe_intraday_transfer_capacity_60min,
                 entsoe_cross_border_flows_60min
  D. Procured capacity (ADD + fill from label): entsoe_procured_capacity_raw,
     entsoe_procured_capacity_15min, entsoe_procured_capacity_60min
  E. Outages (already TIMESTAMPTZ and filled): entsoe_outages_15min, entsoe_outages_60min
Not touched: entsoe_areas, entsoe_outages, entsoe_outage_points, the view
grafana_dashboards.imbalance_prices_eur (no column it reads changes type or
goes away), and every other schema.

Steps (one transaction; any failure rolls everything back):
 1. ADD COLUMN delivery_datetime TIMESTAMPTZ where missing; ALTER TYPE the
    naive ones (local wall time AT TIME ZONE 'Europe/Prague').
 2. Fill:
      - 15-min tables: (trade_date + (period-1) x 15 min) AT TIME ZONE 'Europe/Prague'
        (period is still the wall-clock number here)
      - 60-min / procured: (trade_date + start of time_interval) AT TIME ZONE 'Europe/Prague'
    An ambiguous 02:xx on a past fall-back day resolves to the second (CET)
    pass; re-fetching those days afterwards writes both passes.
 3. SET NOT NULL.
 4. Drop the label / wall-period keys (found by column list) and add the
    delivery_datetime keys.
 5. 15-min tables: renumber period to the count from local midnight. Changes
    rows only after the clock change on DST days (spring: 13..96 -> 9..92).
 6. 15-min tables: re-add (trade_date, period, area_id, country_code) as UNIQUE.

No column or table is dropped. Every statement names finance.<table>
explicitly and the session pins search_path to finance.
"""

from alembic import op
from sqlalchemy import text


revision = '083'
down_revision = '082'
branch_labels = None
depends_on = None

SCHEMA = 'finance'
TZ = 'Europe/Prague'

# 15-min tables: (table, ddl) where ddl is 'add' | 'fill' | 'retype' | 'none'
QUARTER_TABLES = [
    ('entsoe_balancing_energy', 'add'),
    ('entsoe_day_ahead_prices', 'add'),
    ('entsoe_generation_actual', 'add'),
    ('entsoe_generation_forecast', 'add'),
    ('entsoe_generation_forecast_current', 'add'),
    ('entsoe_generation_forecast_intraday', 'add'),
    ('entsoe_generation_scheduled', 'add'),
    ('entsoe_load', 'add'),
    ('entsoe_scheduled_cross_border_flows', 'add'),
    ('entsoe_imbalance_prices', 'fill'),
    ('entsoe_cross_border_flows', 'retype'),
    ('entsoe_congestion_income', 'retype'),
    ('entsoe_intraday_transfer_capacity', 'retype'),
    ('entsoe_outages_15min', 'none'),
]

# Old key of each 15-min table: (constraint type, columns).
QUARTER_OLD_KEYS = {
    'entsoe_congestion_income': ('p', ['trade_date', 'time_interval', 'area_id', 'country_code']),
    'entsoe_intraday_transfer_capacity': (
        'p', ['trade_date', 'time_interval', 'area_id', 'country_code', 'product']),
}
QUARTER_DEFAULT_OLD_KEY = ('p', ['trade_date', 'period', 'area_id', 'country_code'])

# Label-keyed tables: (table, ddl, old key (type, columns), new key (type, columns, name))
LABEL_TABLES = [
    ('entsoe_day_ahead_prices_60min', 'add',
     ('p', ['trade_date', 'time_interval', 'area_id', 'country_code']),
     ('p', ['delivery_datetime', 'area_id', 'country_code'], 'entsoe_day_ahead_prices_60min_pkey')),
    ('entsoe_generation_actual_60min', 'add',
     ('p', ['trade_date', 'time_interval', 'area_id', 'country_code']),
     ('p', ['delivery_datetime', 'area_id', 'country_code'], 'entsoe_generation_actual_60min_pkey')),
    ('entsoe_generation_forecast_60min', 'add',
     ('p', ['trade_date', 'time_interval', 'area_id', 'country_code']),
     ('p', ['delivery_datetime', 'area_id', 'country_code'], 'entsoe_generation_forecast_60min_pkey')),
    ('entsoe_load_60min', 'add',
     ('p', ['trade_date', 'time_interval', 'area_id', 'country_code']),
     ('p', ['delivery_datetime', 'area_id', 'country_code'], 'entsoe_load_60min_pkey')),
    ('entsoe_imbalance_prices_60min', 'fill',
     ('p', ['trade_date', 'time_interval', 'area_id', 'country_code']),
     ('p', ['delivery_datetime', 'area_id', 'country_code'], 'entsoe_imbalance_prices_60min_pkey')),
    ('entsoe_congestion_income_60min', 'retype',
     ('p', ['trade_date', 'time_interval', 'area_id', 'country_code']),
     ('p', ['delivery_datetime', 'area_id', 'country_code'], 'entsoe_congestion_income_60min_pkey')),
    ('entsoe_intraday_transfer_capacity_60min', 'retype',
     ('p', ['trade_date', 'time_interval', 'area_id', 'country_code', 'product']),
     ('p', ['delivery_datetime', 'area_id', 'country_code', 'product'],
      'entsoe_intraday_transfer_capacity_60min_pkey')),
    ('entsoe_outages_60min', 'none',
     ('p', ['trade_date', 'time_interval', 'area_id', 'country_code']),
     ('p', ['delivery_datetime', 'area_id', 'country_code'], 'entsoe_outages_60min_pkey')),
    # Not partitioned. The UNIQUE (delivery_datetime, area_id) already exists
    # and survives the type change; only the label key goes.
    ('entsoe_cross_border_flows_60min', 'retype',
     ('u', ['trade_date', 'time_interval', 'area_id']),
     None),
    ('entsoe_scheduled_cross_border_flows_60min', 'add',
     ('u', ['trade_date', 'time_interval']),
     ('u', ['delivery_datetime'], 'entsoe_scheduled_cross_border_flows_60min_delivery_datetime_key')),
    ('entsoe_procured_capacity_raw', 'add',
     ('u', ['trade_date', 'time_interval', 'reserve_type', 'direction', 'offer_seq']),
     ('u', ['delivery_datetime', 'reserve_type', 'direction', 'offer_seq'],
      'entsoe_procured_capacity_raw_delivery_datetime_key')),
    ('entsoe_procured_capacity_15min', 'add',
     ('u', ['trade_date', 'time_interval']),
     ('u', ['delivery_datetime'], 'entsoe_procured_capacity_15min_delivery_datetime_key')),
    ('entsoe_procured_capacity_60min', 'add',
     ('u', ['trade_date', 'time_interval']),
     ('u', ['delivery_datetime'], 'entsoe_procured_capacity_60min_delivery_datetime_key')),
]

QUARTER_NEW_KEY_COLUMNS = {
    'entsoe_intraday_transfer_capacity': ['delivery_datetime', 'area_id', 'country_code', 'product'],
}
QUARTER_DEFAULT_NEW_KEY_COLUMNS = ['delivery_datetime', 'area_id', 'country_code']

# Elapsed 15-min periods since local midnight of trade_date, 1-based.
ORDINAL_PERIOD = (
    f"(extract(epoch FROM delivery_datetime - (trade_date::timestamp AT TIME ZONE '{TZ}')) / 900)::int + 1"
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


def _prepare_column(table, ddl):
    if ddl == 'add':
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ADD COLUMN delivery_datetime TIMESTAMPTZ")
    elif ddl == 'retype':
        op.execute(
            f"ALTER TABLE {SCHEMA}.{table} ALTER COLUMN delivery_datetime TYPE TIMESTAMPTZ "
            f"USING delivery_datetime AT TIME ZONE '{TZ}'"
        )


def _add_key(table, contype, columns, name):
    kind = 'PRIMARY KEY' if contype == 'p' else 'UNIQUE'
    op.execute(f'ALTER TABLE {SCHEMA}.{table} ADD CONSTRAINT "{name}" {kind} ({", ".join(columns)})')


def upgrade() -> None:
    bind = op.get_bind()
    op.execute(f"SET LOCAL search_path TO {SCHEMA}")
    op.execute("SET LOCAL lock_timeout = '5s'")

    # 1. Column DDL.
    for table, ddl in QUARTER_TABLES:
        _prepare_column(table, ddl)
    for table, ddl, _old, _new in LABEL_TABLES:
        _prepare_column(table, ddl)

    # 2. Fill (only where it was not already a correct instant).
    for table, ddl in QUARTER_TABLES:
        if ddl in ('add', 'fill'):
            op.execute(
                f"UPDATE {SCHEMA}.{table} SET delivery_datetime = "
                f"(trade_date + (period - 1) * interval '15 minutes') AT TIME ZONE '{TZ}'"
            )
    for table, ddl, _old, _new in LABEL_TABLES:
        if ddl in ('add', 'fill'):
            op.execute(
                f"UPDATE {SCHEMA}.{table} SET delivery_datetime = "
                f"(trade_date + substring(time_interval, 1, 5)::time) AT TIME ZONE '{TZ}'"
            )

    # 3. NOT NULL (fails the whole migration if any row was left unfilled).
    for table, _ddl in QUARTER_TABLES:
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ALTER COLUMN delivery_datetime SET NOT NULL")
    for table, _ddl, _old, _new in LABEL_TABLES:
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ALTER COLUMN delivery_datetime SET NOT NULL")

    # 4. Swap keys.
    for table, _ddl in QUARTER_TABLES:
        contype, cols = QUARTER_OLD_KEYS.get(table, QUARTER_DEFAULT_OLD_KEY)
        old_name = _constraint_name(bind, table, cols, contype)
        op.execute(f'ALTER TABLE {SCHEMA}.{table} DROP CONSTRAINT "{old_name}"')
        _add_key(table, 'p', QUARTER_NEW_KEY_COLUMNS.get(table, QUARTER_DEFAULT_NEW_KEY_COLUMNS),
                 f"{table}_pkey")
    for table, _ddl, (old_type, old_cols), new in LABEL_TABLES:
        old_name = _constraint_name(bind, table, old_cols, old_type)
        op.execute(f'ALTER TABLE {SCHEMA}.{table} DROP CONSTRAINT "{old_name}"')
        if new is not None:
            _add_key(table, *new)

    # 5. period -> count from local midnight (no-op except after a clock change).
    for table, _ddl in QUARTER_TABLES:
        op.execute(
            f"UPDATE {SCHEMA}.{table} SET period = {ORDINAL_PERIOD} WHERE period <> {ORDINAL_PERIOD}"
        )

    # 6. The descriptive (trade_date, period) stays unique per area.
    for table, _ddl in QUARTER_TABLES:
        cols = ['trade_date', 'period', 'area_id', 'country_code']
        if table == 'entsoe_intraday_transfer_capacity':
            cols.append('product')
        _add_key(table, 'u', cols, f"{table}_trade_date_period_key")


def downgrade() -> None:
    """Restore the label / wall-period keys; keep delivery_datetime.

    Fails if a fall-back day has been loaded since the upgrade (100 periods,
    repeated labels cannot satisfy the old keys). Columns added by the upgrade
    are dropped again; retyped columns go back to naive local time. The period
    renumbering is not reversed.
    """
    op.execute(f"SET LOCAL search_path TO {SCHEMA}")
    op.execute("SET LOCAL lock_timeout = '5s'")
    for table, _ddl in QUARTER_TABLES:
        op.execute(f'ALTER TABLE {SCHEMA}.{table} DROP CONSTRAINT "{table}_trade_date_period_key"')
        op.execute(f'ALTER TABLE {SCHEMA}.{table} DROP CONSTRAINT "{table}_pkey"')
        contype, cols = QUARTER_OLD_KEYS.get(table, QUARTER_DEFAULT_OLD_KEY)
        _add_key(table, contype, cols, f"{table}_pkey")
    for table, _ddl, (old_type, old_cols), new in LABEL_TABLES:
        if new is not None:
            op.execute(f'ALTER TABLE {SCHEMA}.{table} DROP CONSTRAINT "{new[2]}"')
        name = f"{table}_pkey" if old_type == 'p' else f"{table}_{'_'.join(old_cols)}_key"[:63]
        _add_key(table, old_type, old_cols, name)
    for table, ddl in QUARTER_TABLES + [(t, d) for t, d, _o, _n in LABEL_TABLES]:
        if ddl == 'add':
            op.execute(f"ALTER TABLE {SCHEMA}.{table} DROP COLUMN delivery_datetime")
        elif ddl == 'retype':
            op.execute(
                f"ALTER TABLE {SCHEMA}.{table} ALTER COLUMN delivery_datetime TYPE TIMESTAMP "
                f"USING delivery_datetime AT TIME ZONE '{TZ}'"
            )
        elif ddl == 'fill':
            op.execute(f"ALTER TABLE {SCHEMA}.{table} ALTER COLUMN delivery_datetime DROP NOT NULL")
