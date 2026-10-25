"""OTE / DAM tables: key on delivery_datetime (UTC instant) instead of local labels.

Revision ID: 082
Revises: 081
Create Date: 2026-10-25

Why: on the fall-back day (last Sunday of October) 02:00-03:00 local happens
twice, so a local label like "02:00-02:15" is not unique. OTE publishes such
days with 100 quarter-hour periods; uploads failed on UNIQUE (trade_date,
time_interval) and on 13-char labels like "02a:00-02a:15" overflowing
VARCHAR(11). A new column delivery_datetime TIMESTAMPTZ (UTC start of the
interval) becomes the unique key; trade_date / period / time_interval stay as
descriptive columns, and period keeps OTE's 1..92/96/100 numbering.

Scope - exactly these 12 tables in schema finance, nothing else:
    ote_prices_day_ahead, ote_prices_day_ahead_60min,
    ote_prices_imbalance, ote_prices_imbalance_60min,
    ote_prices_intraday_market, ote_prices_ida, ote_prices_ida_60min,
    ote_trade_balance,
    da_period_summary, da_period_summary_60min,
    da_curve_depth, da_curve_depth_60min
(da_bid is unchanged: its key already uses the period number.)

Steps (one transaction; any failure rolls everything back):
 1. ote_trade_balance: delete zero-only rows labelled with a local time that
    does not exist (02:xx on a spring-forward day; 4 rows on 2026-03-29,
    written by the old 96-period zero-fill). Aborts if any such row is non-zero.
 2. ADD COLUMN delivery_datetime TIMESTAMPTZ and fill it:
      - tables whose period follows OTE numbering: local midnight + (period-1) x 15 min
        (ote_prices_day_ahead_60min: + (period_60-1) x 1 h)
      - the others: (date + start of time_interval) AT TIME ZONE 'Europe/Prague'
 3. SET NOT NULL.
 4. Replace the label-based unique keys with delivery_datetime keys
    (constraints are found by their column list, so dev/prod naming
    differences do not matter).
 5. ote_trade_balance: renumber period to OTE numbering (changes only rows
    after the clock change: 84 rows on 2026-03-29, 13..96 -> 9..92, and 24
    rows on 2026-10-25, 13..60 -> 17..64).
 6. ote_prices_day_ahead: relabel time_interval where the old 96-slot
    arithmetic wrote a wrong label (85 rows on 2025-03-30).

No other table, schema or object is touched. Every statement names
finance.<table> explicitly and the session pins search_path to finance.
"""

from alembic import op
from sqlalchemy import text


revision = '082'
down_revision = '081'
branch_labels = None
depends_on = None

SCHEMA = 'finance'

# table -> (date column, period column, minutes per period); period is OTE numbering
SLOT_FILL = {
    'ote_prices_day_ahead': ('trade_date', 'period', 15),
    'ote_prices_imbalance': ('trade_date', 'period', 15),
    'ote_prices_intraday_market': ('trade_date', 'period', 15),
    'ote_prices_ida': ('trade_date', 'period', 15),
    'ote_prices_day_ahead_60min': ('trade_date', 'period_60', 60),
    'da_period_summary': ('delivery_date', 'period', 15),
    'da_curve_depth': ('delivery_date', 'period', 15),
}

# table -> date column; delivery_datetime comes from the time_interval start
LABEL_FILL = {
    'ote_prices_ida_60min': 'trade_date',
    'ote_prices_imbalance_60min': 'trade_date',
    'ote_trade_balance': 'delivery_date',
    'da_period_summary_60min': 'delivery_date',
    'da_curve_depth_60min': 'delivery_date',
}

# table -> (type, old key columns or None, new key columns, new constraint name)
KEYS = {
    'ote_prices_day_ahead': ('u', ['trade_date', 'time_interval'], ['delivery_datetime'],
                             'ote_prices_day_ahead_delivery_datetime_key'),
    'ote_prices_imbalance': ('u', ['trade_date', 'time_interval'], ['delivery_datetime'],
                             'ote_prices_imbalance_delivery_datetime_key'),
    'ote_prices_intraday_market': ('u', ['trade_date', 'time_interval'], ['delivery_datetime'],
                                   'ote_prices_intraday_market_delivery_datetime_key'),
    'ote_prices_day_ahead_60min': ('u', ['trade_date', 'time_interval'], ['delivery_datetime'],
                                   'ote_prices_day_ahead_60min_delivery_datetime_key'),
    'ote_prices_ida': ('u', None, ['delivery_datetime', 'ida_idx'],
                       'ote_prices_ida_delivery_datetime_ida_idx_key'),
    'ote_prices_ida_60min': ('u', ['trade_date', 'time_interval', 'ida_idx'], ['delivery_datetime', 'ida_idx'],
                             'ote_prices_ida_60min_delivery_datetime_ida_idx_key'),
    'ote_prices_imbalance_60min': ('u', ['trade_date', 'time_interval'], ['delivery_datetime'],
                                   'ote_prices_imbalance_60min_delivery_datetime_key'),
    'ote_trade_balance': ('u', ['delivery_date', 'time_interval'], ['delivery_datetime'],
                          'ote_trade_balance_delivery_datetime_key'),
    'da_period_summary': ('u', None, ['delivery_datetime'],
                          'da_period_summary_delivery_datetime_key'),
    'da_curve_depth': ('u', None, ['delivery_datetime'],
                       'da_curve_depth_delivery_datetime_key'),
    'da_period_summary_60min': ('p', ['delivery_date', 'time_interval'], ['delivery_datetime'],
                                'da_period_summary_60min_pkey'),
    'da_curve_depth_60min': ('p', ['delivery_date', 'time_interval'], ['delivery_datetime'],
                             'da_curve_depth_60min_pkey'),
}

TABLES = list(KEYS)

TRADE_BALANCE_VALUE_COLUMNS = [
    'total_buy_mw', 'total_sell_mw', 'daily_market_buy_mw', 'daily_market_sell_mw',
    'intraday_auction_buy_mw', 'intraday_auction_sell_mw', 'intraday_market_buy_mw',
    'intraday_market_sell_mw', 'realization_diagrams_buy_mw', 'realization_diagrams_sell_mw',
    'total_buy_mwh', 'total_sell_mwh', 'daily_market_buy_mwh', 'daily_market_sell_mwh',
    'intraday_auction_buy_mwh', 'intraday_auction_sell_mwh', 'intraday_market_buy_mwh',
    'intraday_market_sell_mwh', 'realization_diagrams_buy_mwh', 'realization_diagrams_sell_mwh',
]

# Local wall time of a label start that does not exist (spring-forward gap):
# converting to UTC and back does not return the same wall time.
_TB_WALL = "(delivery_date + substring(time_interval, 1, 5)::time)"
_TB_NONEXISTENT = (
    f"(({_TB_WALL} AT TIME ZONE 'Europe/Prague') AT TIME ZONE 'Europe/Prague') <> {_TB_WALL}"
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


def upgrade() -> None:
    bind = op.get_bind()
    op.execute(f"SET LOCAL search_path TO {SCHEMA}")
    op.execute("SET LOCAL lock_timeout = '5s'")

    # 1. Phantom zero rows at non-existent local times (spring-forward gap).
    nonzero = " OR ".join(f"COALESCE({c}, 0) <> 0" for c in TRADE_BALANCE_VALUE_COLUMNS)
    bad = bind.execute(text(
        f"SELECT count(*) FROM {SCHEMA}.ote_trade_balance WHERE {_TB_NONEXISTENT} AND ({nonzero})"
    )).scalar()
    if bad:
        raise RuntimeError(
            f"{bad} non-zero ote_trade_balance row(s) at non-existent local times; refusing to delete"
        )
    op.execute(f"DELETE FROM {SCHEMA}.ote_trade_balance WHERE {_TB_NONEXISTENT}")

    # 2. Add + fill delivery_datetime.
    for table in TABLES:
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ADD COLUMN delivery_datetime TIMESTAMPTZ")

    for table, (date_col, period_col, minutes) in SLOT_FILL.items():
        op.execute(
            f"UPDATE {SCHEMA}.{table} SET delivery_datetime = "
            f"({date_col}::timestamp AT TIME ZONE 'Europe/Prague') "
            f"+ ({period_col} - 1) * interval '{minutes} minutes'"
        )

    for table, date_col in LABEL_FILL.items():
        op.execute(
            f"UPDATE {SCHEMA}.{table} SET delivery_datetime = "
            f"({date_col} + substring(time_interval, 1, 5)::time) AT TIME ZONE 'Europe/Prague'"
        )

    # 3. NOT NULL (fails the whole migration if any row was left unfilled).
    for table in TABLES:
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ALTER COLUMN delivery_datetime SET NOT NULL")

    # 4. Swap keys.
    for table, (contype, old_cols, new_cols, new_name) in KEYS.items():
        if old_cols is not None:
            old_name = _constraint_name(bind, table, old_cols, contype)
            op.execute(f'ALTER TABLE {SCHEMA}.{table} DROP CONSTRAINT "{old_name}"')
        kind = 'PRIMARY KEY' if contype == 'p' else 'UNIQUE'
        op.execute(
            f'ALTER TABLE {SCHEMA}.{table} ADD CONSTRAINT "{new_name}" {kind} ({", ".join(new_cols)})'
        )

    # 5. Trade balance period -> OTE numbering.
    op.execute(f"""
        UPDATE {SCHEMA}.ote_trade_balance
        SET period = (extract(epoch FROM delivery_datetime
                      - (delivery_date::timestamp AT TIME ZONE 'Europe/Prague')) / 900)::int + 1
        WHERE period <> (extract(epoch FROM delivery_datetime
                         - (delivery_date::timestamp AT TIME ZONE 'Europe/Prague')) / 900)::int + 1
    """)

    # 6. ote_prices_day_ahead labels written by the old 96-slot arithmetic on
    #    spring-forward days (85 rows on 2025-03-30): relabel from delivery_datetime.
    start = "to_char(delivery_datetime AT TIME ZONE 'Europe/Prague', 'HH24:MI')"
    end = "to_char((delivery_datetime + interval '15 minutes') AT TIME ZONE 'Europe/Prague', 'HH24:MI')"
    label = f"{start} || '-' || CASE WHEN {end} = '00:00' THEN '24:00' ELSE {end} END"
    op.execute(
        f"UPDATE {SCHEMA}.ote_prices_day_ahead SET time_interval = {label} "
        f"WHERE time_interval <> {label}"
    )


def downgrade() -> None:
    """Restore the label keys and drop delivery_datetime.

    Fails if a fall-back day (repeated 02:xx labels) has been loaded since the
    upgrade - those rows cannot satisfy the old label keys. The step-1 delete
    and step-5 renumbering are not reversed.
    """
    op.execute(f"SET LOCAL search_path TO {SCHEMA}")
    op.execute("SET LOCAL lock_timeout = '5s'")
    for table, (contype, old_cols, new_cols, new_name) in KEYS.items():
        op.execute(f'ALTER TABLE {SCHEMA}.{table} DROP CONSTRAINT "{new_name}"')
        if old_cols is not None:
            kind = 'PRIMARY KEY' if contype == 'p' else 'UNIQUE'
            old_name = f"{table}_pkey" if contype == 'p' else f"{table}_{'_'.join(old_cols)}_key"
            op.execute(
                f'ALTER TABLE {SCHEMA}.{table} ADD CONSTRAINT "{old_name}" {kind} ({", ".join(old_cols)})'
            )
    for table in TABLES:
        op.execute(f"ALTER TABLE {SCHEMA}.{table} DROP COLUMN delivery_datetime")
