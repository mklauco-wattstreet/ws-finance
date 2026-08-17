"""Add okte_prices_ida_60min table.

Revision ID: 075
Revises: 074
Create Date: 2026-08-17

60-minute mirror of okte_prices_ida (migration 074), modeled on
ote_prices_ida_60min (migration 058). See app/backfill/backfill_okte_ida_60min.py
for the aggregation SQL: price is volume-weighted VWAP, volumes and the six
border-flow columns are summed, GROUP BY (trade_date, hour, ida_idx) with the
standard HAVING COUNT(DISTINCT time_interval) = 4 completeness gate.
"""

from alembic import op


revision = '075'
down_revision = '074'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE okte_prices_ida_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            ida_idx INTEGER NOT NULL,
            price_eur_mwh NUMERIC(10, 2),
            volume_mwh NUMERIC(12, 3),
            saldo_dm_mwh NUMERIC(12, 3),
            export_mwh NUMERIC(12, 3),
            import_mwh NUMERIC(12, 3),
            flow_sk_cz NUMERIC(12, 3),
            flow_cz_sk NUMERIC(12, 3),
            flow_sk_hu NUMERIC(12, 3),
            flow_hu_sk NUMERIC(12, 3),
            flow_sk_pl NUMERIC(12, 3),
            flow_pl_sk NUMERIC(12, 3),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            CONSTRAINT okte_prices_ida_60min_trade_date_time_interval_ida_idx_key UNIQUE (trade_date, time_interval, ida_idx)
        );
    """)
    op.execute("""
        CREATE INDEX ix_okte_prices_ida_60min_trade_date
        ON okte_prices_ida_60min (trade_date);
    """)
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE okte_prices_ida_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE okte_prices_ida_60min_id_seq TO user_finance;")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS okte_prices_ida_60min CASCADE;")
