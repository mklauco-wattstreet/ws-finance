"""Add ote_prices_ida_60min table.

Revision ID: 058
Revises: 057
Create Date: 2026-06-01

60-minute view of OTE intraday auction prices (IDA1/IDA2/IDA3).
See docs/60min_tables_plan.md §4.2.

Provisional: aggregator design pending verification of whether IDAs
publish hourly products natively upstream. The DDL is identical
either way.
"""

from alembic import op


revision = '058'
down_revision = '057'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE ote_prices_ida_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            ida_idx INTEGER NOT NULL,
            price_eur_mwh NUMERIC(10, 2),
            volume_mwh NUMERIC(12, 3),
            saldo_dm_mwh NUMERIC(12, 3),
            export_mwh NUMERIC(12, 3),
            import_mwh NUMERIC(12, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE (trade_date, time_interval, ida_idx)
        );
    """)
    op.execute("""
        CREATE INDEX ix_ote_prices_ida_60min_trade_date
        ON ote_prices_ida_60min (trade_date);
    """)
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ote_prices_ida_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE ote_prices_ida_60min_id_seq TO user_finance;")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ote_prices_ida_60min CASCADE;")
