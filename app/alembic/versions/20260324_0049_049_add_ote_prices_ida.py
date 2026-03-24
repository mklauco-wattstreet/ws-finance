"""Add ote_prices_ida table for OTE Intraday Auction prices.

Revision ID: 049
Revises: 048
Create Date: 2026-03-24

Stores IDA1, IDA2, IDA3 intraday auction results from OTE-CR.
96 rows per trade day per IDA index (15-minute intervals).
"""

from alembic import op

revision = '049'
down_revision = '048'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE ote_prices_ida (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            ida_idx INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            price_eur_mwh NUMERIC(10, 2),
            volume_mwh NUMERIC(12, 3),
            saldo_dm_mwh NUMERIC(12, 3),
            export_mwh NUMERIC(12, 3),
            import_mwh NUMERIC(12, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE (trade_date, period, ida_idx),
            CHECK (ida_idx IN (1, 2, 3))
        );
    """)

    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ote_prices_ida TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE ote_prices_ida_id_seq TO user_finance;")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ote_prices_ida CASCADE;")
