"""Add ote_prices_day_ahead_60min table for OTE 60-minute DA contracts.

Revision ID: 047
Revises: 046
Create Date: 2026-03-22

Stores day-ahead 60-minute contract prices from OTE-CR.
24 rows per trade day (one per hour).
"""

from alembic import op

revision = '047'
down_revision = '046'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE ote_prices_day_ahead_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            period_60 INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            price_60min_eur_mwh NUMERIC(10, 2) NOT NULL,
            volume_mwh NUMERIC(12, 3) NOT NULL,
            purchase_15min_products_mwh NUMERIC(12, 3) NOT NULL,
            purchase_60min_products_mwh NUMERIC(12, 3) NOT NULL,
            sale_15min_products_mwh NUMERIC(12, 3) NOT NULL,
            sale_60min_products_mwh NUMERIC(12, 3) NOT NULL,
            saldo_dm_mwh NUMERIC(12, 3) NOT NULL,
            export_mwh NUMERIC(12, 3) NOT NULL,
            import_mwh NUMERIC(12, 3) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE (trade_date, period_60),
            UNIQUE (trade_date, time_interval)
        );
    """)

    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ote_prices_day_ahead_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE ote_prices_day_ahead_60min_id_seq TO user_finance;")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ote_prices_day_ahead_60min CASCADE;")
