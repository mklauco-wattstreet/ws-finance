"""Add cnb_exchange_rate table for CZK/EUR daily fixing.

Revision ID: 046
Revises: 045
Create Date: 2026-03-12

Stores daily CZK/EUR exchange rate from Czech National Bank (CNB).
Simple non-partitioned table — single currency pair, one rate per business day.
"""

from alembic import op

revision = '046'
down_revision = '045'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE cnb_exchange_rate (
            id SERIAL,
            rate_date DATE NOT NULL,
            czk_eur NUMERIC(10,6) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (rate_date)
        );
    """)

    op.execute("""
        CREATE INDEX ix_cnb_exchange_rate_rate_date
        ON cnb_exchange_rate (rate_date);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS cnb_exchange_rate CASCADE;")
