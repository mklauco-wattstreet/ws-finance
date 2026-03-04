"""Add da_bid and da_period_summary tables for DAM matching curves.

Revision ID: 038
Revises: 037
Create Date: 2026-03-04

Changes:
- Create da_bid table for DAM supply/demand bid stacks
- Create da_period_summary table for per-period market depth analytics
"""

from alembic import op
import sqlalchemy as sa

revision = '038'
down_revision = '037'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE da_bid (
            delivery_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            side VARCHAR(4) NOT NULL,
            price NUMERIC(10,2) NOT NULL,
            volume_bid NUMERIC(12,3) NOT NULL,
            volume_matched NUMERIC(12,3) NOT NULL,
            order_resolution VARCHAR(5) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT da_bid_pkey PRIMARY KEY (delivery_date, period, side, price, order_resolution)
        );
    """)
    op.execute("CREATE INDEX idx_da_bid_delivery_date ON da_bid (delivery_date);")
    op.execute("CREATE INDEX idx_da_bid_delivery_date_period ON da_bid (delivery_date, period);")

    op.execute("""
        CREATE TABLE da_period_summary (
            delivery_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            clearing_price NUMERIC(10,2),
            clearing_volume NUMERIC(12,3),
            supply_next_price NUMERIC(10,2),
            supply_next_volume NUMERIC(12,3),
            supply_price_gap NUMERIC(10,2),
            supply_volume_gap NUMERIC(12,3),
            demand_next_price NUMERIC(10,2),
            demand_next_volume NUMERIC(12,3),
            demand_price_gap NUMERIC(10,2),
            demand_volume_gap NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT da_period_summary_pkey PRIMARY KEY (delivery_date, period)
        );
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS da_period_summary;")
    op.execute("DROP TABLE IF EXISTS da_bid;")
