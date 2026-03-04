"""Add da_curve_depth table for price-at-offset metrics.

Revision ID: 039
Revises: 038
Create Date: 2026-03-04

Changes:
- Create da_curve_depth table for price at +N MW depth analytics
"""

from alembic import op
import sqlalchemy as sa

revision = '039'
down_revision = '038'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE da_curve_depth (
            delivery_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            side VARCHAR(4) NOT NULL,
            offset_mw INTEGER NOT NULL,
            price_at_offset NUMERIC(10,2),
            volume_available NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT da_curve_depth_pkey PRIMARY KEY (delivery_date, period, side, offset_mw)
        );
    """)
    op.execute("CREATE INDEX idx_da_curve_depth_date ON da_curve_depth (delivery_date);")
    op.execute("CREATE INDEX idx_da_curve_depth_date_period ON da_curve_depth (delivery_date, period);")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS da_curve_depth;")
