"""Add estimated_price_czk_mwh_60min column to ceps_estimated_imbalance_price_15min.

Revision ID: 048
Revises: 047
Create Date: 2026-03-22

Adds a computed 60-minute average column. The value is the average of 4
fifteen-minute intervals within the same hour (xx:00-xx:15, xx:15-xx:30,
xx:30-xx:45, xx:45-xx+1:00). Calculated on the fly during upsert.
"""

from alembic import op

revision = '048'
down_revision = '047'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE finance.ceps_estimated_imbalance_price_15min
        ADD COLUMN estimated_price_czk_mwh_60min NUMERIC(12,3);
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE finance.ceps_estimated_imbalance_price_15min
        DROP COLUMN IF EXISTS estimated_price_czk_mwh_60min;
    """)
