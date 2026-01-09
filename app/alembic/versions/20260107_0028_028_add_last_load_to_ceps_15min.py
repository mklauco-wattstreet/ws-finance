"""Add last_load_at_interval_mw column to ceps_actual_imbalance_15min.

Revision ID: 028
Revises: 027
Create Date: 2026-01-07

Adds a new column to store the last (most recent) load value within each 15-minute interval.
This provides the ending value of the interval alongside mean and median aggregations.
"""

from alembic import op
import sqlalchemy as sa

revision = '028'
down_revision = '027'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add last_load_at_interval_mw column to ceps_actual_imbalance_15min."""

    op.execute("""
        ALTER TABLE finance.ceps_actual_imbalance_15min
        ADD COLUMN last_load_at_interval_mw NUMERIC(12,5);
    """)


def downgrade() -> None:
    """Remove last_load_at_interval_mw column from ceps_actual_imbalance_15min."""

    op.execute("""
        ALTER TABLE finance.ceps_actual_imbalance_15min
        DROP COLUMN last_load_at_interval_mw;
    """)
