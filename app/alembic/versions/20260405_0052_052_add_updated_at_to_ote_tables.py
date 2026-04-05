"""Add updated_at column to all OTE price tables.

Revision ID: 052
Revises: 051
Create Date: 2026-04-05

Two-step DDL per table:
  1. ADD COLUMN with no default -> existing rows get NULL (= never updated)
  2. SET DEFAULT CURRENT_TIMESTAMP -> new inserts get current time
"""

from alembic import op

revision = '052'
down_revision = '051'
branch_labels = None
depends_on = None

TABLES = [
    'ote_prices_day_ahead',
    'ote_prices_day_ahead_60min',
    'ote_prices_ida',
    'ote_prices_imbalance',
    'ote_prices_intraday_market',
]


def upgrade() -> None:
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;")
        op.execute(f"ALTER TABLE {table} ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;")


def downgrade() -> None:
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS updated_at;")
