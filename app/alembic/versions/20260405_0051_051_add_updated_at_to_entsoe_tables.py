"""Add updated_at column to all ENTSO-E tables and cnb_exchange_rate.

Revision ID: 051
Revises: 050
Create Date: 2026-04-05

Two-step DDL per table:
  1. ADD COLUMN with no default -> existing rows get NULL (= never updated)
  2. SET DEFAULT CURRENT_TIMESTAMP -> new inserts get current time
"""

from alembic import op

revision = '051'
down_revision = '050'
branch_labels = None
depends_on = None

TABLES = [
    'entsoe_areas',
    'entsoe_balancing_energy',
    'entsoe_cross_border_flows',
    'entsoe_day_ahead_prices',
    'entsoe_generation_actual',
    'entsoe_generation_forecast',
    'entsoe_generation_forecast_current',
    'entsoe_generation_forecast_intraday',
    'entsoe_generation_scheduled',
    'entsoe_imbalance_prices',
    'entsoe_load',
    'entsoe_scheduled_cross_border_flows',
    'cnb_exchange_rate',
]


def upgrade() -> None:
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;")
        op.execute(f"ALTER TABLE {table} ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;")


def downgrade() -> None:
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS updated_at;")
