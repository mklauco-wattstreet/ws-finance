"""Add entsoe_generation_forecast table

Revision ID: 005
Revises: 004
Create Date: 2024-12-21 00:04:00.000000

This migration creates the entsoe_generation_forecast table for storing
day-ahead generation forecasts from ENTSO-E (A69 document type).

Captures renewable forecast data for calculating forecast errors:
- forecast_solar_mw: B16 (Solar) day-ahead forecast
- forecast_wind_mw: B19 (Wind Onshore) day-ahead forecast
- forecast_wind_offshore_mw: B18 (Wind Offshore) day-ahead forecast
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '005'
down_revision: Union[str, None] = '004'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create entsoe_generation_forecast table."""
    op.create_table(
        'entsoe_generation_forecast',
        sa.Column('id', sa.Integer, autoincrement=True, nullable=False),
        sa.Column('trade_date', sa.Date, nullable=False),
        sa.Column('period', sa.Integer, nullable=False),
        sa.Column('time_interval', sa.String(11), nullable=False),
        # Renewable forecast columns (MW)
        sa.Column('forecast_solar_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('forecast_wind_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('forecast_wind_offshore_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('id', name='entsoe_generation_forecast_pkey'),
        sa.UniqueConstraint('trade_date', 'period', name='entsoe_generation_forecast_trade_date_period_key'),
        sa.UniqueConstraint('trade_date', 'time_interval', name='entsoe_generation_forecast_trade_date_time_interval_key'),
        schema='finance'
    )

    # Create index for date range queries
    op.create_index(
        'idx_entsoe_generation_forecast_trade_date',
        'entsoe_generation_forecast',
        ['trade_date'],
        schema='finance'
    )


def downgrade() -> None:
    """Drop entsoe_generation_forecast table."""
    op.drop_index('idx_entsoe_generation_forecast_trade_date', table_name='entsoe_generation_forecast', schema='finance')
    op.drop_table('entsoe_generation_forecast', schema='finance')
