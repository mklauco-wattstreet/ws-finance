"""Add weather_forecast and weather_current tables.

Revision ID: 053
Revises: 052
Create Date: 2026-04-10

Open-Meteo weather data for CZ electricity price prediction.
Uses TIMESTAMPTZ for created_at, updated_at, and forecast_made_at.
"""

import sqlalchemy as sa
from alembic import op

revision = '053'
down_revision = '052'
branch_labels = None
depends_on = None

SCHEMA = 'finance'


def upgrade() -> None:
    op.create_table(
        'weather_forecast',
        sa.Column('trade_date', sa.Date, nullable=False),
        sa.Column('time_interval', sa.String(11), nullable=False),
        sa.Column('forecast_made_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('temperature_2m', sa.Numeric(6, 2)),
        sa.Column('shortwave_radiation', sa.Numeric(8, 2)),
        sa.Column('direct_radiation', sa.Numeric(8, 2)),
        sa.Column('cloud_cover', sa.Numeric(5, 2)),
        sa.Column('wind_speed_10m', sa.Numeric(6, 2)),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('trade_date', 'time_interval', 'forecast_made_at'),
        schema=SCHEMA,
    )
    op.create_index(
        'idx_weather_forecast_trade_date',
        'weather_forecast',
        ['trade_date'],
        schema=SCHEMA,
    )

    op.create_table(
        'weather_current',
        sa.Column('trade_date', sa.Date, nullable=False),
        sa.Column('time_interval', sa.String(11), nullable=False),
        sa.Column('temperature_2m', sa.Numeric(6, 2)),
        sa.Column('shortwave_radiation', sa.Numeric(8, 2)),
        sa.Column('direct_radiation', sa.Numeric(8, 2)),
        sa.Column('cloud_cover', sa.Numeric(5, 2)),
        sa.Column('wind_speed_10m', sa.Numeric(6, 2)),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('trade_date', 'time_interval'),
        schema=SCHEMA,
    )
    op.create_index(
        'idx_weather_current_trade_date',
        'weather_current',
        ['trade_date'],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index('idx_weather_current_trade_date', table_name='weather_current', schema=SCHEMA)
    op.drop_table('weather_current', schema=SCHEMA)
    op.drop_index('idx_weather_forecast_trade_date', table_name='weather_forecast', schema=SCHEMA)
    op.drop_table('weather_forecast', schema=SCHEMA)
