"""Rename weather columns to include units.

Revision ID: 054
Revises: 053
Create Date: 2026-04-10

temperature_2m → temperature_2m_degc
shortwave_radiation → shortwave_radiation_wm2
direct_radiation → direct_radiation_wm2
cloud_cover → cloud_cover_pct
wind_speed_10m → wind_speed_10m_kmh
"""

from alembic import op

revision = '054'
down_revision = '053'
branch_labels = None
depends_on = None

SCHEMA = 'finance'

TABLES = ['weather_forecast', 'weather_current']

RENAMES = [
    ('temperature_2m', 'temperature_2m_degc'),
    ('shortwave_radiation', 'shortwave_radiation_wm2'),
    ('direct_radiation', 'direct_radiation_wm2'),
    ('cloud_cover', 'cloud_cover_pct'),
    ('wind_speed_10m', 'wind_speed_10m_kmh'),
]


def upgrade() -> None:
    for table in TABLES:
        for old, new in RENAMES:
            op.alter_column(table, old, new_column_name=new, schema=SCHEMA)


def downgrade() -> None:
    for table in TABLES:
        for old, new in RENAMES:
            op.alter_column(table, new, new_column_name=old, schema=SCHEMA)
