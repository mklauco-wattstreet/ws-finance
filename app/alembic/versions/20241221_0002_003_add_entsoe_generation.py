"""Add entsoe_generation_actual table (wide format)

Revision ID: 003
Revises: 002
Create Date: 2024-12-21 00:02:00.000000

This migration creates the entsoe_generation_actual table for storing
actual generation per type data from ENTSO-E (A75 document type).

Wide-format schema with aggregated fuel type columns:
- gen_nuclear_mw: B14 (Nuclear)
- gen_coal_mw: B02 (Brown coal/Lignite) + B05 (Hard coal)
- gen_gas_mw: B04 (Fossil Gas)
- gen_solar_mw: B16 (Solar)
- gen_wind_mw: B19 (Wind Onshore)
- gen_hydro_pumped_mw: B10 (Hydro Pumped Storage)
- gen_biomass_mw: B01 (Biomass)
- gen_hydro_other_mw: B11 (Run-of-river) + B12 (Water Reservoir)
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '003'
down_revision: Union[str, None] = '002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create entsoe_generation_actual table with wide-format schema."""
    op.create_table(
        'entsoe_generation_actual',
        sa.Column('id', sa.Integer, autoincrement=True, nullable=False),
        sa.Column('trade_date', sa.Date, nullable=False),
        sa.Column('period', sa.Integer, nullable=False),
        sa.Column('time_interval', sa.String(11), nullable=False),
        # Wide-format fuel type columns (aggregated PSR types)
        sa.Column('gen_nuclear_mw', sa.Numeric(12, 3), nullable=True),        # B14
        sa.Column('gen_coal_mw', sa.Numeric(12, 3), nullable=True),           # B02 + B05
        sa.Column('gen_gas_mw', sa.Numeric(12, 3), nullable=True),            # B04
        sa.Column('gen_solar_mw', sa.Numeric(12, 3), nullable=True),          # B16
        sa.Column('gen_wind_mw', sa.Numeric(12, 3), nullable=True),           # B19
        sa.Column('gen_hydro_pumped_mw', sa.Numeric(12, 3), nullable=True),   # B10
        sa.Column('gen_biomass_mw', sa.Numeric(12, 3), nullable=True),        # B01
        sa.Column('gen_hydro_other_mw', sa.Numeric(12, 3), nullable=True),    # B11 + B12
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('id', name='entsoe_generation_actual_pkey'),
        sa.UniqueConstraint('trade_date', 'period', name='entsoe_generation_actual_trade_date_period_key'),
        sa.UniqueConstraint('trade_date', 'time_interval', name='entsoe_generation_actual_trade_date_time_interval_key'),
        schema='finance'
    )

    # Create index for common queries
    op.create_index(
        'idx_entsoe_generation_actual_trade_date',
        'entsoe_generation_actual',
        ['trade_date'],
        schema='finance'
    )


def downgrade() -> None:
    """Drop entsoe_generation_actual table."""
    op.drop_index('idx_entsoe_generation_actual_trade_date', table_name='entsoe_generation_actual', schema='finance')
    op.drop_table('entsoe_generation_actual', schema='finance')
