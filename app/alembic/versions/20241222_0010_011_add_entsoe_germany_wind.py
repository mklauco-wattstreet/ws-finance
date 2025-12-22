"""Add entsoe_germany_wind table for German wind generation data

Revision ID: 011
Revises: 010
Create Date: 2024-12-22 12:30:00.000000

Creates table for German TenneT (10YDE-EON------1) wind generation data.
This serves as a leading indicator for Czech balancing costs due to
cross-border flow impacts when German wind fluctuates.

Data source: ENTSO-E A75 (Generation per Type)
PSR types: B18 (Wind Offshore), B19 (Wind Onshore)
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '011'
down_revision: Union[str, None] = '010'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create entsoe_germany_wind table."""
    op.create_table(
        'entsoe_germany_wind',
        sa.Column('id', sa.Integer, autoincrement=True, nullable=False),
        sa.Column('trade_date', sa.Date, nullable=False),
        sa.Column('period', sa.Integer, nullable=False),
        sa.Column('time_interval', sa.String(11), nullable=False),
        sa.Column('wind_onshore_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('wind_offshore_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('wind_total_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('id', name='entsoe_germany_wind_pkey'),
        sa.UniqueConstraint('trade_date', 'period', name='entsoe_germany_wind_trade_date_period_key'),
        schema='finance'
    )

    # Create index for common queries
    op.create_index(
        'idx_entsoe_germany_wind_trade_date',
        'entsoe_germany_wind',
        ['trade_date'],
        schema='finance'
    )


def downgrade() -> None:
    """Drop entsoe_germany_wind table."""
    op.drop_index(
        'idx_entsoe_germany_wind_trade_date',
        table_name='entsoe_germany_wind',
        schema='finance'
    )
    op.drop_table('entsoe_germany_wind', schema='finance')
