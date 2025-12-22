"""Add entsoe_balancing_energy table

Revision ID: 006
Revises: 005
Create Date: 2024-12-21 00:05:00.000000

This migration creates the entsoe_balancing_energy table for storing
activated balancing energy from ENTSO-E (A84 document type).

Captures TSO intervention volumes for system balance:
- afrr_up_mw: Automatic Frequency Restoration Reserve (upward activation)
- afrr_down_mw: Automatic Frequency Restoration Reserve (downward activation)
- mfrr_up_mw: Manual Frequency Restoration Reserve (upward activation)
- mfrr_down_mw: Manual Frequency Restoration Reserve (downward activation)

BusinessTypes in A84:
- A95: aFRR (Automatic Frequency Restoration Reserve)
- A96: mFRR (Manual Frequency Restoration Reserve)
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '006'
down_revision: Union[str, None] = '005'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create entsoe_balancing_energy table."""
    op.create_table(
        'entsoe_balancing_energy',
        sa.Column('id', sa.Integer, autoincrement=True, nullable=False),
        sa.Column('trade_date', sa.Date, nullable=False),
        sa.Column('period', sa.Integer, nullable=False),
        sa.Column('time_interval', sa.String(11), nullable=False),
        # aFRR columns (Automatic Frequency Restoration Reserve)
        sa.Column('afrr_up_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('afrr_down_mw', sa.Numeric(12, 3), nullable=True),
        # mFRR columns (Manual Frequency Restoration Reserve)
        sa.Column('mfrr_up_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('mfrr_down_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('id', name='entsoe_balancing_energy_pkey'),
        sa.UniqueConstraint('trade_date', 'period', name='entsoe_balancing_energy_trade_date_period_key'),
        sa.UniqueConstraint('trade_date', 'time_interval', name='entsoe_balancing_energy_trade_date_time_interval_key'),
        schema='finance'
    )

    # Create index for date range queries
    op.create_index(
        'idx_entsoe_balancing_energy_trade_date',
        'entsoe_balancing_energy',
        ['trade_date'],
        schema='finance'
    )


def downgrade() -> None:
    """Drop entsoe_balancing_energy table."""
    op.drop_index('idx_entsoe_balancing_energy_trade_date', table_name='entsoe_balancing_energy', schema='finance')
    op.drop_table('entsoe_balancing_energy', schema='finance')
