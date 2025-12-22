"""Add entsoe_scheduled_cross_border_flows table

Revision ID: 008
Revises: 007
Create Date: 2024-12-21 00:07:00.000000

This migration creates the entsoe_scheduled_cross_border_flows table for storing
scheduled commercial exchanges from ENTSO-E (A09 document type).

Captures day-ahead scheduled cross-border flows for CZ borders:
- scheduled_de_mw: Scheduled exchange with Germany (positive = import)
- scheduled_at_mw: Scheduled exchange with Austria
- scheduled_pl_mw: Scheduled exchange with Poland
- scheduled_sk_mw: Scheduled exchange with Slovakia
- scheduled_total_net_mw: Sum of all scheduled exchanges

Compare with entsoe_cross_border_flows (physical A11) to calculate schedule deviation.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '008'
down_revision: Union[str, None] = '007'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create entsoe_scheduled_cross_border_flows table."""
    op.create_table(
        'entsoe_scheduled_cross_border_flows',
        sa.Column('id', sa.Integer, autoincrement=True, nullable=False),
        sa.Column('trade_date', sa.Date, nullable=False),
        sa.Column('period', sa.Integer, nullable=False),
        sa.Column('time_interval', sa.String(11), nullable=False),
        # Scheduled cross-border flow columns (positive = import, negative = export)
        sa.Column('scheduled_de_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('scheduled_at_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('scheduled_pl_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('scheduled_sk_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('scheduled_total_net_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('id', name='entsoe_sched_xborder_flows_pkey'),
        sa.UniqueConstraint('trade_date', 'period', name='entsoe_sched_xborder_flows_date_period_key'),
        sa.UniqueConstraint('trade_date', 'time_interval', name='entsoe_sched_xborder_flows_date_interval_key'),
        schema='finance'
    )

    # Create index for date range queries
    op.create_index(
        'idx_entsoe_scheduled_cross_border_flows_trade_date',
        'entsoe_scheduled_cross_border_flows',
        ['trade_date'],
        schema='finance'
    )


def downgrade() -> None:
    """Drop entsoe_scheduled_cross_border_flows table."""
    op.drop_index('idx_entsoe_scheduled_cross_border_flows_trade_date', table_name='entsoe_scheduled_cross_border_flows', schema='finance')
    op.drop_table('entsoe_scheduled_cross_border_flows', schema='finance')
