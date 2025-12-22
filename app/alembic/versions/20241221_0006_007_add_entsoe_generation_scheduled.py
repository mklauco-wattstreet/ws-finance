"""Add entsoe_generation_scheduled table

Revision ID: 007
Revises: 006
Create Date: 2024-12-21 00:06:00.000000

This migration creates the entsoe_generation_scheduled table for storing
scheduled generation from ENTSO-E (A71 document type).

Captures day-ahead scheduled generation for comparing with actual:
- scheduled_total_mw: Total scheduled generation
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '007'
down_revision: Union[str, None] = '006'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create entsoe_generation_scheduled table."""
    op.create_table(
        'entsoe_generation_scheduled',
        sa.Column('id', sa.Integer, autoincrement=True, nullable=False),
        sa.Column('trade_date', sa.Date, nullable=False),
        sa.Column('period', sa.Integer, nullable=False),
        sa.Column('time_interval', sa.String(11), nullable=False),
        # Scheduled generation (MW)
        sa.Column('scheduled_total_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('id', name='entsoe_generation_scheduled_pkey'),
        sa.UniqueConstraint('trade_date', 'period', name='entsoe_generation_scheduled_trade_date_period_key'),
        sa.UniqueConstraint('trade_date', 'time_interval', name='entsoe_generation_scheduled_trade_date_time_interval_key'),
        schema='finance'
    )

    # Create index for date range queries
    op.create_index(
        'idx_entsoe_generation_scheduled_trade_date',
        'entsoe_generation_scheduled',
        ['trade_date'],
        schema='finance'
    )


def downgrade() -> None:
    """Drop entsoe_generation_scheduled table."""
    op.drop_index('idx_entsoe_generation_scheduled_trade_date', table_name='entsoe_generation_scheduled', schema='finance')
    op.drop_table('entsoe_generation_scheduled', schema='finance')
