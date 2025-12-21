"""Add entsoe_load table

Revision ID: 002
Revises: 001
Create Date: 2024-12-21 00:01:00.000000

This migration creates the entsoe_load table for storing
actual and forecast load data from ENTSO-E (A65 document type).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '002'
down_revision: Union[str, None] = '001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create entsoe_load table."""
    op.create_table(
        'entsoe_load',
        sa.Column('id', sa.Integer, autoincrement=True, nullable=False),
        sa.Column('trade_date', sa.Date, nullable=False),
        sa.Column('period', sa.Integer, nullable=False),
        sa.Column('time_interval', sa.String(11), nullable=False),
        sa.Column('actual_load_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('forecast_load_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('id', name='entsoe_load_pkey'),
        sa.UniqueConstraint('trade_date', 'period', name='entsoe_load_trade_date_period_key'),
        sa.UniqueConstraint('trade_date', 'time_interval', name='entsoe_load_trade_date_time_interval_key'),
        schema='finance'
    )

    # Create index for common queries
    op.create_index(
        'idx_entsoe_load_trade_date',
        'entsoe_load',
        ['trade_date'],
        schema='finance'
    )


def downgrade() -> None:
    """Drop entsoe_load table."""
    op.drop_index('idx_entsoe_load_trade_date', table_name='entsoe_load', schema='finance')
    op.drop_table('entsoe_load', schema='finance')
