"""Add entsoe_cross_border_flows table (wide format)

Revision ID: 004
Revises: 003
Create Date: 2024-12-21 00:03:00.000000

This migration creates the entsoe_cross_border_flows table for storing
cross-border physical flows data from ENTSO-E (A11 document type).

Wide-format schema with border flow columns:
- flow_de_mw: Physical flow to/from Germany (positive = import, negative = export)
- flow_at_mw: Physical flow to/from Austria
- flow_pl_mw: Physical flow to/from Poland
- flow_sk_mw: Physical flow to/from Slovakia
- flow_total_net_mw: Sum of all border flows
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '004'
down_revision: Union[str, None] = '003'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create entsoe_cross_border_flows table with wide-format schema."""
    op.create_table(
        'entsoe_cross_border_flows',
        sa.Column('id', sa.Integer, autoincrement=True, nullable=False),
        sa.Column('delivery_datetime', sa.DateTime, nullable=False),
        sa.Column('area_id', sa.String(20), nullable=False),
        # Wide-format border flow columns (positive = import, negative = export)
        sa.Column('flow_de_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('flow_at_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('flow_pl_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('flow_sk_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('flow_total_net_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('id', name='entsoe_cross_border_flows_pkey'),
        sa.UniqueConstraint('delivery_datetime', 'area_id', name='entsoe_cross_border_flows_datetime_area_key'),
        schema='finance'
    )

    # Create index for common queries
    op.create_index(
        'idx_entsoe_cross_border_flows_delivery_datetime',
        'entsoe_cross_border_flows',
        ['delivery_datetime'],
        schema='finance'
    )


def downgrade() -> None:
    """Drop entsoe_cross_border_flows table."""
    op.drop_index('idx_entsoe_cross_border_flows_delivery_datetime', table_name='entsoe_cross_border_flows', schema='finance')
    op.drop_table('entsoe_cross_border_flows', schema='finance')
