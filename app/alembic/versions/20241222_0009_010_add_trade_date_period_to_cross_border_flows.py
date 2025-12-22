"""Add trade_date/period columns to entsoe_cross_border_flows

Revision ID: 010
Revises: 009
Create Date: 2024-12-22 12:00:00.000000

Aligns entsoe_cross_border_flows with the standard trade_date/period pattern
used by all other tables. This enables consistent ML feature engineering.

New columns:
- trade_date: Date (extracted from delivery_datetime in Europe/Prague TZ)
- period: Integer (1-96, calculated as hour*4 + minute//15 + 1)
- time_interval: String (HH:MM-HH:MM format)

Data is migrated from existing delivery_datetime values.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '010'
down_revision: Union[str, None] = '009'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add trade_date, period, time_interval columns and migrate data."""
    # Step 1: Add columns as nullable
    op.add_column(
        'entsoe_cross_border_flows',
        sa.Column('trade_date', sa.Date, nullable=True),
        schema='finance'
    )
    op.add_column(
        'entsoe_cross_border_flows',
        sa.Column('period', sa.Integer, nullable=True),
        schema='finance'
    )
    op.add_column(
        'entsoe_cross_border_flows',
        sa.Column('time_interval', sa.String(11), nullable=True),
        schema='finance'
    )

    # Step 2: Populate from delivery_datetime using Europe/Prague timezone
    # delivery_datetime is stored as naive datetime in Prague local time
    op.execute("""
        UPDATE finance.entsoe_cross_border_flows
        SET
            trade_date = DATE(delivery_datetime),
            period = (EXTRACT(HOUR FROM delivery_datetime)::int * 4)
                   + (EXTRACT(MINUTE FROM delivery_datetime)::int / 15) + 1,
            time_interval = TO_CHAR(delivery_datetime, 'HH24:MI') || '-'
                          || TO_CHAR(delivery_datetime + INTERVAL '15 minutes', 'HH24:MI')
        WHERE trade_date IS NULL
    """)

    # Step 3: Make columns NOT NULL
    op.alter_column(
        'entsoe_cross_border_flows',
        'trade_date',
        nullable=False,
        schema='finance'
    )
    op.alter_column(
        'entsoe_cross_border_flows',
        'period',
        nullable=False,
        schema='finance'
    )
    op.alter_column(
        'entsoe_cross_border_flows',
        'time_interval',
        nullable=False,
        schema='finance'
    )

    # Step 4: Add unique constraint for trade_date/period/area_id
    op.create_unique_constraint(
        'entsoe_cross_border_flows_trade_date_period_area_key',
        'entsoe_cross_border_flows',
        ['trade_date', 'period', 'area_id'],
        schema='finance'
    )

    # Step 5: Create index on trade_date for efficient queries
    op.create_index(
        'idx_entsoe_cross_border_flows_trade_date',
        'entsoe_cross_border_flows',
        ['trade_date'],
        schema='finance'
    )


def downgrade() -> None:
    """Remove trade_date, period, time_interval columns."""
    op.drop_index(
        'idx_entsoe_cross_border_flows_trade_date',
        table_name='entsoe_cross_border_flows',
        schema='finance'
    )
    op.drop_constraint(
        'entsoe_cross_border_flows_trade_date_period_area_key',
        'entsoe_cross_border_flows',
        schema='finance'
    )
    op.drop_column('entsoe_cross_border_flows', 'time_interval', schema='finance')
    op.drop_column('entsoe_cross_border_flows', 'period', schema='finance')
    op.drop_column('entsoe_cross_border_flows', 'trade_date', schema='finance')
