"""allow nulls in intraday market prices for incomplete data

Revision ID: 026_allow_nulls_intraday
Revises: 025_partition_entsoe_scheduled_cross_border_flows
Create Date: 2026-01-03 19:35:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '026'
down_revision = '025'
branch_labels = None
depends_on = None


def upgrade():
    """
    Allow NULL values in intraday market price/volume columns.
    This is necessary because intraday data is updated continuously throughout the day,
    and periods that haven't occurred yet or have no trading activity will have NULL values.
    """
    # Alter columns to allow NULL
    op.alter_column('ote_prices_intraday_market', 'traded_volume_mwh',
                    existing_type=sa.NUMERIC(precision=12, scale=4),
                    nullable=True,
                    schema='finance')

    op.alter_column('ote_prices_intraday_market', 'traded_volume_purchased_mwh',
                    existing_type=sa.NUMERIC(precision=12, scale=4),
                    nullable=True,
                    schema='finance')

    op.alter_column('ote_prices_intraday_market', 'traded_volume_sold_mwh',
                    existing_type=sa.NUMERIC(precision=12, scale=4),
                    nullable=True,
                    schema='finance')

    op.alter_column('ote_prices_intraday_market', 'weighted_avg_price_eur_mwh',
                    existing_type=sa.NUMERIC(precision=15, scale=3),
                    nullable=True,
                    schema='finance')

    op.alter_column('ote_prices_intraday_market', 'min_price_eur_mwh',
                    existing_type=sa.NUMERIC(precision=15, scale=3),
                    nullable=True,
                    schema='finance')

    op.alter_column('ote_prices_intraday_market', 'max_price_eur_mwh',
                    existing_type=sa.NUMERIC(precision=15, scale=3),
                    nullable=True,
                    schema='finance')

    op.alter_column('ote_prices_intraday_market', 'last_price_eur_mwh',
                    existing_type=sa.NUMERIC(precision=15, scale=3),
                    nullable=True,
                    schema='finance')


def downgrade():
    """
    Revert to NOT NULL constraints.
    WARNING: This will fail if any NULL values exist in the table.
    """
    op.alter_column('ote_prices_intraday_market', 'last_price_eur_mwh',
                    existing_type=sa.NUMERIC(precision=15, scale=3),
                    nullable=False,
                    schema='finance')

    op.alter_column('ote_prices_intraday_market', 'max_price_eur_mwh',
                    existing_type=sa.NUMERIC(precision=15, scale=3),
                    nullable=False,
                    schema='finance')

    op.alter_column('ote_prices_intraday_market', 'min_price_eur_mwh',
                    existing_type=sa.NUMERIC(precision=15, scale=3),
                    nullable=False,
                    schema='finance')

    op.alter_column('ote_prices_intraday_market', 'weighted_avg_price_eur_mwh',
                    existing_type=sa.NUMERIC(precision=15, scale=3),
                    nullable=False,
                    schema='finance')

    op.alter_column('ote_prices_intraday_market', 'traded_volume_sold_mwh',
                    existing_type=sa.NUMERIC(precision=12, scale=4),
                    nullable=False,
                    schema='finance')

    op.alter_column('ote_prices_intraday_market', 'traded_volume_purchased_mwh',
                    existing_type=sa.NUMERIC(precision=12, scale=4),
                    nullable=False,
                    schema='finance')

    op.alter_column('ote_prices_intraday_market', 'traded_volume_mwh',
                    existing_type=sa.NUMERIC(precision=12, scale=4),
                    nullable=False,
                    schema='finance')
