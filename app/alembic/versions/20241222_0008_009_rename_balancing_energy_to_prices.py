"""Rename entsoe_balancing_energy columns from MW to prices

Revision ID: 009
Revises: 008
Create Date: 2024-12-22 10:00:00.000000

ENTSO-E A84 for Czech Republic returns activation PRICES (EUR/MWh),
not MW volumes. Renaming columns to reflect actual data content.
"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '009'
down_revision: Union[str, None] = '008'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Rename columns from _mw to _price_eur."""
    op.alter_column(
        'entsoe_balancing_energy',
        'afrr_up_mw',
        new_column_name='afrr_up_price_eur',
        schema='finance'
    )
    op.alter_column(
        'entsoe_balancing_energy',
        'afrr_down_mw',
        new_column_name='afrr_down_price_eur',
        schema='finance'
    )
    op.alter_column(
        'entsoe_balancing_energy',
        'mfrr_up_mw',
        new_column_name='mfrr_up_price_eur',
        schema='finance'
    )
    op.alter_column(
        'entsoe_balancing_energy',
        'mfrr_down_mw',
        new_column_name='mfrr_down_price_eur',
        schema='finance'
    )


def downgrade() -> None:
    """Rename columns back from _price_eur to _mw."""
    op.alter_column(
        'entsoe_balancing_energy',
        'afrr_up_price_eur',
        new_column_name='afrr_up_mw',
        schema='finance'
    )
    op.alter_column(
        'entsoe_balancing_energy',
        'afrr_down_price_eur',
        new_column_name='afrr_down_mw',
        schema='finance'
    )
    op.alter_column(
        'entsoe_balancing_energy',
        'mfrr_up_price_eur',
        new_column_name='mfrr_up_mw',
        schema='finance'
    )
    op.alter_column(
        'entsoe_balancing_energy',
        'mfrr_down_price_eur',
        new_column_name='mfrr_down_mw',
        schema='finance'
    )
