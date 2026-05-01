"""Rename da_curve_depth direction columns to supply/demand terminology.

Revision ID: 056
Revises: 055
Create Date: 2026-05-01

sell_up_*   -> supply_*           (sell side above clearing, unmatched)
sell_down_* -> supply_matched_*   (sell side below clearing, matched)
buy_down_*  -> demand_*           (buy side below clearing, unmatched)
buy_up_*    -> demand_matched_*   (buy side above clearing, matched)
"""

from alembic import op

revision = '056'
down_revision = '055'
branch_labels = None
depends_on = None

SCHEMA = 'finance'
TABLE = 'da_curve_depth'

SUFFIXES = ('mw_from_clearing', 'price_from_clearing', 'slope')

RENAMES = [
    ('sell_up',   'supply'),
    ('sell_down', 'supply_matched'),
    ('buy_down',  'demand'),
    ('buy_up',    'demand_matched'),
]


def upgrade() -> None:
    for old_prefix, new_prefix in RENAMES:
        for suffix in SUFFIXES:
            op.alter_column(
                TABLE,
                f'{old_prefix}_{suffix}',
                new_column_name=f'{new_prefix}_{suffix}',
                schema=SCHEMA,
            )


def downgrade() -> None:
    for old_prefix, new_prefix in RENAMES:
        for suffix in SUFFIXES:
            op.alter_column(
                TABLE,
                f'{new_prefix}_{suffix}',
                new_column_name=f'{old_prefix}_{suffix}',
                schema=SCHEMA,
            )
