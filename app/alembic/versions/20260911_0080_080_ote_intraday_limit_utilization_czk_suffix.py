"""Suffix the monetary columns of ote_intraday_limit_utilization with _czk.

The six amount columns are all in one currency (they add up: commodity +
imbalance = total change, running sum = utilization, limit - utilization =
available). The portal export carries no unit label; OTE's financial
security is denominated in CZK, so the unit goes into the column names.

Renames only - no data change. Column comments follow the rename.

Revision ID: 080
Revises: 079
Create Date: 2026-09-11
"""

from alembic import op


revision = '080'
down_revision = '079'
branch_labels = None
depends_on = None

TABLE = 'ote_intraday_limit_utilization'
RENAMES = (
    ('utilization_change_commodity', 'utilization_change_commodity_czk'),
    ('utilization_change_imbalance', 'utilization_change_imbalance_czk'),
    ('utilization_change_total', 'utilization_change_total_czk'),
    ('utilization_total', 'utilization_total_czk'),
    ('limit_total', 'limit_total_czk'),
    ('limit_available', 'limit_available_czk'),
)


def upgrade() -> None:
    for old, new in RENAMES:
        op.execute(f"ALTER TABLE {TABLE} RENAME COLUMN {old} TO {new};")


def downgrade() -> None:
    for old, new in RENAMES:
        op.execute(f"ALTER TABLE {TABLE} RENAME COLUMN {new} TO {old};")
