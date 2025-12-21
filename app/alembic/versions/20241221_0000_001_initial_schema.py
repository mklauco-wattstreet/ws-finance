"""Initial schema - existing tables

Revision ID: 001
Revises:
Create Date: 2024-12-21 00:00:00.000000

This migration represents the initial state of the database.
Tables already exist, so this is just a marker migration.
Run: alembic stamp 001
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Tables already exist in the database.

    This migration is a marker for the initial schema state.
    The following tables exist in the 'finance' schema:
    - entsoe_imbalance_prices
    - ote_daily_payments
    - ote_prices_day_ahead
    - ote_prices_imbalance
    - ote_prices_intraday_market
    - ote_trade_balance
    """
    pass


def downgrade() -> None:
    """Cannot downgrade initial schema."""
    pass
