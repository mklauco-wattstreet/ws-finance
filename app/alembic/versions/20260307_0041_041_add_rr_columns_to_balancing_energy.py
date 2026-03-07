"""Add Replacement Reserve (A97) columns to entsoe_balancing_energy.

Revision ID: 041
Revises: 040
Create Date: 2026-03-07

Adds rr_up_price_eur and rr_down_price_eur columns for Replacement Reserve
activation prices (businessType A97) which are already present in the A84 XML
response but were not being stored.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = '041'
down_revision = '040'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE entsoe_balancing_energy ADD COLUMN rr_up_price_eur NUMERIC(15,3);")
    op.execute("ALTER TABLE entsoe_balancing_energy ADD COLUMN rr_down_price_eur NUMERIC(15,3);")


def downgrade() -> None:
    op.execute("ALTER TABLE entsoe_balancing_energy DROP COLUMN IF EXISTS rr_down_price_eur;")
    op.execute("ALTER TABLE entsoe_balancing_energy DROP COLUMN IF EXISTS rr_up_price_eur;")
