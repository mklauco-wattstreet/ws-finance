"""Add HU partition and currency column to entsoe_imbalance_prices.

Revision ID: 037
Revises: 036
Create Date: 2026-02-12

Changes:
- Rename 8 price columns from *_czk_mwh to *_mwh (currency-agnostic)
- Add currency VARCHAR(3) column
- Set existing CZ data to CZK
- Create HU partition
"""

from alembic import op
import sqlalchemy as sa

revision = '037'
down_revision = '036'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: Rename columns (removes CZK suffix for currency-agnostic naming)
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_price_czk_mwh TO pos_imb_price_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_scarcity_czk_mwh TO pos_imb_scarcity_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_incentive_czk_mwh TO pos_imb_incentive_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_financial_neutrality_czk_mwh TO pos_imb_financial_neutrality_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_price_czk_mwh TO neg_imb_price_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_scarcity_czk_mwh TO neg_imb_scarcity_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_incentive_czk_mwh TO neg_imb_incentive_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_financial_neutrality_czk_mwh TO neg_imb_financial_neutrality_mwh;")

    # Step 2: Add currency column with default EUR
    op.execute("ALTER TABLE entsoe_imbalance_prices ADD COLUMN currency VARCHAR(3) NOT NULL DEFAULT 'EUR';")

    # Step 3: Update existing CZ data to use CZK
    op.execute("UPDATE entsoe_imbalance_prices SET currency = 'CZK' WHERE country_code = 'CZ';")

    # Step 4: Create HU partition
    op.execute("""
        CREATE TABLE entsoe_imbalance_prices_hu
        PARTITION OF entsoe_imbalance_prices
        FOR VALUES IN ('HU');
    """)


def downgrade() -> None:
    # Step 1: Drop HU partition
    op.execute("DROP TABLE IF EXISTS entsoe_imbalance_prices_hu;")

    # Step 2: Drop currency column
    op.execute("ALTER TABLE entsoe_imbalance_prices DROP COLUMN currency;")

    # Step 3: Rename columns back to CZK suffix
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_price_mwh TO pos_imb_price_czk_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_scarcity_mwh TO pos_imb_scarcity_czk_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_incentive_mwh TO pos_imb_incentive_czk_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_financial_neutrality_mwh TO pos_imb_financial_neutrality_czk_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_price_mwh TO neg_imb_price_czk_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_scarcity_mwh TO neg_imb_scarcity_czk_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_incentive_mwh TO neg_imb_incentive_czk_mwh;")
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_financial_neutrality_mwh TO neg_imb_financial_neutrality_czk_mwh;")
