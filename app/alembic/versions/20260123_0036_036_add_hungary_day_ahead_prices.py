"""Add Hungary to entsoe_areas and create entsoe_day_ahead_prices table.

Revision ID: 036
Revises: 035
Create Date: 2026-01-23

Adds:
1. Hungary (HU) to entsoe_areas lookup table
2. New entsoe_day_ahead_prices table partitioned by country_code
3. Initial partition for Hungary (HU)

ENTSO-E Document Type: A44 (Day-ahead prices)
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '036'
down_revision = '035'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: Add Hungary to entsoe_areas
    op.execute("""
        INSERT INTO entsoe_areas (id, code, country_name, country_code, is_active)
        VALUES (9, '10YHU-MAVIR----U', 'Hungary', 'HU', true)
        ON CONFLICT (code) DO NOTHING;
    """)

    # Update sequence to account for new ID
    op.execute("SELECT setval('entsoe_areas_id_seq', GREATEST(9, (SELECT MAX(id) FROM entsoe_areas)));")

    # Step 2: Create entsoe_day_ahead_prices partitioned table
    op.execute("""
        CREATE TABLE entsoe_day_ahead_prices (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            price_eur_mwh NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, period, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)

    # Step 3: Create Hungary partition
    op.execute("""
        CREATE TABLE entsoe_day_ahead_prices_hu
        PARTITION OF entsoe_day_ahead_prices
        FOR VALUES IN ('HU');
    """)

    # Step 4: Create index for common queries
    op.execute("""
        CREATE INDEX ix_entsoe_day_ahead_prices_trade_date
        ON entsoe_day_ahead_prices (trade_date);
    """)


def downgrade() -> None:
    # Drop table (cascades to partitions)
    op.execute("DROP TABLE IF EXISTS entsoe_day_ahead_prices CASCADE;")

    # Remove Hungary from entsoe_areas
    op.execute("DELETE FROM entsoe_areas WHERE code = '10YHU-MAVIR----U';")
