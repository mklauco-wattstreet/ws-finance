"""Add DE-LU bidding zone to entsoe_areas and create DE/AT partitions for day-ahead prices.

Revision ID: 040
Revises: 039
Create Date: 2026-03-07

Adds:
1. DE-LU bidding zone (10Y1001A1001A82H) to entsoe_areas as area_id=10
2. Partition entsoe_day_ahead_prices_de for German DA prices
3. Partition entsoe_day_ahead_prices_at for Austrian DA prices

Note: DE-LU is the unified German-Luxembourg bidding zone used for DA prices (A44).
This is distinct from the four German TSO control areas (area_ids 2,6,7,8) used for generation data.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = '040'
down_revision = '039'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: Add DE-LU bidding zone to entsoe_areas
    op.execute("""
        INSERT INTO entsoe_areas (id, code, country_name, country_code, is_active)
        VALUES (10, '10Y1001A1001A82H', 'Germany-Luxembourg (BZ)', 'DE', true)
        ON CONFLICT (code) DO NOTHING;
    """)

    # Update sequence to account for new ID
    op.execute("SELECT setval('entsoe_areas_id_seq', GREATEST(10, (SELECT MAX(id) FROM entsoe_areas)));")

    # Step 2: Create DE partition for day-ahead prices
    op.execute("""
        CREATE TABLE entsoe_day_ahead_prices_de
        PARTITION OF entsoe_day_ahead_prices
        FOR VALUES IN ('DE');
    """)

    # Step 3: Create AT partition for day-ahead prices
    op.execute("""
        CREATE TABLE entsoe_day_ahead_prices_at
        PARTITION OF entsoe_day_ahead_prices
        FOR VALUES IN ('AT');
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS entsoe_day_ahead_prices_at;")
    op.execute("DROP TABLE IF EXISTS entsoe_day_ahead_prices_de;")
    op.execute("DELETE FROM entsoe_areas WHERE code = '10Y1001A1001A82H';")
