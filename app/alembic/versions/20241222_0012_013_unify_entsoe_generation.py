"""Unify entsoe_generation_actual to partitioned table.

Revision ID: 013
Revises: 012
Create Date: 2024-12-22

Creates a partitioned entsoe_generation_actual table with area_id,
enabling multi-area generation storage with efficient partition pruning.

Steps:
1. Rename existing table to _old
2. Create new partitioned table
3. Create partitions for each area (CZ, DE, AT, PL, SK)
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '013'
down_revision = '012'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: Rename existing table
    op.rename_table('entsoe_generation_actual', 'entsoe_generation_actual_old')

    # Step 2: Create new partitioned table
    # Note: Using raw SQL because SQLAlchemy doesn't natively support PARTITION BY
    op.execute("""
        CREATE TABLE entsoe_generation_actual (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            area_id INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            gen_nuclear_mw NUMERIC(12,3),
            gen_coal_mw NUMERIC(12,3),
            gen_gas_mw NUMERIC(12,3),
            gen_solar_mw NUMERIC(12,3),
            gen_wind_mw NUMERIC(12,3),
            gen_wind_offshore_mw NUMERIC(12,3),
            gen_hydro_pumped_mw NUMERIC(12,3),
            gen_biomass_mw NUMERIC(12,3),
            gen_hydro_other_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, period, area_id)
        ) PARTITION BY LIST (area_id);
    """)

    # Add foreign key constraint (not enforced on partitioned tables directly,
    # but documented for reference)
    op.execute("""
        COMMENT ON COLUMN entsoe_generation_actual.area_id IS
        'References entsoe_areas(id). FK enforced at application level.';
    """)

    # Step 3: Create partitions for each area
    # CZ (area_id = 1)
    op.execute("""
        CREATE TABLE entsoe_generation_actual_cz
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN (1);
    """)

    # DE (area_id = 2)
    op.execute("""
        CREATE TABLE entsoe_generation_actual_de
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN (2);
    """)

    # AT (area_id = 3)
    op.execute("""
        CREATE TABLE entsoe_generation_actual_at
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN (3);
    """)

    # PL (area_id = 4)
    op.execute("""
        CREATE TABLE entsoe_generation_actual_pl
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN (4);
    """)

    # SK (area_id = 5)
    op.execute("""
        CREATE TABLE entsoe_generation_actual_sk
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN (5);
    """)

    # Create indexes for common query patterns
    # Index on trade_date for date range queries (inherited by partitions)
    op.execute("""
        CREATE INDEX ix_entsoe_generation_actual_trade_date
        ON entsoe_generation_actual (trade_date);
    """)


def downgrade() -> None:
    # Drop partitioned table and partitions
    op.execute("DROP TABLE IF EXISTS entsoe_generation_actual CASCADE;")

    # Restore original table
    op.rename_table('entsoe_generation_actual_old', 'entsoe_generation_actual')
