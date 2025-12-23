"""Partition entsoe_balancing_energy table by country_code.

Revision ID: 022
Revises: 021
Create Date: 2025-12-23

Converts entsoe_balancing_energy from flat table to partitioned structure.

New schema:
- Primary Key: (trade_date, period, area_id, country_code)
- Partition Strategy: LIST (country_code)
- Partitions: CZ, DE, AT, PL, SK
"""

from alembic import op
import sqlalchemy as sa

revision = '022'
down_revision = '021'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: Rename legacy table
    op.execute("ALTER TABLE entsoe_balancing_energy RENAME TO entsoe_balancing_energy_old;")

    # Step 2: Create new partitioned table
    op.execute("""
        CREATE TABLE entsoe_balancing_energy (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            afrr_up_price_eur NUMERIC(15,3),
            afrr_down_price_eur NUMERIC(15,3),
            mfrr_up_price_eur NUMERIC(15,3),
            mfrr_down_price_eur NUMERIC(15,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, period, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)

    # Step 3: Create partitions
    op.execute("""
        CREATE TABLE entsoe_balancing_energy_cz
        PARTITION OF entsoe_balancing_energy
        FOR VALUES IN ('CZ');
    """)
    op.execute("""
        CREATE TABLE entsoe_balancing_energy_de
        PARTITION OF entsoe_balancing_energy
        FOR VALUES IN ('DE');
    """)
    op.execute("""
        CREATE TABLE entsoe_balancing_energy_at
        PARTITION OF entsoe_balancing_energy
        FOR VALUES IN ('AT');
    """)
    op.execute("""
        CREATE TABLE entsoe_balancing_energy_pl
        PARTITION OF entsoe_balancing_energy
        FOR VALUES IN ('PL');
    """)
    op.execute("""
        CREATE TABLE entsoe_balancing_energy_sk
        PARTITION OF entsoe_balancing_energy
        FOR VALUES IN ('SK');
    """)

    # Step 4: Migrate data from old table (all existing data is CZ, area_id=1)
    op.execute("""
        INSERT INTO entsoe_balancing_energy
            (trade_date, period, area_id, country_code, time_interval,
             afrr_up_price_eur, afrr_down_price_eur, mfrr_up_price_eur,
             mfrr_down_price_eur, created_at)
        SELECT
            trade_date, period, 1, 'CZ', time_interval,
            afrr_up_price_eur, afrr_down_price_eur, mfrr_up_price_eur,
            mfrr_down_price_eur, created_at
        FROM entsoe_balancing_energy_old;
    """)

    # Step 5: Create index on trade_date
    op.execute("""
        CREATE INDEX ix_entsoe_balancing_energy_trade_date
        ON entsoe_balancing_energy (trade_date);
    """)

    # Step 6: Drop old table
    op.execute("DROP TABLE entsoe_balancing_energy_old;")


def downgrade() -> None:
    # Detach partitions
    op.execute("ALTER TABLE entsoe_balancing_energy DETACH PARTITION entsoe_balancing_energy_cz;")
    op.execute("ALTER TABLE entsoe_balancing_energy DETACH PARTITION entsoe_balancing_energy_de;")
    op.execute("ALTER TABLE entsoe_balancing_energy DETACH PARTITION entsoe_balancing_energy_at;")
    op.execute("ALTER TABLE entsoe_balancing_energy DETACH PARTITION entsoe_balancing_energy_pl;")
    op.execute("ALTER TABLE entsoe_balancing_energy DETACH PARTITION entsoe_balancing_energy_sk;")

    # Rename for migration
    op.execute("ALTER TABLE entsoe_balancing_energy_cz RENAME TO entsoe_balancing_energy_new_cz;")

    # Remove sequence dependency
    op.execute("ALTER TABLE entsoe_balancing_energy_new_cz ALTER COLUMN id DROP DEFAULT;")

    # Drop partitioned parent
    op.execute("DROP TABLE entsoe_balancing_energy;")

    # Recreate original flat table
    op.execute("""
        CREATE TABLE entsoe_balancing_energy (
            id SERIAL PRIMARY KEY,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            afrr_up_price_eur NUMERIC(12,3),
            afrr_down_price_eur NUMERIC(12,3),
            mfrr_up_price_eur NUMERIC(12,3),
            mfrr_down_price_eur NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (trade_date, period),
            UNIQUE (trade_date, time_interval)
        );
    """)

    # Migrate CZ data back
    op.execute("""
        INSERT INTO entsoe_balancing_energy
            (trade_date, period, time_interval, afrr_up_price_eur, afrr_down_price_eur,
             mfrr_up_price_eur, mfrr_down_price_eur, created_at)
        SELECT
            trade_date, period, time_interval, afrr_up_price_eur, afrr_down_price_eur,
            mfrr_up_price_eur, mfrr_down_price_eur, created_at
        FROM entsoe_balancing_energy_new_cz
        WHERE country_code = 'CZ';
    """)

    # Drop temp tables
    op.execute("DROP TABLE entsoe_balancing_energy_new_cz;")
    op.execute("DROP TABLE IF EXISTS entsoe_balancing_energy_de;")
    op.execute("DROP TABLE IF EXISTS entsoe_balancing_energy_at;")
    op.execute("DROP TABLE IF EXISTS entsoe_balancing_energy_pl;")
    op.execute("DROP TABLE IF EXISTS entsoe_balancing_energy_sk;")
