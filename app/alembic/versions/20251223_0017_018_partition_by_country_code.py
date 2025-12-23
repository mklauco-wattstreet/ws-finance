"""Restructure entsoe_generation_actual to partition by country_code.

Revision ID: 018
Revises: 017
Create Date: 2025-12-23

Changes partitioning strategy from area_id integers to country_code strings.
This allows new TSOs/bidding zones to be added without modifying partition DDL.

Before: FOR VALUES IN (2, 6, 7, 8) -- fragile, needs update for new TSOs
After:  FOR VALUES IN ('DE')       -- stable, new TSOs auto-route

Steps:
1. Detach existing partitions (they become regular tables)
2. Drop parent partitioned table
3. Create new partitioned table with country_code column
4. Create country-based partitions
5. Migrate data from old tables
6. Drop old tables
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '018'
down_revision = '017'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: Detach all existing partitions
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_cz;")
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_de;")
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_at;")
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_pl;")
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_sk;")

    # Step 2: Rename old tables for data migration
    op.execute("ALTER TABLE entsoe_generation_actual_cz RENAME TO entsoe_generation_actual_old_cz;")
    op.execute("ALTER TABLE entsoe_generation_actual_de RENAME TO entsoe_generation_actual_old_de;")
    op.execute("ALTER TABLE entsoe_generation_actual_at RENAME TO entsoe_generation_actual_old_at;")
    op.execute("ALTER TABLE entsoe_generation_actual_pl RENAME TO entsoe_generation_actual_old_pl;")
    op.execute("ALTER TABLE entsoe_generation_actual_sk RENAME TO entsoe_generation_actual_old_sk;")

    # Step 3: Remove sequence dependency from detached tables before dropping parent
    op.execute("ALTER TABLE entsoe_generation_actual_old_cz ALTER COLUMN id DROP DEFAULT;")
    op.execute("ALTER TABLE entsoe_generation_actual_old_de ALTER COLUMN id DROP DEFAULT;")
    op.execute("ALTER TABLE entsoe_generation_actual_old_at ALTER COLUMN id DROP DEFAULT;")
    op.execute("ALTER TABLE entsoe_generation_actual_old_pl ALTER COLUMN id DROP DEFAULT;")
    op.execute("ALTER TABLE entsoe_generation_actual_old_sk ALTER COLUMN id DROP DEFAULT;")

    # Step 4: Drop old parent table (now empty after detaching partitions)
    op.execute("DROP TABLE entsoe_generation_actual;")

    # Step 5: Create new partitioned table with country_code
    op.execute("""
        CREATE TABLE entsoe_generation_actual (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
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
            PRIMARY KEY (trade_date, period, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)

    # Step 6: Create country-based partitions
    op.execute("""
        CREATE TABLE entsoe_generation_actual_cz
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN ('CZ');
    """)

    op.execute("""
        CREATE TABLE entsoe_generation_actual_de
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN ('DE');
    """)

    op.execute("""
        CREATE TABLE entsoe_generation_actual_at
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN ('AT');
    """)

    op.execute("""
        CREATE TABLE entsoe_generation_actual_pl
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN ('PL');
    """)

    op.execute("""
        CREATE TABLE entsoe_generation_actual_sk
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN ('SK');
    """)

    # Step 7: Migrate data from old tables with country_code lookup
    # CZ (area_id = 1)
    op.execute("""
        INSERT INTO entsoe_generation_actual
            (trade_date, period, area_id, country_code, time_interval,
             gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
             gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
             gen_biomass_mw, gen_hydro_other_mw, created_at)
        SELECT
            trade_date, period, area_id, 'CZ', time_interval,
            gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
            gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
            gen_biomass_mw, gen_hydro_other_mw, created_at
        FROM entsoe_generation_actual_old_cz;
    """)

    # DE (area_ids 2, 6, 7, 8)
    op.execute("""
        INSERT INTO entsoe_generation_actual
            (trade_date, period, area_id, country_code, time_interval,
             gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
             gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
             gen_biomass_mw, gen_hydro_other_mw, created_at)
        SELECT
            trade_date, period, area_id, 'DE', time_interval,
            gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
            gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
            gen_biomass_mw, gen_hydro_other_mw, created_at
        FROM entsoe_generation_actual_old_de;
    """)

    # AT (area_id = 3)
    op.execute("""
        INSERT INTO entsoe_generation_actual
            (trade_date, period, area_id, country_code, time_interval,
             gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
             gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
             gen_biomass_mw, gen_hydro_other_mw, created_at)
        SELECT
            trade_date, period, area_id, 'AT', time_interval,
            gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
            gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
            gen_biomass_mw, gen_hydro_other_mw, created_at
        FROM entsoe_generation_actual_old_at;
    """)

    # PL (area_id = 4)
    op.execute("""
        INSERT INTO entsoe_generation_actual
            (trade_date, period, area_id, country_code, time_interval,
             gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
             gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
             gen_biomass_mw, gen_hydro_other_mw, created_at)
        SELECT
            trade_date, period, area_id, 'PL', time_interval,
            gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
            gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
            gen_biomass_mw, gen_hydro_other_mw, created_at
        FROM entsoe_generation_actual_old_pl;
    """)

    # SK (area_id = 5)
    op.execute("""
        INSERT INTO entsoe_generation_actual
            (trade_date, period, area_id, country_code, time_interval,
             gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
             gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
             gen_biomass_mw, gen_hydro_other_mw, created_at)
        SELECT
            trade_date, period, area_id, 'SK', time_interval,
            gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
            gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
            gen_biomass_mw, gen_hydro_other_mw, created_at
        FROM entsoe_generation_actual_old_sk;
    """)

    # Step 8: Create index on trade_date for common queries
    op.execute("""
        CREATE INDEX ix_entsoe_generation_actual_trade_date
        ON entsoe_generation_actual (trade_date);
    """)

    # Step 9: Drop old tables
    op.execute("DROP TABLE entsoe_generation_actual_old_cz;")
    op.execute("DROP TABLE entsoe_generation_actual_old_de;")
    op.execute("DROP TABLE entsoe_generation_actual_old_at;")
    op.execute("DROP TABLE entsoe_generation_actual_old_pl;")
    op.execute("DROP TABLE entsoe_generation_actual_old_sk;")


def downgrade() -> None:
    # Detach new partitions
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_cz;")
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_de;")
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_at;")
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_pl;")
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_sk;")

    # Rename for migration
    op.execute("ALTER TABLE entsoe_generation_actual_cz RENAME TO entsoe_generation_actual_new_cz;")
    op.execute("ALTER TABLE entsoe_generation_actual_de RENAME TO entsoe_generation_actual_new_de;")
    op.execute("ALTER TABLE entsoe_generation_actual_at RENAME TO entsoe_generation_actual_new_at;")
    op.execute("ALTER TABLE entsoe_generation_actual_pl RENAME TO entsoe_generation_actual_new_pl;")
    op.execute("ALTER TABLE entsoe_generation_actual_sk RENAME TO entsoe_generation_actual_new_sk;")

    # Remove sequence dependency before dropping parent
    op.execute("ALTER TABLE entsoe_generation_actual_new_cz ALTER COLUMN id DROP DEFAULT;")
    op.execute("ALTER TABLE entsoe_generation_actual_new_de ALTER COLUMN id DROP DEFAULT;")
    op.execute("ALTER TABLE entsoe_generation_actual_new_at ALTER COLUMN id DROP DEFAULT;")
    op.execute("ALTER TABLE entsoe_generation_actual_new_pl ALTER COLUMN id DROP DEFAULT;")
    op.execute("ALTER TABLE entsoe_generation_actual_new_sk ALTER COLUMN id DROP DEFAULT;")

    # Drop new parent table
    op.execute("DROP TABLE entsoe_generation_actual;")

    # Recreate old structure (partitioned by area_id)
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

    # Recreate old partitions
    op.execute("CREATE TABLE entsoe_generation_actual_cz PARTITION OF entsoe_generation_actual FOR VALUES IN (1);")
    op.execute("CREATE TABLE entsoe_generation_actual_de PARTITION OF entsoe_generation_actual FOR VALUES IN (2, 6, 7, 8);")
    op.execute("CREATE TABLE entsoe_generation_actual_at PARTITION OF entsoe_generation_actual FOR VALUES IN (3);")
    op.execute("CREATE TABLE entsoe_generation_actual_pl PARTITION OF entsoe_generation_actual FOR VALUES IN (4);")
    op.execute("CREATE TABLE entsoe_generation_actual_sk PARTITION OF entsoe_generation_actual FOR VALUES IN (5);")

    # Migrate data back (without country_code)
    op.execute("""
        INSERT INTO entsoe_generation_actual
        SELECT id, trade_date, period, area_id, time_interval,
               gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
               gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
               gen_biomass_mw, gen_hydro_other_mw, created_at
        FROM entsoe_generation_actual_new_cz;
    """)
    op.execute("""
        INSERT INTO entsoe_generation_actual
        SELECT id, trade_date, period, area_id, time_interval,
               gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
               gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
               gen_biomass_mw, gen_hydro_other_mw, created_at
        FROM entsoe_generation_actual_new_de;
    """)
    op.execute("""
        INSERT INTO entsoe_generation_actual
        SELECT id, trade_date, period, area_id, time_interval,
               gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
               gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
               gen_biomass_mw, gen_hydro_other_mw, created_at
        FROM entsoe_generation_actual_new_at;
    """)
    op.execute("""
        INSERT INTO entsoe_generation_actual
        SELECT id, trade_date, period, area_id, time_interval,
               gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
               gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
               gen_biomass_mw, gen_hydro_other_mw, created_at
        FROM entsoe_generation_actual_new_pl;
    """)
    op.execute("""
        INSERT INTO entsoe_generation_actual
        SELECT id, trade_date, period, area_id, time_interval,
               gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
               gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw,
               gen_biomass_mw, gen_hydro_other_mw, created_at
        FROM entsoe_generation_actual_new_sk;
    """)

    # Drop temp tables
    op.execute("DROP TABLE entsoe_generation_actual_new_cz;")
    op.execute("DROP TABLE entsoe_generation_actual_new_de;")
    op.execute("DROP TABLE entsoe_generation_actual_new_at;")
    op.execute("DROP TABLE entsoe_generation_actual_new_pl;")
    op.execute("DROP TABLE entsoe_generation_actual_new_sk;")

    # Recreate index
    op.execute("CREATE INDEX ix_entsoe_generation_actual_trade_date ON entsoe_generation_actual (trade_date);")
