"""Partition entsoe_load table by country_code.

Revision ID: 019
Revises: 018
Create Date: 2025-12-23

Converts entsoe_load from flat table to partitioned structure matching
entsoe_generation_actual architecture.

New schema:
- Primary Key: (trade_date, period, area_id, country_code)
- Partition Strategy: LIST (country_code)
- Partitions: CZ, DE, AT, PL, SK
"""

from alembic import op
import sqlalchemy as sa

revision = '019'
down_revision = '018'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: Rename legacy table
    op.execute("ALTER TABLE entsoe_load RENAME TO entsoe_load_old;")

    # Step 2: Create new partitioned table
    op.execute("""
        CREATE TABLE entsoe_load (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            actual_load_mw NUMERIC(12,3),
            forecast_load_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, period, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)

    # Step 3: Create partitions
    op.execute("""
        CREATE TABLE entsoe_load_cz
        PARTITION OF entsoe_load
        FOR VALUES IN ('CZ');
    """)
    op.execute("""
        CREATE TABLE entsoe_load_de
        PARTITION OF entsoe_load
        FOR VALUES IN ('DE');
    """)
    op.execute("""
        CREATE TABLE entsoe_load_at
        PARTITION OF entsoe_load
        FOR VALUES IN ('AT');
    """)
    op.execute("""
        CREATE TABLE entsoe_load_pl
        PARTITION OF entsoe_load
        FOR VALUES IN ('PL');
    """)
    op.execute("""
        CREATE TABLE entsoe_load_sk
        PARTITION OF entsoe_load
        FOR VALUES IN ('SK');
    """)

    # Step 4: Migrate data from old table (all existing data is CZ, area_id=1)
    op.execute("""
        INSERT INTO entsoe_load
            (trade_date, period, area_id, country_code, time_interval,
             actual_load_mw, forecast_load_mw, created_at)
        SELECT
            trade_date, period, 1, 'CZ', time_interval,
            actual_load_mw, forecast_load_mw, created_at
        FROM entsoe_load_old;
    """)

    # Step 5: Create index on trade_date
    op.execute("""
        CREATE INDEX ix_entsoe_load_trade_date
        ON entsoe_load (trade_date);
    """)

    # Step 6: Drop old table
    op.execute("DROP TABLE entsoe_load_old;")


def downgrade() -> None:
    # Detach partitions
    op.execute("ALTER TABLE entsoe_load DETACH PARTITION entsoe_load_cz;")
    op.execute("ALTER TABLE entsoe_load DETACH PARTITION entsoe_load_de;")
    op.execute("ALTER TABLE entsoe_load DETACH PARTITION entsoe_load_at;")
    op.execute("ALTER TABLE entsoe_load DETACH PARTITION entsoe_load_pl;")
    op.execute("ALTER TABLE entsoe_load DETACH PARTITION entsoe_load_sk;")

    # Rename for migration
    op.execute("ALTER TABLE entsoe_load_cz RENAME TO entsoe_load_new_cz;")

    # Remove sequence dependency
    op.execute("ALTER TABLE entsoe_load_new_cz ALTER COLUMN id DROP DEFAULT;")

    # Drop partitioned parent
    op.execute("DROP TABLE entsoe_load;")

    # Recreate original flat table
    op.execute("""
        CREATE TABLE entsoe_load (
            id SERIAL PRIMARY KEY,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            actual_load_mw NUMERIC(12,3),
            forecast_load_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (trade_date, period),
            UNIQUE (trade_date, time_interval)
        );
    """)

    # Migrate CZ data back (drop area_id and country_code)
    op.execute("""
        INSERT INTO entsoe_load
            (trade_date, period, time_interval, actual_load_mw, forecast_load_mw, created_at)
        SELECT
            trade_date, period, time_interval, actual_load_mw, forecast_load_mw, created_at
        FROM entsoe_load_new_cz
        WHERE country_code = 'CZ';
    """)

    # Drop temp tables
    op.execute("DROP TABLE entsoe_load_new_cz;")
    op.execute("DROP TABLE IF EXISTS entsoe_load_de;")
    op.execute("DROP TABLE IF EXISTS entsoe_load_at;")
    op.execute("DROP TABLE IF EXISTS entsoe_load_pl;")
    op.execute("DROP TABLE IF EXISTS entsoe_load_sk;")
