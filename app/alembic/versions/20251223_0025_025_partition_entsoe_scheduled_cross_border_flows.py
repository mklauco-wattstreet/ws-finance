"""Partition entsoe_scheduled_cross_border_flows table by country_code.

Revision ID: 025
Revises: 024
Create Date: 2025-12-23

Converts entsoe_scheduled_cross_border_flows from flat table to partitioned structure.

New schema:
- Primary Key: (trade_date, period, area_id, country_code)
- Partition Strategy: LIST (country_code)
- Partitions: CZ, DE, AT, PL, SK
"""

from alembic import op
import sqlalchemy as sa

revision = '025'
down_revision = '024'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: Rename legacy table
    op.execute("ALTER TABLE entsoe_scheduled_cross_border_flows RENAME TO entsoe_scheduled_cross_border_flows_old;")

    # Step 2: Create new partitioned table
    op.execute("""
        CREATE TABLE entsoe_scheduled_cross_border_flows (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            scheduled_de_mw NUMERIC(12,3),
            scheduled_at_mw NUMERIC(12,3),
            scheduled_pl_mw NUMERIC(12,3),
            scheduled_sk_mw NUMERIC(12,3),
            scheduled_total_net_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, period, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)

    # Step 3: Create partitions
    op.execute("""
        CREATE TABLE entsoe_scheduled_cross_border_flows_cz
        PARTITION OF entsoe_scheduled_cross_border_flows
        FOR VALUES IN ('CZ');
    """)
    op.execute("""
        CREATE TABLE entsoe_scheduled_cross_border_flows_de
        PARTITION OF entsoe_scheduled_cross_border_flows
        FOR VALUES IN ('DE');
    """)
    op.execute("""
        CREATE TABLE entsoe_scheduled_cross_border_flows_at
        PARTITION OF entsoe_scheduled_cross_border_flows
        FOR VALUES IN ('AT');
    """)
    op.execute("""
        CREATE TABLE entsoe_scheduled_cross_border_flows_pl
        PARTITION OF entsoe_scheduled_cross_border_flows
        FOR VALUES IN ('PL');
    """)
    op.execute("""
        CREATE TABLE entsoe_scheduled_cross_border_flows_sk
        PARTITION OF entsoe_scheduled_cross_border_flows
        FOR VALUES IN ('SK');
    """)

    # Step 4: Migrate data from old table (all existing data is CZ, area_id=1)
    op.execute("""
        INSERT INTO entsoe_scheduled_cross_border_flows
            (trade_date, period, area_id, country_code, time_interval,
             scheduled_de_mw, scheduled_at_mw, scheduled_pl_mw, scheduled_sk_mw,
             scheduled_total_net_mw, created_at)
        SELECT
            trade_date, period, 1, 'CZ', time_interval,
            scheduled_de_mw, scheduled_at_mw, scheduled_pl_mw, scheduled_sk_mw,
            scheduled_total_net_mw, created_at
        FROM entsoe_scheduled_cross_border_flows_old;
    """)

    # Step 5: Create index on trade_date
    op.execute("""
        CREATE INDEX ix_entsoe_scheduled_cross_border_flows_trade_date
        ON entsoe_scheduled_cross_border_flows (trade_date);
    """)

    # Step 6: Drop old table
    op.execute("DROP TABLE entsoe_scheduled_cross_border_flows_old;")


def downgrade() -> None:
    # Detach partitions
    op.execute("ALTER TABLE entsoe_scheduled_cross_border_flows DETACH PARTITION entsoe_scheduled_cross_border_flows_cz;")
    op.execute("ALTER TABLE entsoe_scheduled_cross_border_flows DETACH PARTITION entsoe_scheduled_cross_border_flows_de;")
    op.execute("ALTER TABLE entsoe_scheduled_cross_border_flows DETACH PARTITION entsoe_scheduled_cross_border_flows_at;")
    op.execute("ALTER TABLE entsoe_scheduled_cross_border_flows DETACH PARTITION entsoe_scheduled_cross_border_flows_pl;")
    op.execute("ALTER TABLE entsoe_scheduled_cross_border_flows DETACH PARTITION entsoe_scheduled_cross_border_flows_sk;")

    # Rename for migration
    op.execute("ALTER TABLE entsoe_scheduled_cross_border_flows_cz RENAME TO entsoe_scheduled_cross_border_flows_new_cz;")

    # Remove sequence dependency
    op.execute("ALTER TABLE entsoe_scheduled_cross_border_flows_new_cz ALTER COLUMN id DROP DEFAULT;")

    # Drop partitioned parent
    op.execute("DROP TABLE entsoe_scheduled_cross_border_flows;")

    # Recreate original flat table
    op.execute("""
        CREATE TABLE entsoe_scheduled_cross_border_flows (
            id SERIAL PRIMARY KEY,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            scheduled_de_mw NUMERIC(12,3),
            scheduled_at_mw NUMERIC(12,3),
            scheduled_pl_mw NUMERIC(12,3),
            scheduled_sk_mw NUMERIC(12,3),
            scheduled_total_net_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (trade_date, period),
            UNIQUE (trade_date, time_interval)
        );
    """)

    # Migrate CZ data back
    op.execute("""
        INSERT INTO entsoe_scheduled_cross_border_flows
            (trade_date, period, time_interval,
             scheduled_de_mw, scheduled_at_mw, scheduled_pl_mw, scheduled_sk_mw,
             scheduled_total_net_mw, created_at)
        SELECT
            trade_date, period, time_interval,
            scheduled_de_mw, scheduled_at_mw, scheduled_pl_mw, scheduled_sk_mw,
            scheduled_total_net_mw, created_at
        FROM entsoe_scheduled_cross_border_flows_new_cz
        WHERE country_code = 'CZ';
    """)

    # Drop temp tables
    op.execute("DROP TABLE entsoe_scheduled_cross_border_flows_new_cz;")
    op.execute("DROP TABLE IF EXISTS entsoe_scheduled_cross_border_flows_de;")
    op.execute("DROP TABLE IF EXISTS entsoe_scheduled_cross_border_flows_at;")
    op.execute("DROP TABLE IF EXISTS entsoe_scheduled_cross_border_flows_pl;")
    op.execute("DROP TABLE IF EXISTS entsoe_scheduled_cross_border_flows_sk;")
