"""Partition entsoe_cross_border_flows table by country_code.

Revision ID: 024
Revises: 023
Create Date: 2025-12-23

Converts entsoe_cross_border_flows from flat table to partitioned structure.
Note: area_id changes from VARCHAR(20) to INTEGER for consistency.

New schema:
- Primary Key: (trade_date, period, area_id, country_code)
- Partition Strategy: LIST (country_code)
- Partitions: CZ, DE, AT, PL, SK
"""

from alembic import op
import sqlalchemy as sa

revision = '024'
down_revision = '023'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: Rename legacy table
    op.execute("ALTER TABLE entsoe_cross_border_flows RENAME TO entsoe_cross_border_flows_old;")

    # Step 2: Create new partitioned table
    # Note: area_id is now INTEGER (was VARCHAR(20) storing EIC codes)
    op.execute("""
        CREATE TABLE entsoe_cross_border_flows (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            delivery_datetime TIMESTAMP NOT NULL,
            flow_de_mw NUMERIC(12,3),
            flow_at_mw NUMERIC(12,3),
            flow_pl_mw NUMERIC(12,3),
            flow_sk_mw NUMERIC(12,3),
            flow_total_net_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, period, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)

    # Step 3: Create partitions
    op.execute("""
        CREATE TABLE entsoe_cross_border_flows_cz
        PARTITION OF entsoe_cross_border_flows
        FOR VALUES IN ('CZ');
    """)
    op.execute("""
        CREATE TABLE entsoe_cross_border_flows_de
        PARTITION OF entsoe_cross_border_flows
        FOR VALUES IN ('DE');
    """)
    op.execute("""
        CREATE TABLE entsoe_cross_border_flows_at
        PARTITION OF entsoe_cross_border_flows
        FOR VALUES IN ('AT');
    """)
    op.execute("""
        CREATE TABLE entsoe_cross_border_flows_pl
        PARTITION OF entsoe_cross_border_flows
        FOR VALUES IN ('PL');
    """)
    op.execute("""
        CREATE TABLE entsoe_cross_border_flows_sk
        PARTITION OF entsoe_cross_border_flows
        FOR VALUES IN ('SK');
    """)

    # Step 4: Migrate data from old table
    # Old table had area_id as VARCHAR(20) with EIC code, we convert to integer (1=CZ)
    op.execute("""
        INSERT INTO entsoe_cross_border_flows
            (trade_date, period, area_id, country_code, time_interval,
             delivery_datetime, flow_de_mw, flow_at_mw, flow_pl_mw, flow_sk_mw,
             flow_total_net_mw, created_at)
        SELECT
            trade_date, period, 1, 'CZ', time_interval,
            delivery_datetime, flow_de_mw, flow_at_mw, flow_pl_mw, flow_sk_mw,
            flow_total_net_mw, created_at
        FROM entsoe_cross_border_flows_old;
    """)

    # Step 5: Create index on trade_date
    op.execute("""
        CREATE INDEX ix_entsoe_cross_border_flows_trade_date
        ON entsoe_cross_border_flows (trade_date);
    """)

    # Step 6: Drop old table
    op.execute("DROP TABLE entsoe_cross_border_flows_old;")


def downgrade() -> None:
    # Detach partitions
    op.execute("ALTER TABLE entsoe_cross_border_flows DETACH PARTITION entsoe_cross_border_flows_cz;")
    op.execute("ALTER TABLE entsoe_cross_border_flows DETACH PARTITION entsoe_cross_border_flows_de;")
    op.execute("ALTER TABLE entsoe_cross_border_flows DETACH PARTITION entsoe_cross_border_flows_at;")
    op.execute("ALTER TABLE entsoe_cross_border_flows DETACH PARTITION entsoe_cross_border_flows_pl;")
    op.execute("ALTER TABLE entsoe_cross_border_flows DETACH PARTITION entsoe_cross_border_flows_sk;")

    # Rename for migration
    op.execute("ALTER TABLE entsoe_cross_border_flows_cz RENAME TO entsoe_cross_border_flows_new_cz;")

    # Remove sequence dependency
    op.execute("ALTER TABLE entsoe_cross_border_flows_new_cz ALTER COLUMN id DROP DEFAULT;")

    # Drop partitioned parent
    op.execute("DROP TABLE entsoe_cross_border_flows;")

    # Recreate original flat table (with VARCHAR area_id)
    op.execute("""
        CREATE TABLE entsoe_cross_border_flows (
            id SERIAL PRIMARY KEY,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            delivery_datetime TIMESTAMP NOT NULL,
            area_id VARCHAR(20) NOT NULL,
            flow_de_mw NUMERIC(12,3),
            flow_at_mw NUMERIC(12,3),
            flow_pl_mw NUMERIC(12,3),
            flow_sk_mw NUMERIC(12,3),
            flow_total_net_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (delivery_datetime, area_id),
            UNIQUE (trade_date, period, area_id)
        );
    """)

    # Migrate CZ data back (convert area_id back to EIC code)
    op.execute("""
        INSERT INTO entsoe_cross_border_flows
            (trade_date, period, time_interval, delivery_datetime, area_id,
             flow_de_mw, flow_at_mw, flow_pl_mw, flow_sk_mw, flow_total_net_mw, created_at)
        SELECT
            trade_date, period, time_interval, delivery_datetime, '10YCZ-CEPS-----N',
            flow_de_mw, flow_at_mw, flow_pl_mw, flow_sk_mw, flow_total_net_mw, created_at
        FROM entsoe_cross_border_flows_new_cz
        WHERE country_code = 'CZ';
    """)

    # Drop temp tables
    op.execute("DROP TABLE entsoe_cross_border_flows_new_cz;")
    op.execute("DROP TABLE IF EXISTS entsoe_cross_border_flows_de;")
    op.execute("DROP TABLE IF EXISTS entsoe_cross_border_flows_at;")
    op.execute("DROP TABLE IF EXISTS entsoe_cross_border_flows_pl;")
    op.execute("DROP TABLE IF EXISTS entsoe_cross_border_flows_sk;")
