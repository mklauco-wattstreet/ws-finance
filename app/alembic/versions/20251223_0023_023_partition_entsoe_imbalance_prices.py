"""Partition entsoe_imbalance_prices table by country_code.

Revision ID: 023
Revises: 022
Create Date: 2025-12-23

Converts entsoe_imbalance_prices from flat table to partitioned structure.

New schema:
- Primary Key: (trade_date, period, area_id, country_code)
- Partition Strategy: LIST (country_code)
- Partitions: CZ, DE, AT, PL, SK
"""

from alembic import op
import sqlalchemy as sa

revision = '023'
down_revision = '022'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: Rename legacy table
    op.execute("ALTER TABLE entsoe_imbalance_prices RENAME TO entsoe_imbalance_prices_old;")

    # Step 2: Create new partitioned table
    op.execute("""
        CREATE TABLE entsoe_imbalance_prices (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            pos_imb_price_czk_mwh NUMERIC(15,3),
            pos_imb_scarcity_czk_mwh NUMERIC(15,3),
            pos_imb_incentive_czk_mwh NUMERIC(15,3),
            pos_imb_financial_neutrality_czk_mwh NUMERIC(15,3),
            neg_imb_price_czk_mwh NUMERIC(15,3),
            neg_imb_scarcity_czk_mwh NUMERIC(15,3),
            neg_imb_incentive_czk_mwh NUMERIC(15,3),
            neg_imb_financial_neutrality_czk_mwh NUMERIC(15,3),
            imbalance_mwh NUMERIC(12,5),
            difference_mwh NUMERIC(12,5),
            situation VARCHAR,
            status VARCHAR,
            delivery_datetime TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, period, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)

    # Step 3: Create partitions
    op.execute("""
        CREATE TABLE entsoe_imbalance_prices_cz
        PARTITION OF entsoe_imbalance_prices
        FOR VALUES IN ('CZ');
    """)
    op.execute("""
        CREATE TABLE entsoe_imbalance_prices_de
        PARTITION OF entsoe_imbalance_prices
        FOR VALUES IN ('DE');
    """)
    op.execute("""
        CREATE TABLE entsoe_imbalance_prices_at
        PARTITION OF entsoe_imbalance_prices
        FOR VALUES IN ('AT');
    """)
    op.execute("""
        CREATE TABLE entsoe_imbalance_prices_pl
        PARTITION OF entsoe_imbalance_prices
        FOR VALUES IN ('PL');
    """)
    op.execute("""
        CREATE TABLE entsoe_imbalance_prices_sk
        PARTITION OF entsoe_imbalance_prices
        FOR VALUES IN ('SK');
    """)

    # Step 4: Migrate data from old table (all existing data is CZ, area_id=1)
    op.execute("""
        INSERT INTO entsoe_imbalance_prices
            (trade_date, period, area_id, country_code, time_interval,
             pos_imb_price_czk_mwh, pos_imb_scarcity_czk_mwh,
             pos_imb_incentive_czk_mwh, pos_imb_financial_neutrality_czk_mwh,
             neg_imb_price_czk_mwh, neg_imb_scarcity_czk_mwh,
             neg_imb_incentive_czk_mwh, neg_imb_financial_neutrality_czk_mwh,
             imbalance_mwh, difference_mwh, situation, status,
             delivery_datetime, created_at)
        SELECT
            trade_date, period, 1, 'CZ', time_interval,
            pos_imb_price_czk_mwh, pos_imb_scarcity_czk_mwh,
            pos_imb_incentive_czk_mwh, pos_imb_financial_neutrality_czk_mwh,
            neg_imb_price_czk_mwh, neg_imb_scarcity_czk_mwh,
            neg_imb_incentive_czk_mwh, neg_imb_financial_neutrality_czk_mwh,
            imbalance_mwh, difference_mwh, situation, status,
            delivery_datetime, created_at
        FROM entsoe_imbalance_prices_old;
    """)

    # Step 5: Create index on trade_date
    op.execute("""
        CREATE INDEX ix_entsoe_imbalance_prices_trade_date
        ON entsoe_imbalance_prices (trade_date);
    """)

    # Step 6: Drop old table
    op.execute("DROP TABLE entsoe_imbalance_prices_old;")


def downgrade() -> None:
    # Detach partitions
    op.execute("ALTER TABLE entsoe_imbalance_prices DETACH PARTITION entsoe_imbalance_prices_cz;")
    op.execute("ALTER TABLE entsoe_imbalance_prices DETACH PARTITION entsoe_imbalance_prices_de;")
    op.execute("ALTER TABLE entsoe_imbalance_prices DETACH PARTITION entsoe_imbalance_prices_at;")
    op.execute("ALTER TABLE entsoe_imbalance_prices DETACH PARTITION entsoe_imbalance_prices_pl;")
    op.execute("ALTER TABLE entsoe_imbalance_prices DETACH PARTITION entsoe_imbalance_prices_sk;")

    # Rename for migration
    op.execute("ALTER TABLE entsoe_imbalance_prices_cz RENAME TO entsoe_imbalance_prices_new_cz;")

    # Remove sequence dependency
    op.execute("ALTER TABLE entsoe_imbalance_prices_new_cz ALTER COLUMN id DROP DEFAULT;")

    # Drop partitioned parent
    op.execute("DROP TABLE entsoe_imbalance_prices;")

    # Recreate original flat table
    op.execute("""
        CREATE TABLE entsoe_imbalance_prices (
            id SERIAL PRIMARY KEY,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            pos_imb_price_czk_mwh NUMERIC(15,3),
            pos_imb_scarcity_czk_mwh NUMERIC(15,3),
            pos_imb_incentive_czk_mwh NUMERIC(15,3),
            pos_imb_financial_neutrality_czk_mwh NUMERIC(15,3),
            neg_imb_price_czk_mwh NUMERIC(15,3),
            neg_imb_scarcity_czk_mwh NUMERIC(15,3),
            neg_imb_incentive_czk_mwh NUMERIC(15,3),
            neg_imb_financial_neutrality_czk_mwh NUMERIC(15,3),
            imbalance_mwh NUMERIC(12,5),
            difference_mwh NUMERIC(12,5),
            situation VARCHAR,
            status VARCHAR,
            delivery_datetime TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (trade_date, time_interval),
            UNIQUE (trade_date, period)
        );
    """)

    # Migrate CZ data back
    op.execute("""
        INSERT INTO entsoe_imbalance_prices
            (trade_date, period, time_interval,
             pos_imb_price_czk_mwh, pos_imb_scarcity_czk_mwh,
             pos_imb_incentive_czk_mwh, pos_imb_financial_neutrality_czk_mwh,
             neg_imb_price_czk_mwh, neg_imb_scarcity_czk_mwh,
             neg_imb_incentive_czk_mwh, neg_imb_financial_neutrality_czk_mwh,
             imbalance_mwh, difference_mwh, situation, status,
             delivery_datetime, created_at)
        SELECT
            trade_date, period, time_interval,
            pos_imb_price_czk_mwh, pos_imb_scarcity_czk_mwh,
            pos_imb_incentive_czk_mwh, pos_imb_financial_neutrality_czk_mwh,
            neg_imb_price_czk_mwh, neg_imb_scarcity_czk_mwh,
            neg_imb_incentive_czk_mwh, neg_imb_financial_neutrality_czk_mwh,
            imbalance_mwh, difference_mwh, situation, status,
            delivery_datetime, created_at
        FROM entsoe_imbalance_prices_new_cz
        WHERE country_code = 'CZ';
    """)

    # Drop temp tables
    op.execute("DROP TABLE entsoe_imbalance_prices_new_cz;")
    op.execute("DROP TABLE IF EXISTS entsoe_imbalance_prices_de;")
    op.execute("DROP TABLE IF EXISTS entsoe_imbalance_prices_at;")
    op.execute("DROP TABLE IF EXISTS entsoe_imbalance_prices_pl;")
    op.execute("DROP TABLE IF EXISTS entsoe_imbalance_prices_sk;")
