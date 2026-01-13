"""Add CEPS Export/Import SVR tables with year partitioning.

Revision ID: 032
Revises: 031
Create Date: 2026-01-09

Creates two new tables in the finance schema for CEPS Export/Import SVR data:
1. ceps_export_import_svr_1min - minute-level raw exchange data
2. ceps_export_import_svr_15min - 15-minute aggregated exchange statistics

Both tables use RANGE partitioning by year for efficient data management.
Exchange data includes ImbalanceNetting, Mari (mFRR), Picasso (aFRR), and total sum.
"""

from alembic import op
import sqlalchemy as sa

revision = '032'
down_revision = '031'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create CEPS Export/Import SVR tables with year-based partitioning."""

    # ========================================================================
    # Table 1: ceps_export_import_svr_1min (minute-level raw exchange data)
    # ========================================================================

    op.execute("""
        CREATE TABLE finance.ceps_export_import_svr_1min (
            id BIGSERIAL,
            delivery_timestamp TIMESTAMP NOT NULL,
            imbalance_netting_mw NUMERIC(15,5),
            mari_mfrr_mw NUMERIC(15,5),
            picasso_afrr_mw NUMERIC(15,5),
            sum_exchange_european_platforms_mw NUMERIC(15,5),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_ceps_export_import_svr_1min_delivery_timestamp UNIQUE (delivery_timestamp)
        ) PARTITION BY RANGE (delivery_timestamp);
    """)

    # Create partitions for years 2024-2028
    # Each partition covers one calendar year
    op.execute("""
        CREATE TABLE finance.ceps_export_import_svr_1min_2024
        PARTITION OF finance.ceps_export_import_svr_1min
        FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2025-01-01 00:00:00');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_export_import_svr_1min_2025
        PARTITION OF finance.ceps_export_import_svr_1min
        FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2026-01-01 00:00:00');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_export_import_svr_1min_2026
        PARTITION OF finance.ceps_export_import_svr_1min
        FOR VALUES FROM ('2026-01-01 00:00:00') TO ('2027-01-01 00:00:00');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_export_import_svr_1min_2027
        PARTITION OF finance.ceps_export_import_svr_1min
        FOR VALUES FROM ('2027-01-01 00:00:00') TO ('2028-01-01 00:00:00');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_export_import_svr_1min_2028
        PARTITION OF finance.ceps_export_import_svr_1min
        FOR VALUES FROM ('2028-01-01 00:00:00') TO ('2029-01-01 00:00:00');
    """)

    # Create indexes for efficient querying
    op.execute("""
        CREATE INDEX idx_ceps_export_import_svr_1min_delivery_timestamp
        ON finance.ceps_export_import_svr_1min (delivery_timestamp);
    """)

    op.execute("""
        CREATE INDEX idx_ceps_export_import_svr_1min_created_at
        ON finance.ceps_export_import_svr_1min (created_at);
    """)

    # ========================================================================
    # Table 2: ceps_export_import_svr_15min (15-minute aggregated exchange)
    # ========================================================================

    op.execute("""
        CREATE TABLE finance.ceps_export_import_svr_15min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            imbalance_netting_mean_mw NUMERIC(15,5),
            mari_mfrr_mean_mw NUMERIC(15,5),
            picasso_afrr_mean_mw NUMERIC(15,5),
            sum_exchange_mean_mw NUMERIC(15,5),
            imbalance_netting_median_mw NUMERIC(15,5),
            mari_mfrr_median_mw NUMERIC(15,5),
            picasso_afrr_median_mw NUMERIC(15,5),
            sum_exchange_median_mw NUMERIC(15,5),
            imbalance_netting_last_at_interval_mw NUMERIC(15,5),
            mari_mfrr_last_at_interval_mw NUMERIC(15,5),
            picasso_afrr_last_at_interval_mw NUMERIC(15,5),
            sum_exchange_last_at_interval_mw NUMERIC(15,5),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_ceps_export_import_svr_15min_trade_date_interval UNIQUE (trade_date, time_interval)
        ) PARTITION BY RANGE (trade_date);
    """)

    # Create partitions for years 2024-2028
    op.execute("""
        CREATE TABLE finance.ceps_export_import_svr_15min_2024
        PARTITION OF finance.ceps_export_import_svr_15min
        FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_export_import_svr_15min_2025
        PARTITION OF finance.ceps_export_import_svr_15min
        FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_export_import_svr_15min_2026
        PARTITION OF finance.ceps_export_import_svr_15min
        FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_export_import_svr_15min_2027
        PARTITION OF finance.ceps_export_import_svr_15min
        FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_export_import_svr_15min_2028
        PARTITION OF finance.ceps_export_import_svr_15min
        FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
    """)

    # Create indexes for efficient querying
    op.execute("""
        CREATE INDEX idx_ceps_export_import_svr_15min_trade_date
        ON finance.ceps_export_import_svr_15min (trade_date);
    """)

    op.execute("""
        CREATE INDEX idx_ceps_export_import_svr_15min_time_interval
        ON finance.ceps_export_import_svr_15min (time_interval);
    """)

    op.execute("""
        CREATE INDEX idx_ceps_export_import_svr_15min_created_at
        ON finance.ceps_export_import_svr_15min (created_at);
    """)


def downgrade() -> None:
    """Drop CEPS Export/Import SVR tables and all partitions."""

    # Drop 15min table (cascades to all partitions)
    op.execute("DROP TABLE IF EXISTS finance.ceps_export_import_svr_15min CASCADE;")

    # Drop 1min table (cascades to all partitions)
    op.execute("DROP TABLE IF EXISTS finance.ceps_export_import_svr_1min CASCADE;")
