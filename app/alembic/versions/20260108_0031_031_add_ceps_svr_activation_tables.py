"""Add CEPS SVR activation tables with year partitioning.

Revision ID: 031
Revises: 030
Create Date: 2026-01-08

Creates two new tables in the finance schema for CEPS SVR (Secondary Reserve) activation data:
1. ceps_svr_activation_1min - minute-level raw activation data
2. ceps_svr_activation_15min - 15-minute aggregated activation statistics

Both tables use RANGE partitioning by year for efficient data management.
Activation data includes aFRR (automatic frequency restoration reserve) and mFRR (manual frequency restoration reserve).
"""

from alembic import op
import sqlalchemy as sa

revision = '031'
down_revision = '030'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create CEPS SVR activation tables with year-based partitioning."""

    # ========================================================================
    # Table 1: ceps_svr_activation_1min (minute-level raw activation data)
    # ========================================================================

    op.execute("""
        CREATE TABLE finance.ceps_svr_activation_1min (
            id BIGSERIAL,
            delivery_timestamp TIMESTAMP NOT NULL,
            afrr_plus_mw NUMERIC(15,3),
            afrr_minus_mw NUMERIC(15,3),
            mfrr_plus_mw NUMERIC(15,3),
            mfrr_minus_mw NUMERIC(15,3),
            mfrr_5_mw NUMERIC(15,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_ceps_svr_activation_1min_delivery_timestamp UNIQUE (delivery_timestamp)
        ) PARTITION BY RANGE (delivery_timestamp);
    """)

    # Create partitions for years 2024-2028
    # Each partition covers one calendar year
    op.execute("""
        CREATE TABLE finance.ceps_svr_activation_1min_2024
        PARTITION OF finance.ceps_svr_activation_1min
        FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2025-01-01 00:00:00');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_svr_activation_1min_2025
        PARTITION OF finance.ceps_svr_activation_1min
        FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2026-01-01 00:00:00');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_svr_activation_1min_2026
        PARTITION OF finance.ceps_svr_activation_1min
        FOR VALUES FROM ('2026-01-01 00:00:00') TO ('2027-01-01 00:00:00');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_svr_activation_1min_2027
        PARTITION OF finance.ceps_svr_activation_1min
        FOR VALUES FROM ('2027-01-01 00:00:00') TO ('2028-01-01 00:00:00');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_svr_activation_1min_2028
        PARTITION OF finance.ceps_svr_activation_1min
        FOR VALUES FROM ('2028-01-01 00:00:00') TO ('2029-01-01 00:00:00');
    """)

    # Create indexes for efficient querying
    op.execute("""
        CREATE INDEX idx_ceps_svr_activation_1min_delivery_timestamp
        ON finance.ceps_svr_activation_1min (delivery_timestamp);
    """)

    op.execute("""
        CREATE INDEX idx_ceps_svr_activation_1min_created_at
        ON finance.ceps_svr_activation_1min (created_at);
    """)

    # ========================================================================
    # Table 2: ceps_svr_activation_15min (15-minute aggregated activation)
    # ========================================================================

    op.execute("""
        CREATE TABLE finance.ceps_svr_activation_15min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            afrr_plus_mean_mw NUMERIC(15,3),
            afrr_minus_mean_mw NUMERIC(15,3),
            mfrr_plus_mean_mw NUMERIC(15,3),
            mfrr_minus_mean_mw NUMERIC(15,3),
            mfrr_5_mean_mw NUMERIC(15,3),
            afrr_plus_median_mw NUMERIC(15,3),
            afrr_minus_median_mw NUMERIC(15,3),
            mfrr_plus_median_mw NUMERIC(15,3),
            mfrr_minus_median_mw NUMERIC(15,3),
            mfrr_5_median_mw NUMERIC(15,3),
            afrr_plus_last_at_interval_mw NUMERIC(15,3),
            afrr_minus_last_at_interval_mw NUMERIC(15,3),
            mfrr_plus_last_at_interval_mw NUMERIC(15,3),
            mfrr_minus_last_at_interval_mw NUMERIC(15,3),
            mfrr_5_last_at_interval_mw NUMERIC(15,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_ceps_svr_activation_15min_trade_date_interval UNIQUE (trade_date, time_interval)
        ) PARTITION BY RANGE (trade_date);
    """)

    # Create partitions for years 2024-2028
    op.execute("""
        CREATE TABLE finance.ceps_svr_activation_15min_2024
        PARTITION OF finance.ceps_svr_activation_15min
        FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_svr_activation_15min_2025
        PARTITION OF finance.ceps_svr_activation_15min
        FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_svr_activation_15min_2026
        PARTITION OF finance.ceps_svr_activation_15min
        FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_svr_activation_15min_2027
        PARTITION OF finance.ceps_svr_activation_15min
        FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_svr_activation_15min_2028
        PARTITION OF finance.ceps_svr_activation_15min
        FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
    """)

    # Create indexes for efficient querying
    op.execute("""
        CREATE INDEX idx_ceps_svr_activation_15min_trade_date
        ON finance.ceps_svr_activation_15min (trade_date);
    """)

    op.execute("""
        CREATE INDEX idx_ceps_svr_activation_15min_time_interval
        ON finance.ceps_svr_activation_15min (time_interval);
    """)

    op.execute("""
        CREATE INDEX idx_ceps_svr_activation_15min_created_at
        ON finance.ceps_svr_activation_15min (created_at);
    """)


def downgrade() -> None:
    """Drop CEPS SVR activation tables and all partitions."""

    # Drop 15min table (cascades to all partitions)
    op.execute("DROP TABLE IF EXISTS finance.ceps_svr_activation_15min CASCADE;")

    # Drop 1min table (cascades to all partitions)
    op.execute("DROP TABLE IF EXISTS finance.ceps_svr_activation_1min CASCADE;")
