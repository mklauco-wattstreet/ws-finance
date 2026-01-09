"""Add CEPS actual imbalance tables with year partitioning.

Revision ID: 027
Revises: 026
Create Date: 2026-01-07

Creates two new tables in the finance schema for CEPS (Czech electricity grid) imbalance data:
1. ceps_actual_imbalance_1min - minute-level raw data
2. ceps_actual_imbalance_15min - 15-minute aggregated data

Both tables use RANGE partitioning by year for efficient data management.
"""

from alembic import op
import sqlalchemy as sa

revision = '027'
down_revision = '026'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create CEPS imbalance tables with year-based partitioning."""

    # ========================================================================
    # Table 1: ceps_actual_imbalance_1min (minute-level raw data)
    # ========================================================================

    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_1min (
            id BIGSERIAL,
            delivery_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            load_mw NUMERIC(12,5) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (delivery_timestamp, id)
        ) PARTITION BY RANGE (delivery_timestamp);
    """)

    # Create partitions for years 2024-2028
    # Each partition covers one calendar year in Europe/Prague timezone
    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_1min_2024
        PARTITION OF finance.ceps_actual_imbalance_1min
        FOR VALUES FROM ('2024-01-01 00:00:00+01') TO ('2025-01-01 00:00:00+01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_1min_2025
        PARTITION OF finance.ceps_actual_imbalance_1min
        FOR VALUES FROM ('2025-01-01 00:00:00+01') TO ('2026-01-01 00:00:00+01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_1min_2026
        PARTITION OF finance.ceps_actual_imbalance_1min
        FOR VALUES FROM ('2026-01-01 00:00:00+01') TO ('2027-01-01 00:00:00+01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_1min_2027
        PARTITION OF finance.ceps_actual_imbalance_1min
        FOR VALUES FROM ('2027-01-01 00:00:00+01') TO ('2028-01-01 00:00:00+01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_1min_2028
        PARTITION OF finance.ceps_actual_imbalance_1min
        FOR VALUES FROM ('2028-01-01 00:00:00+01') TO ('2029-01-01 00:00:00+01');
    """)

    # Create index for efficient querying
    op.execute("""
        CREATE INDEX ix_ceps_actual_imbalance_1min_delivery_timestamp
        ON finance.ceps_actual_imbalance_1min (delivery_timestamp);
    """)

    # Add unique constraint for UPSERT operations
    op.execute("""
        ALTER TABLE finance.ceps_actual_imbalance_1min
        ADD CONSTRAINT uq_ceps_1min_delivery_timestamp UNIQUE (delivery_timestamp);
    """)

    # ========================================================================
    # Table 2: ceps_actual_imbalance_15min (15-minute aggregated data)
    # ========================================================================

    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_15min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            load_mean_mw NUMERIC(12,5),
            load_median_mw NUMERIC(12,5),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, id)
        ) PARTITION BY RANGE (trade_date);
    """)

    # Create partitions for years 2024-2028
    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_15min_2024
        PARTITION OF finance.ceps_actual_imbalance_15min
        FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_15min_2025
        PARTITION OF finance.ceps_actual_imbalance_15min
        FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_15min_2026
        PARTITION OF finance.ceps_actual_imbalance_15min
        FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_15min_2027
        PARTITION OF finance.ceps_actual_imbalance_15min
        FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_15min_2028
        PARTITION OF finance.ceps_actual_imbalance_15min
        FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
    """)

    # Create indexes for efficient querying
    op.execute("""
        CREATE INDEX ix_ceps_actual_imbalance_15min_trade_date
        ON finance.ceps_actual_imbalance_15min (trade_date);
    """)

    op.execute("""
        CREATE INDEX ix_ceps_actual_imbalance_15min_time_interval
        ON finance.ceps_actual_imbalance_15min (time_interval);
    """)

    # Add unique constraint for UPSERT operations
    op.execute("""
        ALTER TABLE finance.ceps_actual_imbalance_15min
        ADD CONSTRAINT uq_ceps_15min_trade_date_interval UNIQUE (trade_date, time_interval);
    """)


def downgrade() -> None:
    """Drop CEPS imbalance tables and all partitions."""

    # Drop 15min table (cascades to all partitions)
    op.execute("DROP TABLE IF EXISTS finance.ceps_actual_imbalance_15min CASCADE;")

    # Drop 1min table (cascades to all partitions)
    op.execute("DROP TABLE IF EXISTS finance.ceps_actual_imbalance_1min CASCADE;")
