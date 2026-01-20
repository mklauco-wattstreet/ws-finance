"""Add CEPS Generation RES tables with year partitioning.

Revision ID: 033
Revises: 032
Create Date: 2026-01-16

Creates two new tables in the finance schema for CEPS renewable energy generation data:
1. ceps_generation_res_1min - minute-level raw generation data
2. ceps_generation_res_15min - 15-minute aggregated generation statistics

Both tables use RANGE partitioning by year for efficient data management.
Generation data includes VTE (wind) and FVE (solar/photovoltaic).
"""

from alembic import op
import sqlalchemy as sa

revision = '033'
down_revision = '032'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create CEPS Generation RES tables with year-based partitioning."""

    # ========================================================================
    # Table 1: ceps_generation_res_1min (minute-level raw generation data)
    # ========================================================================

    op.execute("""
        CREATE TABLE finance.ceps_generation_res_1min (
            id BIGSERIAL,
            delivery_timestamp TIMESTAMP NOT NULL,
            wind_mw NUMERIC(12,3),
            solar_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_ceps_generation_res_1min_delivery_timestamp UNIQUE (delivery_timestamp)
        ) PARTITION BY RANGE (delivery_timestamp);
    """)

    # Create partitions for years 2024-2028
    op.execute("""
        CREATE TABLE finance.ceps_generation_res_1min_2024
        PARTITION OF finance.ceps_generation_res_1min
        FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2025-01-01 00:00:00');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_res_1min_2025
        PARTITION OF finance.ceps_generation_res_1min
        FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2026-01-01 00:00:00');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_res_1min_2026
        PARTITION OF finance.ceps_generation_res_1min
        FOR VALUES FROM ('2026-01-01 00:00:00') TO ('2027-01-01 00:00:00');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_res_1min_2027
        PARTITION OF finance.ceps_generation_res_1min
        FOR VALUES FROM ('2027-01-01 00:00:00') TO ('2028-01-01 00:00:00');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_res_1min_2028
        PARTITION OF finance.ceps_generation_res_1min
        FOR VALUES FROM ('2028-01-01 00:00:00') TO ('2029-01-01 00:00:00');
    """)

    op.execute("""
        CREATE INDEX idx_ceps_generation_res_1min_delivery_timestamp
        ON finance.ceps_generation_res_1min (delivery_timestamp);
    """)

    # ========================================================================
    # Table 2: ceps_generation_res_15min (15-minute aggregated generation)
    # ========================================================================

    op.execute("""
        CREATE TABLE finance.ceps_generation_res_15min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            wind_mean_mw NUMERIC(12,3),
            wind_median_mw NUMERIC(12,3),
            wind_last_at_interval_mw NUMERIC(12,3),
            solar_mean_mw NUMERIC(12,3),
            solar_median_mw NUMERIC(12,3),
            solar_last_at_interval_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_ceps_generation_res_15min_trade_date_interval UNIQUE (trade_date, time_interval)
        ) PARTITION BY RANGE (trade_date);
    """)

    # Create partitions for years 2024-2028
    op.execute("""
        CREATE TABLE finance.ceps_generation_res_15min_2024
        PARTITION OF finance.ceps_generation_res_15min
        FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_res_15min_2025
        PARTITION OF finance.ceps_generation_res_15min
        FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_res_15min_2026
        PARTITION OF finance.ceps_generation_res_15min
        FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_res_15min_2027
        PARTITION OF finance.ceps_generation_res_15min
        FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_res_15min_2028
        PARTITION OF finance.ceps_generation_res_15min
        FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
    """)

    op.execute("""
        CREATE INDEX idx_ceps_generation_res_15min_trade_date
        ON finance.ceps_generation_res_15min (trade_date);
    """)


def downgrade() -> None:
    """Drop CEPS Generation RES tables and all partitions."""
    op.execute("DROP TABLE IF EXISTS finance.ceps_generation_res_15min CASCADE;")
    op.execute("DROP TABLE IF EXISTS finance.ceps_generation_res_1min CASCADE;")
