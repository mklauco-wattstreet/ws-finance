"""Add CEPS Generation and Generation Plan tables.

Revision ID: 034
Revises: 033
Create Date: 2026-01-16

Creates two new tables in the finance schema:
1. ceps_generation_15min - Actual generation by power plant type (15-min averages)
2. ceps_generation_plan_15min - Planned total generation (15-min data)

Power plant types:
- TPP: Thermal Power Plant
- CCGT: Combined-Cycle Gas Turbine Power Plant
- NPP: Nuclear Power Plant
- HPP: Hydro Power Plant
- PsPP: Pumped-Storage Plant
- AltPP: Alternative Power Plant
- ApPP: Autoproducer Power Plant (canceled since Oct 2014, kept for historical)
- WPP: Wind Power Plant
- PVPP: Photovoltaic Power Plant

Both tables use RANGE partitioning by year.
"""

from alembic import op
import sqlalchemy as sa

revision = '034'
down_revision = '033'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create CEPS Generation tables with year-based partitioning."""

    # ========================================================================
    # Table 1: ceps_generation_15min (actual generation by plant type)
    # ========================================================================

    op.execute("""
        CREATE TABLE finance.ceps_generation_15min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            tpp_mw NUMERIC(12,3),
            ccgt_mw NUMERIC(12,3),
            npp_mw NUMERIC(12,3),
            hpp_mw NUMERIC(12,3),
            pspp_mw NUMERIC(12,3),
            altpp_mw NUMERIC(12,3),
            appp_mw NUMERIC(12,3),
            wpp_mw NUMERIC(12,3),
            pvpp_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_ceps_generation_15min_trade_date_interval UNIQUE (trade_date, time_interval)
        ) PARTITION BY RANGE (trade_date);
    """)

    # Create partitions for years 2024-2028
    op.execute("""
        CREATE TABLE finance.ceps_generation_15min_2024
        PARTITION OF finance.ceps_generation_15min
        FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_15min_2025
        PARTITION OF finance.ceps_generation_15min
        FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_15min_2026
        PARTITION OF finance.ceps_generation_15min
        FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_15min_2027
        PARTITION OF finance.ceps_generation_15min
        FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_15min_2028
        PARTITION OF finance.ceps_generation_15min
        FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
    """)

    op.execute("""
        CREATE INDEX idx_ceps_generation_15min_trade_date
        ON finance.ceps_generation_15min (trade_date);
    """)

    # ========================================================================
    # Table 2: ceps_generation_plan_15min (planned total generation)
    # ========================================================================

    op.execute("""
        CREATE TABLE finance.ceps_generation_plan_15min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            total_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_ceps_generation_plan_15min_trade_date_interval UNIQUE (trade_date, time_interval)
        ) PARTITION BY RANGE (trade_date);
    """)

    # Create partitions for years 2024-2028
    op.execute("""
        CREATE TABLE finance.ceps_generation_plan_15min_2024
        PARTITION OF finance.ceps_generation_plan_15min
        FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_plan_15min_2025
        PARTITION OF finance.ceps_generation_plan_15min
        FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_plan_15min_2026
        PARTITION OF finance.ceps_generation_plan_15min
        FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_plan_15min_2027
        PARTITION OF finance.ceps_generation_plan_15min
        FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_generation_plan_15min_2028
        PARTITION OF finance.ceps_generation_plan_15min
        FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
    """)

    op.execute("""
        CREATE INDEX idx_ceps_generation_plan_15min_trade_date
        ON finance.ceps_generation_plan_15min (trade_date);
    """)


def downgrade() -> None:
    """Drop CEPS Generation tables and all partitions."""
    op.execute("DROP TABLE IF EXISTS finance.ceps_generation_plan_15min CASCADE;")
    op.execute("DROP TABLE IF EXISTS finance.ceps_generation_15min CASCADE;")
