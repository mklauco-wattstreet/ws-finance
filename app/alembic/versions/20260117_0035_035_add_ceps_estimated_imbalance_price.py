"""Add CEPS Estimated Imbalance Price table.

Revision ID: 035
Revises: 034
Create Date: 2026-01-17

Creates a new table in the finance schema:
- ceps_estimated_imbalance_price_15min - Estimated imbalance price (native 15-min data)

Data source: OdhadovanaCenaOdchylky SOAP API
- Single column: estimated_price_czk_mwh (Estimated price in CZK/MWh)

Uses RANGE partitioning by year.
"""

from alembic import op
import sqlalchemy as sa

revision = '035'
down_revision = '034'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create CEPS Estimated Imbalance Price table with year-based partitioning."""

    # ========================================================================
    # Table: ceps_estimated_imbalance_price_15min
    # ========================================================================

    op.execute("""
        CREATE TABLE finance.ceps_estimated_imbalance_price_15min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            estimated_price_czk_mwh NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_ceps_estimated_imbalance_price_15min_trade_date_interval UNIQUE (trade_date, time_interval)
        ) PARTITION BY RANGE (trade_date);
    """)

    # Create partitions for years 2024-2028
    op.execute("""
        CREATE TABLE finance.ceps_estimated_imbalance_price_15min_2024
        PARTITION OF finance.ceps_estimated_imbalance_price_15min
        FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_estimated_imbalance_price_15min_2025
        PARTITION OF finance.ceps_estimated_imbalance_price_15min
        FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_estimated_imbalance_price_15min_2026
        PARTITION OF finance.ceps_estimated_imbalance_price_15min
        FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_estimated_imbalance_price_15min_2027
        PARTITION OF finance.ceps_estimated_imbalance_price_15min
        FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
    """)

    op.execute("""
        CREATE TABLE finance.ceps_estimated_imbalance_price_15min_2028
        PARTITION OF finance.ceps_estimated_imbalance_price_15min
        FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
    """)

    op.execute("""
        CREATE INDEX idx_ceps_estimated_imbalance_price_15min_trade_date
        ON finance.ceps_estimated_imbalance_price_15min (trade_date);
    """)


def downgrade() -> None:
    """Drop CEPS Estimated Imbalance Price table and all partitions."""
    op.execute("DROP TABLE IF EXISTS finance.ceps_estimated_imbalance_price_15min CASCADE;")
