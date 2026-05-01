"""Rebuild da_curve_depth with wall-detection schema.

Revision ID: 055
Revises: 054
Create Date: 2026-05-01

Replaces MW-offset sampling with largest-price-jump (wall) detection in
four directions from the clearing price: sell_up, sell_down, buy_down, buy_up.

- Renames existing finance.da_curve_depth -> finance.da_curve_depth_legacy_offset_mw
  (legacy table retained until backfill is verified).
- Creates new finance.da_curve_depth keyed on (delivery_date, period) with 12
  wall fields (mw_from_clearing, price_from_clearing, slope per direction)
  plus clearing_price.
- Adds index on (delivery_date, time_interval) for downstream join performance.
"""

from alembic import op

revision = '055'
down_revision = '054'
branch_labels = None
depends_on = None

SCHEMA = 'finance'


def upgrade() -> None:
    op.execute(f"ALTER TABLE {SCHEMA}.da_curve_depth RENAME TO da_curve_depth_legacy_offset_mw;")
    op.execute(f"ALTER INDEX {SCHEMA}.idx_da_curve_depth_date RENAME TO idx_da_curve_depth_legacy_date;")
    op.execute(f"ALTER INDEX {SCHEMA}.idx_da_curve_depth_date_period RENAME TO idx_da_curve_depth_legacy_date_period;")
    op.execute(f"ALTER TABLE {SCHEMA}.da_curve_depth_legacy_offset_mw RENAME CONSTRAINT da_curve_depth_pkey TO da_curve_depth_legacy_offset_mw_pkey;")

    op.execute(f"""
        CREATE TABLE {SCHEMA}.da_curve_depth (
            delivery_date  DATE           NOT NULL,
            period         INTEGER        NOT NULL,
            time_interval  VARCHAR(11)    NOT NULL,
            clearing_price NUMERIC(10, 2) NOT NULL,

            sell_up_mw_from_clearing      NUMERIC(12, 3),
            sell_up_price_from_clearing   NUMERIC(10, 2),
            sell_up_slope                 NUMERIC(10, 4),

            sell_down_mw_from_clearing    NUMERIC(12, 3),
            sell_down_price_from_clearing NUMERIC(10, 2),
            sell_down_slope               NUMERIC(10, 4),

            buy_down_mw_from_clearing     NUMERIC(12, 3),
            buy_down_price_from_clearing  NUMERIC(10, 2),
            buy_down_slope                NUMERIC(10, 4),

            buy_up_mw_from_clearing       NUMERIC(12, 3),
            buy_up_price_from_clearing    NUMERIC(10, 2),
            buy_up_slope                  NUMERIC(10, 4),

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT da_curve_depth_pkey PRIMARY KEY (delivery_date, period)
        );
    """)
    op.execute(f"CREATE INDEX idx_da_curve_depth_date_interval ON {SCHEMA}.da_curve_depth (delivery_date, time_interval);")


def downgrade() -> None:
    op.execute(f"DROP INDEX IF EXISTS {SCHEMA}.idx_da_curve_depth_date_interval;")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.da_curve_depth;")

    op.execute(f"ALTER TABLE {SCHEMA}.da_curve_depth_legacy_offset_mw RENAME CONSTRAINT da_curve_depth_legacy_offset_mw_pkey TO da_curve_depth_pkey;")
    op.execute(f"ALTER INDEX {SCHEMA}.idx_da_curve_depth_legacy_date_period RENAME TO idx_da_curve_depth_date_period;")
    op.execute(f"ALTER INDEX {SCHEMA}.idx_da_curve_depth_legacy_date RENAME TO idx_da_curve_depth_date;")
    op.execute(f"ALTER TABLE {SCHEMA}.da_curve_depth_legacy_offset_mw RENAME TO da_curve_depth;")
