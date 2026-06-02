"""Add ote_prices_imbalance_60min table.

Revision ID: 063
Revises: 062
Create Date: 2026-06-02

60-min mirror of the OTE-CR domestic imbalance settlement table
(ote_prices_imbalance). Adds the CZ-specific CZK/MWh prices and
imbalance volumes/costs needed at hourly granularity — distinct from
entsoe_imbalance_prices_60min (which holds the ENTSO-E per-country
EUR/MWh data).

See docs/60min_tables_plan.md §4.7 for the column-by-column rules:
  * Volumes (system_imbalance_mwh, absolute_imbalance_sum_mwh,
    positive_imbalance_mwh, negative_imbalance_mwh,
    rounded_imbalance_mwh): SUM across the 4 quarters.
  * Costs (cost_of_be_czk, cost_of_imbalance_czk): SUM.
  * Prices (settlement_price_*, price_*_component_czk_mwh,
    price_not_performed_activation_czk_mwh): MEAN — consistent with
    the rest of the 60-min set.
"""

from alembic import op


revision = '063'
down_revision = '062'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE ote_prices_imbalance_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            system_imbalance_mwh NUMERIC(12, 5),
            absolute_imbalance_sum_mwh NUMERIC(12, 5),
            positive_imbalance_mwh NUMERIC(12, 5),
            negative_imbalance_mwh NUMERIC(12, 5),
            rounded_imbalance_mwh NUMERIC(12, 5),
            cost_of_be_czk NUMERIC(15, 3),
            cost_of_imbalance_czk NUMERIC(15, 3),
            settlement_price_imbalance_czk_mwh NUMERIC(15, 3),
            settlement_price_counter_imbalance_czk_mwh NUMERIC(15, 3),
            price_protective_be_component_czk_mwh NUMERIC(15, 3),
            price_be_component_czk_mwh NUMERIC(15, 3),
            price_im_component_czk_mwh NUMERIC(15, 3),
            price_si_component_czk_mwh NUMERIC(15, 3),
            price_not_performed_activation_czk_mwh NUMERIC(15, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE (trade_date, time_interval)
        );
    """)
    op.execute("""
        CREATE INDEX ix_ote_prices_imbalance_60min_trade_date
        ON ote_prices_imbalance_60min (trade_date);
    """)
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ote_prices_imbalance_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE ote_prices_imbalance_60min_id_seq TO user_finance;")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ote_prices_imbalance_60min CASCADE;")
