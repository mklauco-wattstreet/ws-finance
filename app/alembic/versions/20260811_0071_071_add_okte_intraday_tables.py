"""Add OKTE intraday market order-book tables (15-min MW + hourly MWh).

Revision ID: 071
Revises: 070
Create Date: 2026-08-11

OKTE (the Slovak power exchange) publishes its intraday continuous market as
TWO SEPARATE order books, not one aggregated at two resolutions:

  * okte_prices_intraday_market        - quarter-hourly product, quoted in MW
  * okte_prices_intraday_market_60min  - hourly product, quoted in MWh

The 60min table is NOT a rollup of the 15min table (no aggregation logic,
views, or triggers are created here) - it is its own independently-traded
product with its own order flow, hence the _mw vs _mwh column suffix split.

Audit columns are TIMESTAMPTZ from the start (this repo finished moving
ENTSO-E/CEPS audit columns off naive timestamp in migrations 068-070; no new
table should reintroduce a naive column).
"""

from alembic import op


revision = '071'
down_revision = '070'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # okte_prices_intraday_market - quarter-hourly product (MW)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE okte_prices_intraday_market (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(20) NOT NULL,
            buy_orders_mw NUMERIC(14, 4),
            sell_orders_mw NUMERIC(14, 4),
            buy_trades_mw NUMERIC(14, 4),
            sell_trades_mw NUMERIC(14, 4),
            traded_quantity_diff_mw NUMERIC(14, 4),
            avg_price_eur_mwh NUMERIC(15, 3),
            weighted_avg_price_eur_mwh NUMERIC(15, 3),
            min_price_eur_mwh NUMERIC(15, 3),
            max_price_eur_mwh NUMERIC(15, 3),
            last_price_eur_mwh NUMERIC(15, 3),
            total_traded_quantity_mw NUMERIC(14, 4),
            simple_orders_vwap_eur_mwh NUMERIC(15, 3),
            simple_orders_quantity_mw NUMERIC(14, 4),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            CONSTRAINT okte_prices_intraday_market_trade_date_period_key UNIQUE (trade_date, period)
        );
    """)
    op.execute(
        "CREATE INDEX ix_okte_prices_intraday_market_trade_date "
        "ON okte_prices_intraday_market (trade_date);"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE okte_prices_intraday_market "
        "TO user_finance;"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE okte_prices_intraday_market_id_seq "
        "TO user_finance;"
    )

    # ------------------------------------------------------------------
    # okte_prices_intraday_market_60min - hourly product (MWh)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE okte_prices_intraday_market_60min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(20) NOT NULL,
            buy_orders_mwh NUMERIC(14, 4),
            sell_orders_mwh NUMERIC(14, 4),
            buy_trades_mwh NUMERIC(14, 4),
            sell_trades_mwh NUMERIC(14, 4),
            traded_quantity_diff_mwh NUMERIC(14, 4),
            avg_price_eur_mwh NUMERIC(15, 3),
            weighted_avg_price_eur_mwh NUMERIC(15, 3),
            min_price_eur_mwh NUMERIC(15, 3),
            max_price_eur_mwh NUMERIC(15, 3),
            last_price_eur_mwh NUMERIC(15, 3),
            total_traded_quantity_mwh NUMERIC(14, 4),
            simple_orders_vwap_eur_mwh NUMERIC(15, 3),
            simple_orders_quantity_mwh NUMERIC(14, 4),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            CONSTRAINT okte_prices_intraday_market_60min_trade_date_period_key UNIQUE (trade_date, period)
        );
    """)
    op.execute(
        "CREATE INDEX ix_okte_prices_intraday_market_60min_trade_date "
        "ON okte_prices_intraday_market_60min (trade_date);"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE okte_prices_intraday_market_60min "
        "TO user_finance;"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE okte_prices_intraday_market_60min_id_seq "
        "TO user_finance;"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS okte_prices_intraday_market_60min CASCADE;")
    op.execute("DROP TABLE IF EXISTS okte_prices_intraday_market CASCADE;")
