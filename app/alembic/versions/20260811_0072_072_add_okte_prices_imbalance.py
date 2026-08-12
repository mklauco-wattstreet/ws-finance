"""Add okte_prices_imbalance - Slovak imbalance settlement (regulardaily).

Revision ID: 072
Revises: 071
Create Date: 2026-08-11

SK counterpart to ote_prices_imbalance, sourced from the public ISZO endpoint:

    GET https://iszo.okte.sk/api/v1/SystemImbalance
        ?dateFrom=&dateTo=&evaluationType=regulardaily

Note the host is iszo.okte.sk (imbalance settlement system), NOT isot.okte.sk
(short-term market, used by the IDM tables in migration 071).

SCOPE: regulardaily ONLY. OKTE republishes the same delivery day as
preliminarydaily (D+1), regulardaily (D+2), decadal, monthly and final, with
differing values. Storing one vintage keeps (trade_date, period) unique.

NAMING: columns follow ote_prices_imbalance, not OKTE's API codes, so the two
tables read the same way. system_imbalance_mwh, positive_imbalance_mwh,
negative_imbalance_mwh are identical to their CZ counterparts;
settlement_price_imbalance_eur_mwh and cost_of_imbalance_eur mirror the CZK
versions. Every column carries its unit. The OKTE code each one comes from is
noted inline for traceability back to the API.

CURRENCY: EUR (the Czech table is CZK) - hence the _eur / _eur_mwh suffixes.

DELIBERATELY NOT STORED. The API returns ~36 period fields; this table keeps
only the imbalance price/volume signal, matching the CZ table's scope:
  - settlement bookkeeping: sip, oip, balance, psrec
  - contracted/metered energy: tcc, tcs, tmc, tmc_ab, tmc_c, tms, tms_ab, tms_c
  - balanced-from-top/bottom aggregates: *_sbb, *_sbt
  - day-level coefficients: srec (coefficient), kzpo
  - discontinued fields: tacre (2012), pckre/pczre (2013),
    mppre/mpnre (ended 30 Jun 2024, superseded by sipr from 1 Jul 2024)
All remain retrievable from the API if ever needed - re-fetching is cheap.

Note `srec` appears at BOTH day and period level in the API with different
meanings (day = coefficient, period = cost in EUR/MWh). Only the period-level
cost is stored here.
"""

from alembic import op


revision = '072'
down_revision = '071'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE okte_prices_imbalance (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,

            -- provenance: regulardaily lands at D+2; evaluation_date makes a
            -- restatement visible without a second vintage row
            evaluation_date DATE,
            emergency BOOLEAN,

            -- prices (EUR/MWh)
            settlement_price_imbalance_eur_mwh NUMERIC(15, 4),          -- okte: isp
            system_imbalance_price_eur_mwh NUMERIC(15, 4),              -- okte: sipr (since 1 Jul 2024)
            shared_regulation_electricity_cost_eur_mwh NUMERIC(15, 4),  -- okte: srec
            shared_regulation_electricity_price_eur_mwh NUMERIC(15, 4), -- okte: srecp

            -- imbalance volumes (MWh) - names identical to ote_prices_imbalance
            system_imbalance_mwh NUMERIC(15, 4),                        -- okte: si
            positive_imbalance_mwh NUMERIC(15, 4),                      -- okte: pi
            negative_imbalance_mwh NUMERIC(15, 4),                      -- okte: ni

            -- imbalance payment (EUR) - mirrors cost_of_imbalance_czk
            cost_of_imbalance_eur NUMERIC(18, 4),                       -- okte: psi

            -- regulation electricity volumes (MWh) and payments (EUR)
            positive_regulation_electricity_mwh NUMERIC(15, 4),         -- okte: pre
            negative_regulation_electricity_mwh NUMERIC(15, 4),         -- okte: nre
            payment_positive_regulation_electricity_eur NUMERIC(18, 4), -- okte: pspre
            payment_negative_regulation_electricity_eur NUMERIC(18, 4), -- okte: psnre
            total_regulation_electricity_mwh NUMERIC(15, 4),            -- okte: tre
            total_cost_regulation_electricity_eur NUMERIC(18, 4),       -- okte: tcre

            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            CONSTRAINT okte_prices_imbalance_trade_date_period_key UNIQUE (trade_date, period)
        );
    """)
    op.execute(
        "CREATE INDEX ix_okte_prices_imbalance_trade_date "
        "ON okte_prices_imbalance (trade_date);"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE okte_prices_imbalance "
        "TO user_finance;"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE okte_prices_imbalance_id_seq "
        "TO user_finance;"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS okte_prices_imbalance CASCADE;")
