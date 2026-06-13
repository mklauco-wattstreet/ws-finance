"""Rename mislabeled CEPS imbalance 'load_*' columns to 'system_imbalance_*'.

Revision ID: 067
Revises: 066
Create Date: 2026-06-13

The finance.ceps_actual_imbalance_* tables store the CEPS system imbalance
(ACE), fetched from the SOAP operation AktualniSystemovaOdchylkaCR
("Aktuální systémová odchylka ČR", series "Aktuální odchylka [MW]"). The value
columns were mistakenly named load_*. This is NOT load: the series is signed
(mean ~ -31 MW, range -1000..+480) and correlates 0.978 with the OTE settled
system imbalance; CZ load is strictly positive at ~5000-9000 MW.

This migration only renames columns (data and partitions are untouched).

Sign convention (documented as column comments):
  + (positive) = system surplus / long  (generation > consumption vs schedule)
  - (negative) = system deficit / short  (generation < consumption vs schedule)
Source: CEPS AktualniSystemovaOdchylkaCR, unit MW.
"""

from alembic import op

revision = '067'
down_revision = '066'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ceps_actual_imbalance_1min
    op.execute("ALTER TABLE finance.ceps_actual_imbalance_1min RENAME COLUMN load_mw TO system_imbalance_mw;")

    # ceps_actual_imbalance_15min
    op.execute("ALTER TABLE finance.ceps_actual_imbalance_15min RENAME COLUMN load_mean_mw TO system_imbalance_mean_mw;")
    op.execute("ALTER TABLE finance.ceps_actual_imbalance_15min RENAME COLUMN load_median_mw TO system_imbalance_median_mw;")
    op.execute("ALTER TABLE finance.ceps_actual_imbalance_15min RENAME COLUMN last_load_at_interval_mw TO system_imbalance_last_mw;")

    # ceps_actual_imbalance_60min
    op.execute("ALTER TABLE finance.ceps_actual_imbalance_60min RENAME COLUMN load_mean_mw TO system_imbalance_mean_mw;")
    op.execute("ALTER TABLE finance.ceps_actual_imbalance_60min RENAME COLUMN load_median_mw TO system_imbalance_median_mw;")

    # Document the quantity and sign convention on the source column.
    op.execute(
        "COMMENT ON COLUMN finance.ceps_actual_imbalance_1min.system_imbalance_mw IS "
        "'CEPS system imbalance / ACE [MW] from AktualniSystemovaOdchylkaCR. "
        "Sign: + = surplus/long, - = deficit/short.';"
    )
    op.execute(
        "COMMENT ON COLUMN finance.ceps_actual_imbalance_15min.system_imbalance_mean_mw IS "
        "'Mean CEPS system imbalance [MW] over the 15-min interval. + = surplus/long, - = deficit/short.';"
    )
    op.execute(
        "COMMENT ON COLUMN finance.ceps_actual_imbalance_60min.system_imbalance_mean_mw IS "
        "'Mean CEPS system imbalance [MW] over the hour. + = surplus/long, - = deficit/short.';"
    )


def downgrade() -> None:
    op.execute("COMMENT ON COLUMN finance.ceps_actual_imbalance_60min.system_imbalance_mean_mw IS NULL;")
    op.execute("COMMENT ON COLUMN finance.ceps_actual_imbalance_15min.system_imbalance_mean_mw IS NULL;")
    op.execute("COMMENT ON COLUMN finance.ceps_actual_imbalance_1min.system_imbalance_mw IS NULL;")

    # ceps_actual_imbalance_60min
    op.execute("ALTER TABLE finance.ceps_actual_imbalance_60min RENAME COLUMN system_imbalance_median_mw TO load_median_mw;")
    op.execute("ALTER TABLE finance.ceps_actual_imbalance_60min RENAME COLUMN system_imbalance_mean_mw TO load_mean_mw;")

    # ceps_actual_imbalance_15min
    op.execute("ALTER TABLE finance.ceps_actual_imbalance_15min RENAME COLUMN system_imbalance_last_mw TO last_load_at_interval_mw;")
    op.execute("ALTER TABLE finance.ceps_actual_imbalance_15min RENAME COLUMN system_imbalance_median_mw TO load_median_mw;")
    op.execute("ALTER TABLE finance.ceps_actual_imbalance_15min RENAME COLUMN system_imbalance_mean_mw TO load_mean_mw;")

    # ceps_actual_imbalance_1min
    op.execute("ALTER TABLE finance.ceps_actual_imbalance_1min RENAME COLUMN system_imbalance_mw TO load_mw;")
