"""Add CEPS cross-border tables (CrossborderPowerFlows, EmergencyExchange).

Revision ID: 091
Revises: 090
Create Date: 2026-11-20

Plan: docs/cross_border_plan.md section 2.2, CEPS row. Three new tables in
schema finance, built like their ceps_export_import_svr_* siblings after
migrations 068/069/084: TIMESTAMPTZ audit columns, delivery_datetime (UTC
instant) in the unique key, RANGE partitions per year 2024-2028. The primary key
(partition column, [time_interval,] id) mirrors the models of the sibling tables.

  ceps_crossborder_flows_1min   dataset crossborder_flows (SOAP CrossborderPowerFlows,
                                1-min, RANGE by delivery_timestamp). Actual and
                                planned flow per border profile PSE (PL), SEPS (SK),
                                APG (AT), TenneT (DE), 50HzT (DE) and CEPS (their
                                sum). Positive = import into CZ, negative = export.
  ceps_crossborder_flows_15min  the same, aggregated to 15 min by the uploader
                                (mean / median / last_at_interval per column),
                                RANGE by trade_date.
  ceps_emergency_exchange_60min dataset emergency_exchange (SOAP EmergencyExchange).
                                Hourly, long format: one row per hour, border and
                                direction (e.g. CAD_CEPS_50HZT) and exchange type.
                                RANGE by trade_date.

delivery_instant (generated column of the older 1-min tables) is not created:
delivery_datetime is the instant and is correct on the fall-back day.

Downgrade drops exactly these three tables (and their partitions).
"""

from alembic import op


revision = '091'
down_revision = '090'
branch_labels = None
depends_on = None

YEARS = (2024, 2025, 2026, 2027, 2028)

_BORDERS = ('pse', 'seps', 'apg', 'tennet', 'fiftyhertz', 'ceps')
_FLOW_COLUMNS = [f'{b}_{k}' for b in _BORDERS for k in ('actual', 'planned')]


def _partitions(table: str, minute: bool) -> None:
    for year in YEARS:
        lo = f"'{year}-01-01 00:00:00'" if minute else f"'{year}-01-01'"
        hi = f"'{year + 1}-01-01 00:00:00'" if minute else f"'{year + 1}-01-01'"
        op.execute(f"""
            CREATE TABLE finance.{table}_{year}
            PARTITION OF finance.{table}
            FOR VALUES FROM ({lo}) TO ({hi});
        """)


def upgrade() -> None:
    # ---- 1-min raw flows ----------------------------------------------------
    cols_1min = ",\n            ".join(f"{c}_mw NUMERIC(15,5)" for c in _FLOW_COLUMNS)
    op.execute(f"""
        CREATE TABLE finance.ceps_crossborder_flows_1min (
            id BIGSERIAL,
            delivery_timestamp TIMESTAMP NOT NULL,
            delivery_datetime TIMESTAMPTZ NOT NULL,
            {cols_1min},
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT ceps_crossborder_flows_1min_pkey PRIMARY KEY (delivery_timestamp, id),
            CONSTRAINT ceps_crossborder_flows_1min_delivery_datetime_key
                UNIQUE (delivery_datetime, delivery_timestamp)
        ) PARTITION BY RANGE (delivery_timestamp);
    """)
    _partitions('ceps_crossborder_flows_1min', minute=True)
    op.execute("CREATE INDEX idx_ceps_crossborder_flows_1min_delivery_timestamp "
               "ON finance.ceps_crossborder_flows_1min (delivery_timestamp);")
    op.execute("CREATE INDEX idx_ceps_crossborder_flows_1min_created_at "
               "ON finance.ceps_crossborder_flows_1min (created_at);")
    op.execute("COMMENT ON TABLE finance.ceps_crossborder_flows_1min IS "
               "'CEPS CrossborderPowerFlows, 1-min. Positive = import into CZ, negative = export. "
               "*_planned_mw is an hourly step function repeated on every minute. "
               "ceps_*_mw is the sum of the five border profiles.';")

    # ---- 15-min aggregate ---------------------------------------------------
    cols_15min = ",\n            ".join(
        f"{c}_{stat}_mw NUMERIC(15,5)"
        for c in _FLOW_COLUMNS for stat in ('mean', 'median', 'last_at_interval')
    )
    op.execute(f"""
        CREATE TABLE finance.ceps_crossborder_flows_15min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            delivery_datetime TIMESTAMPTZ NOT NULL,
            {cols_15min},
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT ceps_crossborder_flows_15min_pkey PRIMARY KEY (trade_date, time_interval, id),
            CONSTRAINT ceps_crossborder_flows_15min_delivery_datetime_key
                UNIQUE (delivery_datetime, trade_date)
        ) PARTITION BY RANGE (trade_date);
    """)
    _partitions('ceps_crossborder_flows_15min', minute=False)
    op.execute("CREATE INDEX idx_ceps_crossborder_flows_15min_trade_date "
               "ON finance.ceps_crossborder_flows_15min (trade_date);")
    op.execute("CREATE INDEX idx_ceps_crossborder_flows_15min_time_interval "
               "ON finance.ceps_crossborder_flows_15min (time_interval);")
    op.execute("CREATE INDEX idx_ceps_crossborder_flows_15min_created_at "
               "ON finance.ceps_crossborder_flows_15min (created_at);")

    # ---- emergency exchange (hourly, long format) ---------------------------
    op.execute("""
        CREATE TABLE finance.ceps_emergency_exchange_60min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            delivery_datetime TIMESTAMPTZ NOT NULL,
            border_direction VARCHAR(40) NOT NULL,
            exchange_type VARCHAR(10) NOT NULL,
            energy_mwh NUMERIC(15,5),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT ceps_emergency_exchange_60min_pkey PRIMARY KEY (trade_date, time_interval, id),
            CONSTRAINT ceps_emergency_exchange_60min_delivery_datetime_key
                UNIQUE (delivery_datetime, trade_date, border_direction, exchange_type)
        ) PARTITION BY RANGE (trade_date);
    """)
    _partitions('ceps_emergency_exchange_60min', minute=False)
    op.execute("CREATE INDEX idx_ceps_emergency_exchange_60min_trade_date "
               "ON finance.ceps_emergency_exchange_60min (trade_date);")
    op.execute("CREATE INDEX idx_ceps_emergency_exchange_60min_created_at "
               "ON finance.ceps_emergency_exchange_60min (created_at);")
    op.execute("COMMENT ON TABLE finance.ceps_emergency_exchange_60min IS "
               "'CEPS EmergencyExchange summed per hour (agregation HR, function SUM). "
               "border_direction like CAD_CEPS_50HZT (CEPS to 50Hertz); exchange_type R. "
               "energy_mwh = sum of the 15-min values of the hour, unit as labelled by CEPS (MW / MWh).';")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS finance.ceps_emergency_exchange_60min;")
    op.execute("DROP TABLE IF EXISTS finance.ceps_crossborder_flows_15min;")
    op.execute("DROP TABLE IF EXISTS finance.ceps_crossborder_flows_1min;")
