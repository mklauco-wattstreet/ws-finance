"""Add fetch_log: one row per run of a source dataset.

Revision ID: 085
Revises: 084
Create Date: 2026-10-26

The data tables cannot tell when a source was last fetched: uploads only touch
updated_at when a value changes, and created_at only moves when a fetch brings
new rows. fetch_log records every run instead, written by app/fetch_log.py.

Scope at creation: the OTE-CR cron scripts only (source 'ote'). Other sources
can log into the same table later without a schema change.

- dataset: the existing name of what was fetched - the download script's name
  without "download_" (day_ahead_prices, dam_curves, ...), daily_payments, or
  the portal-session report key (trade_balance, intraday_limit, fs_trend).
- trigger: cron | api (OTE refresh API) | manual (run from a terminal).
- status: NULL while running; ok | failed | skipped when finished. A row whose
  finished_at stays NULL long after started_at is a run that was killed
  (OOM, container restart, the API's hard kill) before it could finish.
- data_from / data_to: requested window, half-open [from, to), when known.
- rows_inserted / rows_updated: NULL when the uploader does not report them
  (the OTE uploaders run as subprocesses and do not).
"""

from alembic import op


revision = '085'
down_revision = '084'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE fetch_log (
            id BIGSERIAL,
            source VARCHAR(20) NOT NULL,
            dataset VARCHAR(50) NOT NULL,
            country_code VARCHAR(2),
            trigger VARCHAR(10) NOT NULL,
            dry_run BOOLEAN NOT NULL DEFAULT FALSE,
            args TEXT,
            started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMPTZ,
            status VARCHAR(10),
            rows_inserted INTEGER,
            rows_updated INTEGER,
            data_from TIMESTAMPTZ,
            data_to TIMESTAMPTZ,
            error TEXT,
            PRIMARY KEY (id),
            CONSTRAINT fetch_log_trigger_check CHECK (trigger IN ('cron', 'api', 'manual')),
            CONSTRAINT fetch_log_status_check CHECK (status IN ('ok', 'failed', 'skipped'))
        );
    """)
    op.execute(
        "CREATE INDEX ix_fetch_log_source_dataset_started_at "
        "ON fetch_log (source, dataset, started_at DESC);"
    )
    op.execute(
        "COMMENT ON TABLE fetch_log IS "
        "'One row per run of a source dataset (app/fetch_log.py). status NULL = running, "
        "or killed if finished_at stays NULL.';"
    )
    op.execute(
        "COMMENT ON COLUMN fetch_log.dataset IS "
        "'Existing name of what was fetched: download script name without download_, "
        "daily_payments, or portal-session report key.';"
    )
    op.execute(
        "COMMENT ON COLUMN fetch_log.trigger IS "
        "'cron | api (OTE refresh API) | manual (terminal).';"
    )
    op.execute(
        "COMMENT ON COLUMN fetch_log.args IS "
        "'Command-line arguments of the run (e.g. --ida 2).';"
    )
    op.execute(
        "COMMENT ON COLUMN fetch_log.data_from IS "
        "'Requested window start, half-open [data_from, data_to), when known.';"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE fetch_log TO user_finance;"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE fetch_log_id_seq TO user_finance;"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS fetch_log CASCADE;")
