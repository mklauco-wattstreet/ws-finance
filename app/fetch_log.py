"""
Fetch log - one finance.fetch_log row per run of a source dataset (migration 085).

Whole script (status from the exit code, error from the last ERROR log line):
    if __name__ == '__main__':
        fetch_log.run_logged("ote", "day_ahead_prices", main)

Finer-grained (one row per report of a multi-report run):
    run = fetch_log.start("ote", "intraday_limit", dry_run=False, data_from=day, data_to=day + 1)
    ...
    fetch_log.finish(run, "ok")            # or "failed" / "skipped", error="..."

Logging never breaks a pipeline: every DB error is caught and logged as a
warning, start() then returns None and finish(None, ...) is a no-op. Each write
uses its own short autocommit connection, so a failed data transaction in the
caller cannot roll back the log row.

trigger: FETCH_TRIGGER env var (the OTE refresh API sets "api"); otherwise
"manual" when run from a terminal, "cron" when not.

A row whose finished_at stays NULL long after started_at is a run that was
killed (OOM, container restart, the API's hard kill) before it could finish.
"""

import logging
import os
import sys
from datetime import date, datetime, time
from typing import Callable, Optional
from zoneinfo import ZoneInfo

import psycopg2

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA

PRAGUE = ZoneInfo("Europe/Prague")
ERROR_MAX_CHARS = 2000

logger = logging.getLogger("fetch_log")


def _trigger() -> str:
    env = os.environ.get("FETCH_TRIGGER")
    if env in ("cron", "api", "manual"):
        return env
    return "manual" if sys.stdin.isatty() else "cron"


def _ts(value):
    """A date becomes Prague midnight; datetimes pass through."""
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, time(), tzinfo=PRAGUE)
    return value


def _execute(sql: str, params: tuple):
    conn = psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, port=DB_PORT, connect_timeout=5,
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {DB_SCHEMA}")
            cur.execute(sql, params)
            return cur.fetchone() if cur.description else None
    finally:
        conn.close()


def start(source: str, dataset: str, *, dry_run: bool = False, country_code: Optional[str] = None,
          data_from=None, data_to=None) -> Optional[int]:
    """Insert a running row; returns its id, or None if the log is unavailable."""
    args = " ".join(sys.argv[1:]) or None
    try:
        row = _execute(
            "INSERT INTO fetch_log (source, dataset, country_code, trigger, dry_run, args, data_from, data_to) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (source, dataset, country_code, _trigger(), dry_run, args, _ts(data_from), _ts(data_to)),
        )
        return row[0]
    except Exception as e:
        logger.warning(f"fetch_log: could not record start of {source}/{dataset}: {e}")
        return None


def finish(run_id: Optional[int], status: str, error: Optional[str] = None,
           rows_inserted: Optional[int] = None, rows_updated: Optional[int] = None):
    """Close a row with ok | failed | skipped. No-op for run_id None."""
    if run_id is None:
        return
    if error and len(error) > ERROR_MAX_CHARS:
        error = error[:ERROR_MAX_CHARS] + "..."
    try:
        _execute(
            "UPDATE fetch_log SET finished_at = CURRENT_TIMESTAMP, status = %s, error = %s, "
            "rows_inserted = %s, rows_updated = %s WHERE id = %s",
            (status, error, rows_inserted, rows_updated, run_id),
        )
    except Exception as e:
        logger.warning(f"fetch_log: could not record finish of run {run_id}: {e}")


class _LastError(logging.Handler):
    """Remembers the last ERROR record so a bare sys.exit(1) still logs a reason."""

    def __init__(self):
        super().__init__(level=logging.ERROR)
        self.last: Optional[str] = None

    def emit(self, record):
        self.last = record.getMessage()


def run_logged(source: str, dataset: str, main_fn: Callable[[], object]):
    """Run a script's main() as one fetch_log row: exit code 0 / None = ok, else failed."""
    run_id = start(source, dataset, dry_run="--dry-run" in sys.argv[1:])
    capture = _LastError()
    logging.getLogger().addHandler(capture)
    status, error = "failed", None
    try:
        main_fn()
        status = "ok"
    except SystemExit as e:
        if e.code in (None, 0):
            status = "ok"
        else:
            error = f"exit code {e.code}"
            if capture.last:
                error += f": {capture.last}"
        raise
    except BaseException as e:
        error = f"{type(e).__name__}: {e}"
        raise
    finally:
        logging.getLogger().removeHandler(capture)
        finish(run_id, status, error)
