#!/usr/bin/env python3
"""
Base runner module providing shared functionality for all ENTSO-E data runners.

Features:
- Database connection with context manager
- Bulk upsert via execute_values()
- Logging setup
- Dry-run support
- XML file saving
- Backfill support with automatic 7-day chunking
"""

import sentry_init  # noqa: F401 - must be first to capture errors
sentry_init.set_module("entsoe")
import sys
import re
import logging
import argparse
import traceback as _traceback
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Generator
import zoneinfo

import psycopg2
from psycopg2 import extras

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA

# Prague timezone for date conversions
PRAGUE_TZ = zoneinfo.ZoneInfo("Europe/Prague")

# Server-side guard against a connection holding an open transaction while it
# waits on something external. Generous on purpose: it must never fire on
# healthy work, only on a genuinely wedged session.
IDLE_TX_TIMEOUT_MS = 15 * 60 * 1000  # 15 minutes


# ENTSO-E request URLs carry the API token as a query parameter. urllib3 logs
# the full URL in its "Retrying (...)" message at WARNING level, so raising the
# urllib3 logger to WARNING does NOT suppress it - the token reached the logs in
# cleartext. Redact at the handler instead, so no library can leak it.
_SECRET_QS_RE = re.compile(r"(securityToken=)[^&\s'\"<>]+", re.IGNORECASE)


class SecretRedactingFilter(logging.Filter):
    """Strip API tokens out of log records before any handler formats them.

    Attached to HANDLERS rather than loggers on purpose: a filter on a logger
    only sees records logged directly to it, never records propagated up from
    child loggers such as urllib3. A handler filter sees everything it emits.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            # Never let redaction break logging itself.
            return True
        redacted = _SECRET_QS_RE.sub(r"\1***", message)
        if redacted != message:
            # Collapse msg+args into the redacted text: the token usually sits
            # in record.args (urllib3 passes the URL as a format argument), so
            # rewriting record.msg alone would not catch it.
            record.msg = redacted
            record.args = None
        # Exception text is rendered by the FORMATTER, which runs after
        # filters - so record.exc_text is still empty here on the first pass
        # and a traceback carrying the token would slip through. Render it
        # ourselves, redacted; the formatter then reuses this cached value.
        if record.exc_info and not getattr(record, "exc_text", None):
            try:
                rendered = "".join(_traceback.format_exception(*record.exc_info))
                if rendered.endswith("\n"):
                    rendered = rendered[:-1]
                record.exc_text = _SECRET_QS_RE.sub(r"\1***", rendered)
            except Exception:
                pass
        elif getattr(record, "exc_text", None):
            record.exc_text = _SECRET_QS_RE.sub(r"\1***", record.exc_text)
        return True


class BaseRunner(ABC):
    """Base class for all ENTSO-E data runners.

    Provides common functionality for:
    - Database connections
    - Bulk upserts
    - Logging
    - Time range calculations
    - XML file management
    - Backfill with automatic 7-day chunking
    """

    # Override in subclasses
    RUNNER_NAME = "BaseRunner"
    # Store XML files in downloads volume (separate from code)
    # Container path: /app/downloads/entsoe (mounted from ./downloads)
    DATA_DIR = Path("/app/downloads/entsoe")

    # Maximum chunk size for API requests (ENTSO-E limit)
    MAX_CHUNK_DAYS = 7

    def __init__(
        self,
        debug: bool = False,
        dry_run: bool = False,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        max_runtime: Optional[int] = None
    ):
        """
        Initialize runner.

        Args:
            debug: Enable debug logging
            dry_run: Fetch and parse but don't upload
            start_date: Optional start date for backfill (YYYY-MM-DD)
            end_date: Optional end date for backfill (YYYY-MM-DD)
            max_runtime: Optional wall-clock budget in seconds. None (the
                default) means no deadline, i.e. unchanged behaviour.
        """
        self.debug = debug
        self.dry_run = dry_run
        self.start_date = start_date
        self.end_date = end_date
        self.is_backfill = start_date is not None or end_date is not None
        self.country_stats = {}  # {country_code: record_count}
        self.max_runtime = max_runtime
        self._started_at = datetime.now(timezone.utc)
        self.deadline_hit = False
        self.logger = self._setup_logging()

    def deadline_exceeded(self) -> bool:
        """True once the wall-clock budget from --max-runtime is spent.

        Runners call this between units of work to stop STARTING new fetches;
        it never interrupts work already in flight and never skips a unit that
        was already fetched. Anything not reached this firing is picked up by
        the next one - every upsert is ON CONFLICT idempotent, so a truncated
        run costs latency, never correctness.
        """
        if self.max_runtime is None:
            return False
        elapsed = (datetime.now(timezone.utc) - self._started_at).total_seconds()
        if elapsed >= self.max_runtime:
            if not self.deadline_hit:
                self.deadline_hit = True
                self.logger.warning(
                    f"{self.RUNNER_NAME}: max runtime {self.max_runtime}s reached "
                    f"after {elapsed:.0f}s - stopping early, remaining work "
                    f"resumes on the next run"
                )
            return True
        return False

    def is_data_unavailable_error(self, error: Exception) -> bool:
        """Check if an exception indicates data is temporarily unavailable (not a real error)."""
        error_str = str(error)
        unavailable_codes = ['503', '404', '409', 'No matching data', 'Max retries exceeded', 'Read timed out', 'timed out']
        return any(code in error_str for code in unavailable_codes)

    def track_country(self, country_code: str, count: int):
        """Track records processed per country for summary logging."""
        self.country_stats[country_code] = self.country_stats.get(country_code, 0) + count

    def format_summary(self, total: int) -> str:
        """Format one-line summary with per-country breakdown."""
        if self.country_stats:
            parts = [f"{cc}={n}" for cc, n in sorted(self.country_stats.items())]
            return f"{self.RUNNER_NAME}: {total} records ({', '.join(parts)})"
        return f"{self.RUNNER_NAME}: {total} records"

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the runner."""
        log_level = logging.DEBUG if self.debug else logging.INFO
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Quieten urllib3/requests DEBUG chatter. NOTE: this alone does NOT
        # protect the token - urllib3's retry message is itself logged at
        # WARNING and passes straight through. The redaction filter below is
        # what actually prevents the leak.
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)

        # SECURITY: redact API tokens on every root handler, so records from
        # any library (urllib3 included) are scrubbed before being formatted.
        for handler in logging.getLogger().handlers:
            if not any(isinstance(f, SecretRedactingFilter) for f in handler.filters):
                handler.addFilter(SecretRedactingFilter())

        return logging.getLogger(self.RUNNER_NAME)

    @contextmanager
    def database_connection(self):
        """
        Context manager for database connections.

        Yields:
            psycopg2 connection object

        Raises:
            Exception: If connection fails
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                port=DB_PORT,
                connect_timeout=10
            )
            # Session setup runs in autocommit so it does NOT leave the backend
            # sitting in an open transaction. psycopg2 defaults to
            # autocommit=False, which made the SET below issue an implicit
            # BEGIN that nothing ever committed - every connection showed up as
            # "idle in transaction" from the moment it opened, and under a
            # session-mode pgbouncer that pinned a pool slot for the whole
            # process lifetime.
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {DB_SCHEMA}")
                # Backstop for the pathology above: kill a session that has an
                # open transaction and is doing nothing (e.g. a runner blocked
                # on a hung ENTSO-E request). This never interrupts a RUNNING
                # statement, so long legitimate queries (FDW sync, backfills)
                # are unaffected. Deliberately NOT paired with a
                # statement_timeout for that reason.
                cur.execute(
                    f"SET idle_in_transaction_session_timeout = {int(IDLE_TX_TIMEOUT_MS)}"
                )
            # Restore normal transactional behaviour: bulk_upsert() manages its
            # own commit/rollback and its atomicity must not change.
            conn.autocommit = False
            self.logger.debug(f"Connected to {DB_NAME}@{DB_HOST}:{DB_PORT}")
            yield conn
        except Exception as e:
            self.logger.error(f"DB connection failed: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def bulk_upsert(
        self,
        conn,
        table: str,
        columns: List[str],
        records: List[Tuple],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        skip_unchanged: bool = False
    ) -> int:
        """
        Perform bulk upsert using execute_values.

        Args:
            conn: Database connection
            table: Table name
            columns: Column names for insert
            records: List of tuples with values
            conflict_columns: Columns for ON CONFLICT clause
            update_columns: Columns to update on conflict (default: all except conflict)
            skip_unchanged: When True, guard the UPDATE with an
                "IS DISTINCT FROM" clause over update_columns so re-upserting
                identical rows does not bump updated_at (mirrors commit
                5c19e38). Default False = no behaviour change.

        Returns:
            Number of records upserted
        """
        if not records:
            self.logger.warning("No records to upsert")
            return 0

        if self.dry_run:
            self.logger.info(f"DRY RUN - Would upsert {len(records)} records to {table}")
            return len(records)

        # Build update columns if not specified
        if update_columns is None:
            update_columns = [c for c in columns if c not in conflict_columns]

        # Build query
        columns_str = ", ".join(columns)
        conflict_str = ", ".join(conflict_columns)
        update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_columns])
        update_str += ", updated_at = CURRENT_TIMESTAMP"

        where_str = ""
        if skip_unchanged and update_columns:
            where_str = "\n            WHERE " + "\n               OR ".join(
                f"{table}.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in update_columns
            )

        query = f"""
            INSERT INTO {table} ({columns_str})
            VALUES %s
            ON CONFLICT ({conflict_str})
            DO UPDATE SET {update_str}{where_str}
        """

        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, records, page_size=1000)
            conn.commit()
            upserted = len(records)
            self.logger.debug(f"Upserted {upserted} records to {table}")
            return upserted
        except Exception as e:
            conn.rollback()
            self.logger.error(f"✗ Bulk upsert failed: {e}")
            raise
        finally:
            cursor.close()

    def get_time_range(self, hours: int = 3) -> Tuple[datetime, datetime]:
        """
        Get time range for data fetching.

        Returns period from (now - hours) to now, rounded to 15 minutes.

        Args:
            hours: Hours of data to fetch

        Returns:
            Tuple of (period_start, period_end) in UTC
        """
        now_utc = datetime.now(timezone.utc)

        # Round down to nearest 15 minutes
        minutes = (now_utc.minute // 15) * 15
        period_end = now_utc.replace(minute=minutes, second=0, microsecond=0)

        # Start is hours before
        period_start = period_end - timedelta(hours=hours)

        return period_start, period_end

    def get_backfill_chunks(self) -> Generator[Tuple[datetime, datetime], None, None]:
        """
        Generate time range chunks for backfill operations.

        Splits the date range into chunks of MAX_CHUNK_DAYS (7 days) to comply
        with ENTSO-E API limits. Each chunk is a tuple of (start_utc, end_utc).

        Yields:
            Tuple of (period_start, period_end) as UTC datetime objects

        Raises:
            ValueError: If backfill dates are not configured
        """
        if not self.is_backfill:
            raise ValueError("Backfill dates not configured")

        # Use today if end_date not specified
        end = self.end_date or date.today()
        start = self.start_date or end - timedelta(days=7)

        # Validate
        if end < start:
            raise ValueError(f"end_date ({end}) must be >= start_date ({start})")

        self.logger.debug(f"Backfill: {start} to {end}")

        # Convert dates to UTC datetimes (start of day in Prague -> UTC)
        current_start = datetime.combine(start, datetime.min.time())
        current_start = current_start.replace(tzinfo=PRAGUE_TZ).astimezone(timezone.utc)

        final_end = datetime.combine(end + timedelta(days=1), datetime.min.time())
        final_end = final_end.replace(tzinfo=PRAGUE_TZ).astimezone(timezone.utc)

        while current_start < final_end:
            # Calculate chunk end (max 7 days from start)
            chunk_end = min(
                current_start + timedelta(days=self.MAX_CHUNK_DAYS),
                final_end
            )

            yield current_start, chunk_end

            # Move to next chunk
            current_start = chunk_end

    def get_output_path(self, filename: str, period_start: datetime) -> Path:
        """
        Get output path for XML file.

        Creates directory structure: DATA_DIR/YYYY/MM/

        Args:
            filename: Base filename
            period_start: Period start for directory structure

        Returns:
            Full path to output file
        """
        output_dir = self.DATA_DIR / str(period_start.year) / f"{period_start.month:02d}"
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir / filename

    def save_xml(self, content: str, filepath: Path) -> None:
        """
        Save XML content to file.

        Args:
            content: XML content string
            filepath: Path to save file
        """
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        self.logger.debug(f"Saved XML to: {filepath}")

    def print_header(self) -> None:
        """Print runner header (no-op, kept for API compatibility)."""
        pass

    def print_footer(self, success: bool = True) -> None:
        """Print runner footer (no-op, kept for API compatibility)."""
        pass

    def _areas_with_data(self, conn, target_date, areas) -> set:
        """Return set of area_ids that already have data for target_date."""
        area_ids = [a[0] for a in areas]
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT area_id FROM {self.TABLE_NAME} "
                f"WHERE trade_date = %s AND area_id = ANY(%s)",
                (target_date, area_ids)
            )
            return {row[0] for row in cur.fetchall()}

    def _run_with_availability_check(self, target_date, areas) -> int:
        """Check DB per area for target_date, fetch full day from API if missing.

        Converts target_date (Prague local) to UTC range and calls _process_area()
        for each area that has no data yet. Subclasses must define _process_area().

        Args:
            target_date: date object (Prague local date)
            areas: list of (area_id, area_code, display_label, country_code) tuples
        """
        start_prague = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=PRAGUE_TZ)
        end_prague = start_prague + timedelta(days=1)
        period_start = start_prague.astimezone(timezone.utc)
        period_end = end_prague.astimezone(timezone.utc)

        total_records = 0
        with self.database_connection() as conn:
            existing = self._areas_with_data(conn, target_date, areas)
            for area_id, area_code, display_label, country_code in areas:
                if area_id in existing:
                    self.logger.debug(f"  {display_label}: data exists for {target_date}, skipping")
                    continue

                records = self._process_area(
                    period_start, period_end,
                    area_id, area_code, display_label, country_code, conn
                )
                total_records += records

        return total_records

    @abstractmethod
    def run(self) -> bool:
        """
        Execute the runner.

        Returns:
            True if successful, False otherwise
        """
        pass

    @classmethod
    def create_argument_parser(cls) -> argparse.ArgumentParser:
        """Create argument parser for the runner."""
        parser = argparse.ArgumentParser(
            description=f"{cls.RUNNER_NAME} - ENTSO-E Data Pipeline"
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Enable debug logging'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Fetch and parse but don\'t upload to database'
        )
        parser.add_argument(
            '--start',
            type=str,
            metavar='YYYY-MM-DD',
            help='Start date for backfill (enables backfill mode)'
        )
        parser.add_argument(
            '--end',
            type=str,
            metavar='YYYY-MM-DD',
            help='End date for backfill (defaults to today if --start is provided)'
        )
        parser.add_argument(
            '--max-runtime',
            type=int,
            default=None,
            metavar='SECONDS',
            help=(
                'Wall-clock budget. Stop starting new work once exceeded and '
                'exit cleanly; the remainder resumes on the next run. '
                'Intended for cron so a degraded upstream cannot make a run '
                'outlive its own schedule interval. Omit for backfills.'
            )
        )
        return parser

    @classmethod
    def parse_date(cls, date_str: Optional[str]) -> Optional[date]:
        """Parse date string to date object."""
        if date_str is None:
            return None
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD.")

    @classmethod
    def main(cls) -> None:
        """Main entry point for the runner."""
        parser = cls.create_argument_parser()
        args = parser.parse_args()

        # Parse dates
        try:
            start_date = cls.parse_date(args.start)
            end_date = cls.parse_date(args.end)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

        runner = cls(
            debug=args.debug,
            dry_run=args.dry_run,
            start_date=start_date,
            end_date=end_date,
            max_runtime=args.max_runtime
        )
        success = runner.run()
        sys.exit(0 if success else 1)
