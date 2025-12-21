#!/usr/bin/env python3
"""
Base runner module providing shared functionality for all ENTSO-E data runners.

Features:
- Database connection with context manager
- Bulk upsert via execute_values()
- Logging setup
- Dry-run support
- XML file saving
"""

import sys
import logging
import argparse
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import psycopg2
from psycopg2 import extras

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA


class BaseRunner(ABC):
    """Base class for all ENTSO-E data runners.

    Provides common functionality for:
    - Database connections
    - Bulk upserts
    - Logging
    - Time range calculations
    - XML file management
    """

    # Override in subclasses
    RUNNER_NAME = "BaseRunner"
    DATA_DIR = Path(__file__).parent.parent / "entsoe" / "data"

    def __init__(self, debug: bool = False, dry_run: bool = False):
        """
        Initialize runner.

        Args:
            debug: Enable debug logging
            dry_run: Fetch and parse but don't upload
        """
        self.debug = debug
        self.dry_run = dry_run
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the runner."""
        log_level = logging.DEBUG if self.debug else logging.INFO
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # SECURITY: Suppress urllib3/requests debug logging to prevent
        # API tokens from appearing in logs (they log full URLs with query params)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)

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
            self.logger.info("Connecting to database...")
            conn = psycopg2.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                port=DB_PORT,
                connect_timeout=10,
                options=f'-c search_path={DB_SCHEMA}'
            )
            self.logger.info(f"✓ Connected to {DB_NAME}@{DB_HOST}:{DB_PORT}")
            yield conn
        except Exception as e:
            self.logger.error(f"✗ Database connection failed: {e}")
            raise
        finally:
            if conn:
                conn.close()
                self.logger.info("Database connection closed")

    def bulk_upsert(
        self,
        conn,
        table: str,
        columns: List[str],
        records: List[Tuple],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None
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

        query = f"""
            INSERT INTO {table} ({columns_str})
            VALUES %s
            ON CONFLICT ({conflict_str})
            DO UPDATE SET {update_str}
        """

        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, records, page_size=1000)
            conn.commit()
            upserted = len(records)
            self.logger.info(f"✓ Upserted {upserted} records to {table}")
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
        """Print runner header."""
        self.logger.info("")
        self.logger.info("╔══════════════════════════════════════════════════════════╗")
        self.logger.info(f"║  {self.RUNNER_NAME:<56} ║")
        self.logger.info("╚══════════════════════════════════════════════════════════╝")
        self.logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if self.dry_run:
            self.logger.info("DRY RUN MODE - No data will be uploaded")
        self.logger.info("")

    def print_footer(self, success: bool = True) -> None:
        """Print runner footer."""
        status = "Completed Successfully" if success else "Completed with Errors"
        self.logger.info("")
        self.logger.info("╔══════════════════════════════════════════════════════════╗")
        self.logger.info(f"║  {status:<56} ║")
        self.logger.info("╚══════════════════════════════════════════════════════════╝")
        self.logger.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("")

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
        return parser

    @classmethod
    def main(cls) -> None:
        """Main entry point for the runner."""
        parser = cls.create_argument_parser()
        args = parser.parse_args()

        runner = cls(debug=args.debug, dry_run=args.dry_run)
        success = runner.run()
        sys.exit(0 if success else 1)
