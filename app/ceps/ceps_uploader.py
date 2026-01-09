#!/usr/bin/env python3
"""
CEPS Imbalance Data Uploader

Uploads CEPS actual system imbalance data to PostgreSQL:
1. Parses CSV files from CEPS downloads
2. Uploads raw 1-minute data to finance.ceps_actual_imbalance_1min
3. Aggregates to 15-minute intervals in finance.ceps_actual_imbalance_15min

CSV Format (from ceps_hybrid_downloader.py):
- Line 1: Verze dat;Od;Do;Agregační funkce;Agregace;
- Line 2: ;04.01.2026 00:00:00;04.01.2026 23:59:59;agregace průměr;minuta;
- Line 3: Datum;Aktuální odchylka [MW];
- Line 4+: 04.01.2026 00:00;-160.5321;

Date format: DD.MM.YYYY HH:mm in Europe/Prague timezone
"""

import sys
import csv
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_values

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT
from common import setup_logging


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        port=DB_PORT
    )


def parse_ceps_csv(csv_path: Path, logger) -> List[Tuple[datetime, float]]:
    """
    Parse CEPS CSV file and extract timestamp + load data.

    Args:
        csv_path: Path to CSV file
        logger: Logger instance

    Returns:
        List of (delivery_timestamp, load_mw) tuples
    """
    logger.info(f"Parsing CSV: {csv_path}")

    data = []

    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            # Skip first 3 lines (headers and metadata)
            for _ in range(3):
                next(f)

            # Read CSV data (semicolon-separated)
            reader = csv.reader(f, delimiter=';')

            for row_num, row in enumerate(reader, start=4):
                if len(row) < 2:
                    continue

                timestamp_str = row[0].strip()
                load_str = row[1].strip()

                if not timestamp_str or not load_str:
                    continue

                try:
                    # Parse date in format "04.01.2026 00:00"
                    # Store as naive timestamp (no timezone conversion)
                    # Data is already in Europe/Prague local time
                    delivery_timestamp = datetime.strptime(timestamp_str, "%d.%m.%Y %H:%M")

                    # Parse load value
                    load_mw = float(load_str)

                    data.append((delivery_timestamp, load_mw))

                except ValueError as e:
                    logger.warning(f"Line {row_num}: Could not parse '{timestamp_str}' or '{load_str}': {e}")
                    continue

        logger.info(f"✓ Parsed {len(data)} records from {csv_path.name}")
        return data

    except Exception as e:
        logger.error(f"✗ Error parsing {csv_path}: {e}")
        raise


def upload_1min_data(conn, data: List[Tuple[datetime, float]], logger) -> int:
    """
    Upload 1-minute data to finance.ceps_actual_imbalance_1min.

    Uses UPSERT to handle duplicates.
    Deduplicates data within the same batch (keeps last occurrence).

    Args:
        conn: Database connection
        data: List of (delivery_timestamp, load_mw) tuples
        logger: Logger instance

    Returns:
        Number of records inserted/updated
    """
    if not data:
        logger.warning("No data to upload")
        return 0

    # Deduplicate: keep last occurrence of each timestamp
    # (later records in CSV are more recent/accurate)
    seen = {}
    for timestamp, load_mw in data:
        seen[timestamp] = load_mw

    deduplicated_data = [(ts, load) for ts, load in seen.items()]

    if len(deduplicated_data) < len(data):
        duplicates = len(data) - len(deduplicated_data)
        logger.warning(f"⚠ Found {duplicates} duplicate timestamps, keeping last occurrence")

    logger.info(f"Uploading {len(deduplicated_data)} unique records to ceps_actual_imbalance_1min...")

    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO finance.ceps_actual_imbalance_1min (delivery_timestamp, load_mw)
                VALUES %s
                ON CONFLICT (delivery_timestamp) DO UPDATE SET
                    load_mw = EXCLUDED.load_mw,
                    created_at = CURRENT_TIMESTAMP
                """,
                deduplicated_data
            )
            conn.commit()

        logger.info(f"✓ Uploaded {len(deduplicated_data)} records to 1min table")
        return len(deduplicated_data)

    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Error uploading 1min data: {e}")
        raise


def aggregate_to_15min(conn, trade_date: datetime.date, logger) -> int:
    """
    Aggregate 1-minute data to 15-minute intervals for a specific date.

    Calculates:
    - load_mean_mw: Average load in interval
    - load_median_mw: Median load in interval
    - last_load_at_interval_mw: Last (most recent) load in interval

    Args:
        conn: Database connection
        trade_date: Date to aggregate
        logger: Logger instance

    Returns:
        Number of 15-minute intervals created/updated
    """
    logger.info(f"Aggregating data for {trade_date} to 15min intervals...")

    try:
        with conn.cursor() as cur:
            # Aggregate 1-minute data to 15-minute intervals
            # delivery_timestamp is now stored as naive timestamp (local Prague time)
            cur.execute("""
                WITH interval_data AS (
                    SELECT
                        DATE(delivery_timestamp) AS trade_date,
                        -- Calculate 15-minute interval bucket
                        DATE_TRUNC('hour', delivery_timestamp) +
                        INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS interval_start,
                        delivery_timestamp,
                        load_mw
                    FROM finance.ceps_actual_imbalance_1min
                    WHERE DATE(delivery_timestamp) = %s
                ),
                aggregated AS (
                    SELECT
                        trade_date,
                        TO_CHAR(interval_start, 'HH24:MI') || '-' ||
                        TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
                        AVG(load_mw) AS load_mean_mw,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY load_mw) AS load_median_mw,
                        (ARRAY_AGG(load_mw ORDER BY delivery_timestamp DESC))[1] AS last_load_at_interval_mw
                    FROM interval_data
                    GROUP BY trade_date, interval_start
                )
                INSERT INTO finance.ceps_actual_imbalance_15min
                    (trade_date, time_interval, load_mean_mw, load_median_mw, last_load_at_interval_mw)
                SELECT
                    trade_date,
                    time_interval,
                    load_mean_mw,
                    load_median_mw,
                    last_load_at_interval_mw
                FROM aggregated
                ON CONFLICT (trade_date, time_interval) DO UPDATE SET
                    load_mean_mw = EXCLUDED.load_mean_mw,
                    load_median_mw = EXCLUDED.load_median_mw,
                    last_load_at_interval_mw = EXCLUDED.last_load_at_interval_mw,
                    created_at = CURRENT_TIMESTAMP
            """, (trade_date,))

            rows_affected = cur.rowcount
            conn.commit()

        logger.info(f"✓ Created/updated {rows_affected} 15-minute intervals for {trade_date}")
        return rows_affected

    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Error aggregating to 15min: {e}")
        raise


def process_csv_file(csv_path: Path, conn, logger) -> Tuple[int, int]:
    """
    Process a single CSV file: parse, upload 1min data, aggregate to 15min.

    Args:
        csv_path: Path to CSV file
        conn: Database connection
        logger: Logger instance

    Returns:
        Tuple of (1min_records, 15min_intervals) uploaded
    """
    logger.info("=" * 70)
    logger.info(f"Processing: {csv_path}")
    logger.info("=" * 70)

    # Parse CSV
    data = parse_ceps_csv(csv_path, logger)

    if not data:
        logger.warning("No data found in CSV file")
        return 0, 0

    # Upload 1-minute data
    records_1min = upload_1min_data(conn, data, logger)

    # Get unique trade dates from the data
    trade_dates = set(dt.date() for dt, _ in data)

    # Aggregate each date to 15-minute intervals
    total_15min = 0
    for trade_date in sorted(trade_dates):
        intervals_15min = aggregate_to_15min(conn, trade_date, logger)
        total_15min += intervals_15min

    logger.info("=" * 70)
    logger.info(f"✓ Completed: {csv_path.name}")
    logger.info(f"  1min records: {records_1min}")
    logger.info(f"  15min intervals: {total_15min}")
    logger.info("=" * 70)

    return records_1min, total_15min


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Upload CEPS imbalance data from CSV files to PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload all CSV files from a specific month
  python3 ceps_uploader.py --folder /app/downloads/ceps/2026/01

  # Upload specific CSV file
  python3 ceps_uploader.py --file /app/downloads/ceps/2026/01/data_AktualniSystemovaOdchylkaCR_20260104_141035.csv

  # Upload with debug logging
  python3 ceps_uploader.py --folder /app/downloads/ceps/2026/01 --debug
        """
    )

    # Mutually exclusive: either folder or file
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        '--folder',
        type=str,
        help='Folder containing CSV files to upload (uploads all *.csv files)'
    )
    input_group.add_argument(
        '--file',
        type=str,
        help='Single CSV file to upload'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(debug=args.debug)

    # Get list of CSV files to process
    csv_files = []

    if args.folder:
        folder_path = Path(args.folder)
        if not folder_path.exists():
            logger.error(f"Folder does not exist: {folder_path}")
            sys.exit(1)

        if not folder_path.is_dir():
            logger.error(f"Not a directory: {folder_path}")
            sys.exit(1)

        csv_files = sorted(folder_path.glob("*.csv"))

        if not csv_files:
            logger.error(f"No CSV files found in: {folder_path}")
            sys.exit(1)

        logger.info(f"Found {len(csv_files)} CSV files in {folder_path}")

    elif args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            logger.error(f"File does not exist: {file_path}")
            sys.exit(1)

        if not file_path.is_file():
            logger.error(f"Not a file: {file_path}")
            sys.exit(1)

        csv_files = [file_path]

    # Connect to database
    try:
        conn = get_db_connection()
        logger.info(f"✓ Connected to database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    except Exception as e:
        logger.error(f"✗ Failed to connect to database: {e}")
        sys.exit(1)

    # Process all CSV files
    total_records_1min = 0
    total_intervals_15min = 0
    successful_files = 0
    failed_files = 0

    try:
        for csv_file in csv_files:
            try:
                records_1min, intervals_15min = process_csv_file(csv_file, conn, logger)
                total_records_1min += records_1min
                total_intervals_15min += intervals_15min
                successful_files += 1
            except Exception as e:
                logger.error(f"✗ Failed to process {csv_file.name}: {e}")
                failed_files += 1
                if args.debug:
                    import traceback
                    logger.error(traceback.format_exc())
                continue

        # Summary
        logger.info("")
        logger.info("=" * 70)
        logger.info("UPLOAD SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Files processed: {len(csv_files)}")
        logger.info(f"  Successful: {successful_files}")
        logger.info(f"  Failed: {failed_files}")
        logger.info(f"Total 1min records: {total_records_1min:,}")
        logger.info(f"Total 15min intervals: {total_intervals_15min:,}")
        logger.info("=" * 70)

        if failed_files > 0:
            sys.exit(1)

    finally:
        conn.close()
        logger.info("✓ Database connection closed")


if __name__ == "__main__":
    main()
