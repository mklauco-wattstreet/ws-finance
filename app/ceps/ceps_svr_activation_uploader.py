#!/usr/bin/env python3
"""
CEPS SVR Activation Data Uploader

Uploads CEPS SVR (Secondary Reserve) activation data to PostgreSQL:
1. Parses CSV files from CEPS downloads
2. Uploads raw 1-minute activation data to finance.ceps_svr_activation_1min
3. Aggregates to 15-minute intervals in finance.ceps_svr_activation_15min

CSV Format (from ceps_svr_activation_downloader.py):
- Line 1: Od;Do;
- Line 2: 07.01.2026 00:00:00;07.01.2026 23:59:59;
- Line 3: Datum;aFRR+ [MW];aFRR- [MW];mFRR+ [MW];mFRR- [MW];mFRR5 [MW];
- Line 4+: 07.01.2026 00:00;6.15641;0;0;0;0;

Date format: DD.MM.YYYY HH:mm in Europe/Prague timezone (stored as naive timestamp)

Activation columns (power in MW):
- aFRR+ [MW] - automatic frequency restoration reserve (upward)
- aFRR- [MW] - automatic frequency restoration reserve (downward)
- mFRR+ [MW] - manual frequency restoration reserve (upward)
- mFRR- [MW] - manual frequency restoration reserve (downward)
- mFRR5 [MW] - manual frequency restoration reserve (5 minute)
"""

import sys
import csv
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional
from decimal import Decimal

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


def parse_ceps_svr_activation_csv(csv_path: Path, logger) -> List[Tuple[datetime, Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]]:
    """
    Parse CEPS SVR activation CSV file and extract timestamp + activation data.

    Args:
        csv_path: Path to CSV file
        logger: Logger instance

    Returns:
        List of (delivery_timestamp, afrr_plus, afrr_minus, mfrr_plus, mfrr_minus, mfrr_5) tuples
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
                # CSV format: Datum;aFRR+ [MW];aFRR- [MW];mFRR+ [MW];mFRR- [MW];mFRR5 [MW];
                # Need at least 6 columns (timestamp + 5 activation columns)
                if len(row) < 6:
                    continue

                timestamp_str = row[0].strip()
                if not timestamp_str:
                    continue

                try:
                    # Parse date in format "07.01.2026 00:00"
                    # Store as naive timestamp (no timezone conversion)
                    # Data is already in Europe/Prague local time
                    delivery_timestamp = datetime.strptime(timestamp_str, "%d.%m.%Y %H:%M")

                    # Parse activation values (handle empty/missing values)
                    def parse_activation(value_str):
                        """Parse activation value, return None if empty or invalid."""
                        if not value_str or value_str.strip() == '':
                            return None
                        try:
                            return float(value_str.strip())
                        except ValueError:
                            return None

                    afrr_plus = parse_activation(row[1])
                    afrr_minus = parse_activation(row[2])
                    mfrr_plus = parse_activation(row[3])
                    mfrr_minus = parse_activation(row[4])
                    mfrr_5 = parse_activation(row[5])

                    data.append((
                        delivery_timestamp,
                        afrr_plus,
                        afrr_minus,
                        mfrr_plus,
                        mfrr_minus,
                        mfrr_5
                    ))

                except ValueError as e:
                    logger.warning(f"Line {row_num}: Could not parse '{timestamp_str}': {e}")
                    continue

        logger.info(f"✓ Parsed {len(data)} records from {csv_path.name}")
        return data

    except Exception as e:
        logger.error(f"✗ Error parsing {csv_path}: {e}")
        raise


def upload_1min_data(conn, data: List[Tuple], logger) -> int:
    """
    Upload 1-minute SVR activation data to finance.ceps_svr_activation_1min.

    Uses UPSERT to handle duplicates.
    Deduplicates data within the same batch (keeps last occurrence).

    Args:
        conn: Database connection
        data: List of (delivery_timestamp, afrr_plus, afrr_minus, mfrr_plus, mfrr_minus, mfrr_5) tuples
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
    for timestamp, *activations in data:
        seen[timestamp] = activations

    deduplicated_data = [(ts, *activations) for ts, activations in seen.items()]

    if len(deduplicated_data) < len(data):
        duplicates = len(data) - len(deduplicated_data)
        logger.warning(f"⚠ Found {duplicates} duplicate timestamps, keeping last occurrence")

    logger.info(f"Uploading {len(deduplicated_data)} unique records to ceps_svr_activation_1min...")

    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO finance.ceps_svr_activation_1min (
                    delivery_timestamp,
                    afrr_plus_mw,
                    afrr_minus_mw,
                    mfrr_plus_mw,
                    mfrr_minus_mw,
                    mfrr_5_mw
                )
                VALUES %s
                ON CONFLICT (delivery_timestamp) DO UPDATE SET
                    afrr_plus_mw = EXCLUDED.afrr_plus_mw,
                    afrr_minus_mw = EXCLUDED.afrr_minus_mw,
                    mfrr_plus_mw = EXCLUDED.mfrr_plus_mw,
                    mfrr_minus_mw = EXCLUDED.mfrr_minus_mw,
                    mfrr_5_mw = EXCLUDED.mfrr_5_mw,
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
    Aggregate 1-minute SVR activation data to 15-minute intervals for a specific date.

    Calculates for each activation column:
    - mean: Average activation in interval
    - median: Median activation in interval
    - last_at_interval: Last (most recent) activation in interval

    Args:
        conn: Database connection
        trade_date: Date to aggregate
        logger: Logger instance

    Returns:
        Number of 15-minute intervals created/updated
    """
    logger.info(f"Aggregating SVR activation data for {trade_date} to 15min intervals...")

    try:
        with conn.cursor() as cur:
            # Aggregate 1-minute data to 15-minute intervals
            # delivery_timestamp is stored as naive timestamp (local Prague time)
            cur.execute("""
                WITH interval_data AS (
                    SELECT
                        DATE(delivery_timestamp) AS trade_date,
                        -- Calculate 15-minute interval bucket
                        DATE_TRUNC('hour', delivery_timestamp) +
                        INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS interval_start,
                        delivery_timestamp,
                        afrr_plus_mw,
                        afrr_minus_mw,
                        mfrr_plus_mw,
                        mfrr_minus_mw,
                        mfrr_5_mw
                    FROM finance.ceps_svr_activation_1min
                    WHERE DATE(delivery_timestamp) = %s
                ),
                aggregated AS (
                    SELECT
                        trade_date,
                        TO_CHAR(interval_start, 'HH24:MI') || '-' ||
                        TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,

                        -- aFRR+ statistics
                        AVG(afrr_plus_mw) AS afrr_plus_mean_mw,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY afrr_plus_mw) AS afrr_plus_median_mw,
                        (ARRAY_AGG(afrr_plus_mw ORDER BY delivery_timestamp DESC) FILTER (WHERE afrr_plus_mw IS NOT NULL))[1] AS afrr_plus_last_at_interval_mw,

                        -- aFRR- statistics
                        AVG(afrr_minus_mw) AS afrr_minus_mean_mw,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY afrr_minus_mw) AS afrr_minus_median_mw,
                        (ARRAY_AGG(afrr_minus_mw ORDER BY delivery_timestamp DESC) FILTER (WHERE afrr_minus_mw IS NOT NULL))[1] AS afrr_minus_last_at_interval_mw,

                        -- mFRR+ statistics
                        AVG(mfrr_plus_mw) AS mfrr_plus_mean_mw,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mfrr_plus_mw) AS mfrr_plus_median_mw,
                        (ARRAY_AGG(mfrr_plus_mw ORDER BY delivery_timestamp DESC) FILTER (WHERE mfrr_plus_mw IS NOT NULL))[1] AS mfrr_plus_last_at_interval_mw,

                        -- mFRR- statistics
                        AVG(mfrr_minus_mw) AS mfrr_minus_mean_mw,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mfrr_minus_mw) AS mfrr_minus_median_mw,
                        (ARRAY_AGG(mfrr_minus_mw ORDER BY delivery_timestamp DESC) FILTER (WHERE mfrr_minus_mw IS NOT NULL))[1] AS mfrr_minus_last_at_interval_mw,

                        -- mFRR 5 statistics
                        AVG(mfrr_5_mw) AS mfrr_5_mean_mw,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mfrr_5_mw) AS mfrr_5_median_mw,
                        (ARRAY_AGG(mfrr_5_mw ORDER BY delivery_timestamp DESC) FILTER (WHERE mfrr_5_mw IS NOT NULL))[1] AS mfrr_5_last_at_interval_mw

                    FROM interval_data
                    GROUP BY trade_date, interval_start
                )
                INSERT INTO finance.ceps_svr_activation_15min (
                    trade_date, time_interval,
                    afrr_plus_mean_mw, afrr_minus_mean_mw,
                    mfrr_plus_mean_mw, mfrr_minus_mean_mw, mfrr_5_mean_mw,
                    afrr_plus_median_mw, afrr_minus_median_mw,
                    mfrr_plus_median_mw, mfrr_minus_median_mw, mfrr_5_median_mw,
                    afrr_plus_last_at_interval_mw, afrr_minus_last_at_interval_mw,
                    mfrr_plus_last_at_interval_mw, mfrr_minus_last_at_interval_mw, mfrr_5_last_at_interval_mw
                )
                SELECT
                    trade_date, time_interval,
                    afrr_plus_mean_mw, afrr_minus_mean_mw,
                    mfrr_plus_mean_mw, mfrr_minus_mean_mw, mfrr_5_mean_mw,
                    afrr_plus_median_mw, afrr_minus_median_mw,
                    mfrr_plus_median_mw, mfrr_minus_median_mw, mfrr_5_median_mw,
                    afrr_plus_last_at_interval_mw, afrr_minus_last_at_interval_mw,
                    mfrr_plus_last_at_interval_mw, mfrr_minus_last_at_interval_mw, mfrr_5_last_at_interval_mw
                FROM aggregated
                ON CONFLICT (trade_date, time_interval) DO UPDATE SET
                    afrr_plus_mean_mw = EXCLUDED.afrr_plus_mean_mw,
                    afrr_minus_mean_mw = EXCLUDED.afrr_minus_mean_mw,
                    mfrr_plus_mean_mw = EXCLUDED.mfrr_plus_mean_mw,
                    mfrr_minus_mean_mw = EXCLUDED.mfrr_minus_mean_mw,
                    mfrr_5_mean_mw = EXCLUDED.mfrr_5_mean_mw,
                    afrr_plus_median_mw = EXCLUDED.afrr_plus_median_mw,
                    afrr_minus_median_mw = EXCLUDED.afrr_minus_median_mw,
                    mfrr_plus_median_mw = EXCLUDED.mfrr_plus_median_mw,
                    mfrr_minus_median_mw = EXCLUDED.mfrr_minus_median_mw,
                    mfrr_5_median_mw = EXCLUDED.mfrr_5_median_mw,
                    afrr_plus_last_at_interval_mw = EXCLUDED.afrr_plus_last_at_interval_mw,
                    afrr_minus_last_at_interval_mw = EXCLUDED.afrr_minus_last_at_interval_mw,
                    mfrr_plus_last_at_interval_mw = EXCLUDED.mfrr_plus_last_at_interval_mw,
                    mfrr_minus_last_at_interval_mw = EXCLUDED.mfrr_minus_last_at_interval_mw,
                    mfrr_5_last_at_interval_mw = EXCLUDED.mfrr_5_last_at_interval_mw,
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
    data = parse_ceps_svr_activation_csv(csv_path, logger)

    if not data:
        logger.warning("No data found in CSV file")
        return 0, 0

    # Upload 1-minute data
    records_1min = upload_1min_data(conn, data, logger)

    # Get unique trade dates from the data
    trade_dates = set(dt.date() for dt, *_ in data)

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
        description='Upload CEPS SVR activation data from CSV files to PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload all CSV files from a specific month
  python3 ceps_svr_activation_uploader.py --folder /app/downloads/ceps/2026/01

  # Upload specific CSV file
  python3 ceps_svr_activation_uploader.py --file /app/downloads/ceps/2026/01/data_AktivaceSVRvCR_20260107_163913.csv

  # Upload with debug logging
  python3 ceps_svr_activation_uploader.py --folder /app/downloads/ceps/2026/01 --debug
        """
    )

    # Mutually exclusive: either folder or file
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        '--folder',
        type=str,
        help='Folder containing CSV files to upload (uploads all *.csv files with AktivaceSVRvCR in name)'
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

        # Filter for AktivaceSVRvCR CSV files
        csv_files = sorted([f for f in folder_path.glob("*.csv") if "AktivaceSVRvCR" in f.name])

        if not csv_files:
            logger.error(f"No AktivaceSVRvCR CSV files found in: {folder_path}")
            sys.exit(1)

        logger.info(f"Found {len(csv_files)} AktivaceSVRvCR CSV files in {folder_path}")

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
