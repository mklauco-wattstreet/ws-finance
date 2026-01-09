#!/usr/bin/env python3
"""
CEPS RE Price Data Uploader

Uploads CEPS actual reserve energy (RE) pricing data to PostgreSQL:
1. Parses CSV files from CEPS downloads
2. Uploads raw 1-minute pricing data to finance.ceps_actual_re_price_1min
3. Aggregates to 15-minute intervals in finance.ceps_actual_re_price_15min

CSV Format (from ceps_re_price_downloader.py):
- Line 1: Verze dat;Od;Do;Agregační funkce;Agregace;Typ DT;
- Line 2: reálná data;04.01.2026 00:00:00;04.01.2026 23:59:59;agregace průměr;minuta;Vše;
- Line 3: Datum;aFRR [EUR/MWh];mFRR+ [EUR/MWh];mFRR- [EUR/MWh];mFRR5 [EUR/MWh];
- Line 4+: 04.01.2026 00:00;191.312;0;0;0;

Date format: DD.MM.YYYY HH:mm in Europe/Prague timezone (stored as naive timestamp)

Price columns:
- aFRR [EUR/MWh] - single value applies to BOTH aFRR+ and aFRR- in database
- mFRR+ (manual frequency restoration reserve - upward)
- mFRR- (manual frequency restoration reserve - downward)
- mFRR5 (manual frequency restoration reserve - 5 minute)

IMPORTANT: Official logic - when CSV has single aFRR column, that value applies to both aFRR+ and aFRR- columns in the database.
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


def parse_ceps_re_price_csv(csv_path: Path, logger) -> List[Tuple[datetime, Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]]:
    """
    Parse CEPS RE price CSV file and extract timestamp + price data.

    Args:
        csv_path: Path to CSV file
        logger: Logger instance

    Returns:
        List of (delivery_timestamp, price_afrr_plus, price_afrr_minus, price_mfrr_plus, price_mfrr_minus, price_mfrr_5) tuples

    Note: CSV has single aFRR column. Official logic: single aFRR value applies to BOTH aFRR+ and aFRR- in database.
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
                # CSV format: Datum;aFRR [EUR/MWh];mFRR+ [EUR/MWh];mFRR- [EUR/MWh];mFRR5 [EUR/MWh];
                # Need at least 5 columns (timestamp + 4 price columns)
                if len(row) < 5:
                    continue

                timestamp_str = row[0].strip()
                if not timestamp_str:
                    continue

                try:
                    # Parse date in format "04.01.2026 00:00"
                    # Store as naive timestamp (no timezone conversion)
                    # Data is already in Europe/Prague local time
                    delivery_timestamp = datetime.strptime(timestamp_str, "%d.%m.%Y %H:%M")

                    # Parse price values (handle empty/missing values)
                    def parse_price(value_str):
                        """Parse price value, return None if empty or invalid."""
                        if not value_str or value_str.strip() == '':
                            return None
                        try:
                            return float(value_str.strip())
                        except ValueError:
                            return None

                    # OFFICIAL LOGIC: Single aFRR column applies to BOTH aFRR+ and aFRR-
                    price_afrr = parse_price(row[1])
                    price_afrr_plus = price_afrr   # Same value for both
                    price_afrr_minus = price_afrr  # Same value for both

                    price_mfrr_plus = parse_price(row[2])
                    price_mfrr_minus = parse_price(row[3])
                    price_mfrr_5 = parse_price(row[4])

                    data.append((
                        delivery_timestamp,
                        price_afrr_plus,
                        price_afrr_minus,
                        price_mfrr_plus,
                        price_mfrr_minus,
                        price_mfrr_5
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
    Upload 1-minute RE price data to finance.ceps_actual_re_price_1min.

    Uses UPSERT to handle duplicates.
    Deduplicates data within the same batch (keeps last occurrence).

    Args:
        conn: Database connection
        data: List of (delivery_timestamp, price_afrr_plus, price_afrr_minus, price_mfrr_plus, price_mfrr_minus, price_mfrr_5) tuples
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
    for timestamp, *prices in data:
        seen[timestamp] = prices

    deduplicated_data = [(ts, *prices) for ts, prices in seen.items()]

    if len(deduplicated_data) < len(data):
        duplicates = len(data) - len(deduplicated_data)
        logger.warning(f"⚠ Found {duplicates} duplicate timestamps, keeping last occurrence")

    logger.info(f"Uploading {len(deduplicated_data)} unique records to ceps_actual_re_price_1min...")

    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO finance.ceps_actual_re_price_1min (
                    delivery_timestamp,
                    price_afrr_plus_eur_mwh,
                    price_afrr_minus_eur_mwh,
                    price_mfrr_plus_eur_mwh,
                    price_mfrr_minus_eur_mwh,
                    price_mfrr_5_eur_mwh
                )
                VALUES %s
                ON CONFLICT (delivery_timestamp) DO UPDATE SET
                    price_afrr_plus_eur_mwh = EXCLUDED.price_afrr_plus_eur_mwh,
                    price_afrr_minus_eur_mwh = EXCLUDED.price_afrr_minus_eur_mwh,
                    price_mfrr_plus_eur_mwh = EXCLUDED.price_mfrr_plus_eur_mwh,
                    price_mfrr_minus_eur_mwh = EXCLUDED.price_mfrr_minus_eur_mwh,
                    price_mfrr_5_eur_mwh = EXCLUDED.price_mfrr_5_eur_mwh,
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
    Aggregate 1-minute RE price data to 15-minute intervals for a specific date.

    Calculates for each price column:
    - mean: Average price in interval
    - median: Median price in interval
    - last_at_interval: Last (most recent) price in interval

    Args:
        conn: Database connection
        trade_date: Date to aggregate
        logger: Logger instance

    Returns:
        Number of 15-minute intervals created/updated
    """
    logger.info(f"Aggregating RE price data for {trade_date} to 15min intervals...")

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
                        price_afrr_plus_eur_mwh,
                        price_afrr_minus_eur_mwh,
                        price_mfrr_plus_eur_mwh,
                        price_mfrr_minus_eur_mwh,
                        price_mfrr_5_eur_mwh
                    FROM finance.ceps_actual_re_price_1min
                    WHERE DATE(delivery_timestamp) = %s
                ),
                aggregated AS (
                    SELECT
                        trade_date,
                        TO_CHAR(interval_start, 'HH24:MI') || '-' ||
                        TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,

                        -- aFRR+ statistics
                        AVG(price_afrr_plus_eur_mwh) AS price_afrr_plus_mean_eur_mwh,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_afrr_plus_eur_mwh) AS price_afrr_plus_median_eur_mwh,
                        (ARRAY_AGG(price_afrr_plus_eur_mwh ORDER BY delivery_timestamp DESC) FILTER (WHERE price_afrr_plus_eur_mwh IS NOT NULL))[1] AS price_afrr_plus_last_at_interval_eur_mwh,

                        -- aFRR- statistics
                        AVG(price_afrr_minus_eur_mwh) AS price_afrr_minus_mean_eur_mwh,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_afrr_minus_eur_mwh) AS price_afrr_minus_median_eur_mwh,
                        (ARRAY_AGG(price_afrr_minus_eur_mwh ORDER BY delivery_timestamp DESC) FILTER (WHERE price_afrr_minus_eur_mwh IS NOT NULL))[1] AS price_afrr_minus_last_at_interval_eur_mwh,

                        -- mFRR+ statistics
                        AVG(price_mfrr_plus_eur_mwh) AS price_mfrr_plus_mean_eur_mwh,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_mfrr_plus_eur_mwh) AS price_mfrr_plus_median_eur_mwh,
                        (ARRAY_AGG(price_mfrr_plus_eur_mwh ORDER BY delivery_timestamp DESC) FILTER (WHERE price_mfrr_plus_eur_mwh IS NOT NULL))[1] AS price_mfrr_plus_last_at_interval_eur_mwh,

                        -- mFRR- statistics
                        AVG(price_mfrr_minus_eur_mwh) AS price_mfrr_minus_mean_eur_mwh,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_mfrr_minus_eur_mwh) AS price_mfrr_minus_median_eur_mwh,
                        (ARRAY_AGG(price_mfrr_minus_eur_mwh ORDER BY delivery_timestamp DESC) FILTER (WHERE price_mfrr_minus_eur_mwh IS NOT NULL))[1] AS price_mfrr_minus_last_at_interval_eur_mwh,

                        -- mFRR 5 statistics
                        AVG(price_mfrr_5_eur_mwh) AS price_mfrr_5_mean_eur_mwh,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_mfrr_5_eur_mwh) AS price_mfrr_5_median_eur_mwh,
                        (ARRAY_AGG(price_mfrr_5_eur_mwh ORDER BY delivery_timestamp DESC) FILTER (WHERE price_mfrr_5_eur_mwh IS NOT NULL))[1] AS price_mfrr_5_last_at_interval_eur_mwh

                    FROM interval_data
                    GROUP BY trade_date, interval_start
                )
                INSERT INTO finance.ceps_actual_re_price_15min (
                    trade_date, time_interval,
                    price_afrr_plus_mean_eur_mwh, price_afrr_minus_mean_eur_mwh,
                    price_mfrr_plus_mean_eur_mwh, price_mfrr_minus_mean_eur_mwh, price_mfrr_5_mean_eur_mwh,
                    price_afrr_plus_median_eur_mwh, price_afrr_minus_median_eur_mwh,
                    price_mfrr_plus_median_eur_mwh, price_mfrr_minus_median_eur_mwh, price_mfrr_5_median_eur_mwh,
                    price_afrr_plus_last_at_interval_eur_mwh, price_afrr_minus_last_at_interval_eur_mwh,
                    price_mfrr_plus_last_at_interval_eur_mwh, price_mfrr_minus_last_at_interval_eur_mwh, price_mfrr_5_last_at_interval_eur_mwh
                )
                SELECT
                    trade_date, time_interval,
                    price_afrr_plus_mean_eur_mwh, price_afrr_minus_mean_eur_mwh,
                    price_mfrr_plus_mean_eur_mwh, price_mfrr_minus_mean_eur_mwh, price_mfrr_5_mean_eur_mwh,
                    price_afrr_plus_median_eur_mwh, price_afrr_minus_median_eur_mwh,
                    price_mfrr_plus_median_eur_mwh, price_mfrr_minus_median_eur_mwh, price_mfrr_5_median_eur_mwh,
                    price_afrr_plus_last_at_interval_eur_mwh, price_afrr_minus_last_at_interval_eur_mwh,
                    price_mfrr_plus_last_at_interval_eur_mwh, price_mfrr_minus_last_at_interval_eur_mwh, price_mfrr_5_last_at_interval_eur_mwh
                FROM aggregated
                ON CONFLICT (trade_date, time_interval) DO UPDATE SET
                    price_afrr_plus_mean_eur_mwh = EXCLUDED.price_afrr_plus_mean_eur_mwh,
                    price_afrr_minus_mean_eur_mwh = EXCLUDED.price_afrr_minus_mean_eur_mwh,
                    price_mfrr_plus_mean_eur_mwh = EXCLUDED.price_mfrr_plus_mean_eur_mwh,
                    price_mfrr_minus_mean_eur_mwh = EXCLUDED.price_mfrr_minus_mean_eur_mwh,
                    price_mfrr_5_mean_eur_mwh = EXCLUDED.price_mfrr_5_mean_eur_mwh,
                    price_afrr_plus_median_eur_mwh = EXCLUDED.price_afrr_plus_median_eur_mwh,
                    price_afrr_minus_median_eur_mwh = EXCLUDED.price_afrr_minus_median_eur_mwh,
                    price_mfrr_plus_median_eur_mwh = EXCLUDED.price_mfrr_plus_median_eur_mwh,
                    price_mfrr_minus_median_eur_mwh = EXCLUDED.price_mfrr_minus_median_eur_mwh,
                    price_mfrr_5_median_eur_mwh = EXCLUDED.price_mfrr_5_median_eur_mwh,
                    price_afrr_plus_last_at_interval_eur_mwh = EXCLUDED.price_afrr_plus_last_at_interval_eur_mwh,
                    price_afrr_minus_last_at_interval_eur_mwh = EXCLUDED.price_afrr_minus_last_at_interval_eur_mwh,
                    price_mfrr_plus_last_at_interval_eur_mwh = EXCLUDED.price_mfrr_plus_last_at_interval_eur_mwh,
                    price_mfrr_minus_last_at_interval_eur_mwh = EXCLUDED.price_mfrr_minus_last_at_interval_eur_mwh,
                    price_mfrr_5_last_at_interval_eur_mwh = EXCLUDED.price_mfrr_5_last_at_interval_eur_mwh,
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
    data = parse_ceps_re_price_csv(csv_path, logger)

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
        description='Upload CEPS RE price data from CSV files to PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload all CSV files from a specific month
  python3 ceps_re_price_uploader.py --folder /app/downloads/ceps/2026/01

  # Upload specific CSV file
  python3 ceps_re_price_uploader.py --file /app/downloads/ceps/2026/01/data_AktualniCenaRE_20260104_141035.csv

  # Upload with debug logging
  python3 ceps_re_price_uploader.py --folder /app/downloads/ceps/2026/01 --debug
        """
    )

    # Mutually exclusive: either folder or file
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        '--folder',
        type=str,
        help='Folder containing CSV files to upload (uploads all *.csv files with AktualniCenaRE in name)'
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

        # Filter for AktualniCenaRE CSV files
        csv_files = sorted([f for f in folder_path.glob("*.csv") if "AktualniCenaRE" in f.name])

        if not csv_files:
            logger.error(f"No AktualniCenaRE CSV files found in: {folder_path}")
            sys.exit(1)

        logger.info(f"Found {len(csv_files)} AktualniCenaRE CSV files in {folder_path}")

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
