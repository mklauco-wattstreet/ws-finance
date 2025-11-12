#!/usr/bin/env python3
"""
ENTSO-E Imbalance Data Pipeline - Complete workflow.

Fetches imbalance prices (A85) and volumes (A86) for the preceding hour,
parses the XML data, and uploads to PostgreSQL database.

This script runs every 15 minutes via cron.

Usage:
    python3 entsoe_imbalance_pipeline.py [--debug] [--dry-run]

Options:
    --debug     Enable debug logging
    --dry-run   Fetch and parse but don't upload to database
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from entsoe.entsoe_client import EntsoeClient
from entsoe.parse_imbalance_to_db import ImbalanceDataParser
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT
import psycopg2
from psycopg2 import extras


def setup_logging(debug=False):
    """Setup logging configuration."""
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def get_preceding_hour_range(lag_hours=3):
    """
    Get the time range for the preceding hour with a lag.

    ENTSO-E data is not available immediately. We use a 3-hour lag
    to ensure data is available when we fetch it.

    Returns current time minus lag, rounded down to nearest 15 minutes,
    and period start is 1 hour before that.

    Args:
        lag_hours: Hours to lag behind current time (default 3)

    Returns:
        tuple: (period_start, period_end) as datetime objects
    """
    now = datetime.now()

    # Apply lag for data availability
    lagged_time = now - timedelta(hours=lag_hours)

    # Round down to nearest 15 minutes
    minutes = (lagged_time.minute // 15) * 15
    period_end = lagged_time.replace(minute=minutes, second=0, microsecond=0)

    # One hour before
    period_start = period_end - timedelta(hours=1)

    return period_start, period_end


def connect_database(logger):
    """Connect to PostgreSQL database."""
    logger.info("Connecting to database...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            connect_timeout=10
        )
        logger.info(f"✓ Connected to {DB_NAME}@{DB_HOST}:{DB_PORT}")
        return conn
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return None


def create_table_if_not_exists(conn, logger):
    """Create entsoe_imbalance_prices table if it doesn't exist."""
    cursor = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS entsoe_imbalance_prices (
        id SERIAL PRIMARY KEY,
        trade_date DATE NOT NULL,
        period INTEGER NOT NULL,
        time_interval VARCHAR(11) NOT NULL,
        pos_imb_price_czk_mwh NUMERIC(15, 3) NOT NULL,
        pos_imb_scarcity_czk_mwh NUMERIC(15, 3) NOT NULL,
        pos_imb_incentive_czk_mwh NUMERIC(15, 3) NOT NULL,
        pos_imb_financial_neutrality_czk_mwh NUMERIC(15, 3) NOT NULL,
        neg_imb_price_czk_mwh NUMERIC(15, 3) NOT NULL,
        neg_imb_scarcity_czk_mwh NUMERIC(15, 3) NOT NULL,
        neg_imb_incentive_czk_mwh NUMERIC(15, 3) NOT NULL,
        neg_imb_financial_neutrality_czk_mwh NUMERIC(15, 3) NOT NULL,
        imbalance_mwh NUMERIC(12, 5),
        difference_mwh NUMERIC(12, 5),
        situation VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (trade_date, period),
        UNIQUE (trade_date, time_interval)
    );

    CREATE INDEX IF NOT EXISTS idx_entsoe_imb_prices_trade_date
        ON entsoe_imbalance_prices(trade_date);
    CREATE INDEX IF NOT EXISTS idx_entsoe_imb_prices_period
        ON entsoe_imbalance_prices(period);
    """

    try:
        cursor.execute(create_table_sql)
        conn.commit()
        logger.debug("Table entsoe_imbalance_prices verified/created")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to create table: {e}")
        return False
    finally:
        cursor.close()


def check_if_exists(conn, trade_date, period):
    """Check if record already exists for given trade_date and period."""
    cursor = conn.cursor()
    query = """
        SELECT COUNT(*) FROM entsoe_imbalance_prices
        WHERE trade_date = %s AND period = %s
    """
    cursor.execute(query, (trade_date, period))
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0


def upload_to_database(conn, parser, logger, dry_run=False):
    """
    Upload parsed data to database.

    Args:
        conn: Database connection
        parser: ImbalanceDataParser with combined_data
        logger: Logger instance
        dry_run: If True, only check for duplicates but don't insert

    Returns:
        tuple: (inserted_count, skipped_count)
    """
    if not parser.combined_data:
        logger.warning("No data to upload")
        return 0, 0

    cursor = conn.cursor()
    inserted = 0
    skipped = 0

    insert_query = """
        INSERT INTO entsoe_imbalance_prices (
            trade_date, period, time_interval,
            pos_imb_price_czk_mwh, pos_imb_scarcity_czk_mwh,
            pos_imb_incentive_czk_mwh, pos_imb_financial_neutrality_czk_mwh,
            neg_imb_price_czk_mwh, neg_imb_scarcity_czk_mwh,
            neg_imb_incentive_czk_mwh, neg_imb_financial_neutrality_czk_mwh,
            imbalance_mwh, difference_mwh, situation, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for record in parser.combined_data:
        trade_date = record['trade_date']
        period = record['period']

        # Check if exists
        if check_if_exists(conn, trade_date, period):
            logger.debug(f"Record exists: {trade_date} period {period} - skipping")
            skipped += 1
            continue

        if dry_run:
            logger.debug(f"DRY RUN: Would insert {trade_date} period {period}")
            inserted += 1
            continue

        # Insert record
        try:
            values = (
                record['trade_date'],
                record['period'],
                record['time_interval'],
                record['pos_imb_price_czk_mwh'],
                record['pos_imb_scarcity_czk_mwh'],
                record['pos_imb_incentive_czk_mwh'],
                record['pos_imb_financial_neutrality_czk_mwh'],
                record['neg_imb_price_czk_mwh'],
                record['neg_imb_scarcity_czk_mwh'],
                record['neg_imb_incentive_czk_mwh'],
                record['neg_imb_financial_neutrality_czk_mwh'],
                record['imbalance_mwh'],
                record['difference_mwh'],
                record['situation'],
                record['status']
            )

            cursor.execute(insert_query, values)
            inserted += 1
            logger.debug(f"Inserted: {trade_date} period {period}")

        except Exception as e:
            logger.error(f"Failed to insert {trade_date} period {period}: {e}")
            conn.rollback()
            continue

    if not dry_run:
        conn.commit()

    cursor.close()
    return inserted, skipped


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="ENTSO-E Imbalance Data Pipeline"
    )
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--dry-run', action='store_true', help='Fetch and parse but don\'t upload')

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(debug=args.debug)

    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║  ENTSO-E Imbalance Data Pipeline                         ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.dry_run:
        logger.info("DRY RUN MODE - No data will be uploaded")

    logger.info("")

    # Calculate time period
    period_start, period_end = get_preceding_hour_range()
    logger.info(f"Period: {period_start.strftime('%Y-%m-%d %H:%M')} to {period_end.strftime('%Y-%m-%d %H:%M')}")

    # Setup file paths
    start_str = period_start.strftime('%Y%m%d%H%M')
    end_str = period_end.strftime('%Y%m%d%H%M')

    output_dir = Path(f'/app/scripts/entsoe/data/{period_start.year}/{period_start.month:02d}')
    output_dir.mkdir(parents=True, exist_ok=True)

    prices_file = output_dir / f'entsoe_imbalance_prices_{start_str}_{end_str}.xml'
    volumes_file = output_dir / f'entsoe_imbalance_volumes_{start_str}_{end_str}.xml'

    # Initialize client
    logger.info("Initializing ENTSO-E client...")
    try:
        client = EntsoeClient()
        logger.info("✓ Client initialized")
    except Exception as e:
        logger.error(f"✗ Client initialization failed: {e}")
        sys.exit(1)

    # Fetch prices
    logger.info("")
    logger.info("Fetching Imbalance Prices (A85)...")
    try:
        xml_prices = client.fetch_data('A85', period_start, period_end)
        with open(prices_file, 'w') as f:
            f.write(xml_prices)
        logger.info(f"✓ Saved: {prices_file.name}")
    except Exception as e:
        logger.error(f"✗ Failed to fetch prices: {e}")
        sys.exit(1)

    # Fetch volumes
    logger.info("")
    logger.info("Fetching Imbalance Volumes (A86)...")
    try:
        xml_volumes = client.fetch_data('A86', period_start, period_end)
        with open(volumes_file, 'w') as f:
            f.write(xml_volumes)
        logger.info(f"✓ Saved: {volumes_file.name}")
    except Exception as e:
        logger.error(f"✗ Failed to fetch volumes: {e}")
        sys.exit(1)

    # Parse data
    logger.info("")
    logger.info("Parsing XML data...")
    data_parser = ImbalanceDataParser()

    try:
        data_parser.parse_prices_xml(str(prices_file))
        data_parser.parse_volumes_xml(str(volumes_file))
        data_parser.combine_data()
        logger.info(f"✓ Parsed {len(data_parser.combined_data)} records")
    except Exception as e:
        logger.error(f"✗ Parsing failed: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    # Upload to database
    if not args.dry_run:
        logger.info("")
        logger.info("Connecting to database...")
        conn = connect_database(logger)

        if not conn:
            logger.error("Cannot proceed without database connection")
            sys.exit(1)

        try:
            # Upload data (table must already exist)
            logger.info("")
            logger.info("Uploading to database...")
            inserted, skipped = upload_to_database(conn, data_parser, logger, dry_run=False)

            logger.info(f"✓ Upload complete:")
            logger.info(f"  - Inserted: {inserted} records")
            logger.info(f"  - Skipped (duplicates): {skipped} records")

        finally:
            conn.close()
            logger.info("Database connection closed")
    else:
        logger.info("")
        logger.info("DRY RUN - Skipping database upload")
        logger.info(f"Would process {len(data_parser.combined_data)} records")

    # Summary
    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║  Pipeline Completed Successfully                          ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")

    sys.exit(0)


if __name__ == '__main__':
    main()
