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
from datetime import datetime, timedelta, timezone

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


def get_three_hour_range():
    """
    Get 3-hour time range starting from current time rounded down to nearest 15 minutes.

    ENTSO-E API requires UTC times. Container runs in CET, so we convert to UTC.
    We fetch 3 hours of data to ensure we have current data.

    Returns:
        tuple: (period_start, period_end) as datetime objects in UTC
    """
    # Get current time in UTC
    now_utc = datetime.now(timezone.utc)

    # Round down to nearest 15 minutes
    minutes = (now_utc.minute // 15) * 15
    period_end = now_utc.replace(minute=minutes, second=0, microsecond=0)

    # 3 hours before
    period_start = period_end - timedelta(hours=3)

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


def upload_to_database(conn, parser, logger, dry_run=False):
    """
    Upload parsed data to database using UPSERT.

    Args:
        conn: Database connection
        parser: ImbalanceDataParser with combined_data
        logger: Logger instance
        dry_run: If True, don't actually insert/update

    Returns:
        tuple: (upserted_count, upserted_records)
    """
    if not parser.combined_data:
        logger.warning("No data to upload")
        return 0, []

    cursor = conn.cursor()
    upserted = 0
    upserted_records = []

    upsert_query = """
        INSERT INTO entsoe_imbalance_prices (
            trade_date, period, time_interval,
            pos_imb_price_czk_mwh, pos_imb_scarcity_czk_mwh,
            pos_imb_incentive_czk_mwh, pos_imb_financial_neutrality_czk_mwh,
            neg_imb_price_czk_mwh, neg_imb_scarcity_czk_mwh,
            neg_imb_incentive_czk_mwh, neg_imb_financial_neutrality_czk_mwh,
            imbalance_mwh, difference_mwh, situation, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_date, time_interval)
        DO UPDATE SET
            period = EXCLUDED.period,
            pos_imb_price_czk_mwh = EXCLUDED.pos_imb_price_czk_mwh,
            pos_imb_scarcity_czk_mwh = EXCLUDED.pos_imb_scarcity_czk_mwh,
            pos_imb_incentive_czk_mwh = EXCLUDED.pos_imb_incentive_czk_mwh,
            pos_imb_financial_neutrality_czk_mwh = EXCLUDED.pos_imb_financial_neutrality_czk_mwh,
            neg_imb_price_czk_mwh = EXCLUDED.neg_imb_price_czk_mwh,
            neg_imb_scarcity_czk_mwh = EXCLUDED.neg_imb_scarcity_czk_mwh,
            neg_imb_incentive_czk_mwh = EXCLUDED.neg_imb_incentive_czk_mwh,
            neg_imb_financial_neutrality_czk_mwh = EXCLUDED.neg_imb_financial_neutrality_czk_mwh,
            imbalance_mwh = EXCLUDED.imbalance_mwh,
            difference_mwh = EXCLUDED.difference_mwh,
            situation = EXCLUDED.situation,
            status = EXCLUDED.status
    """

    for record in parser.combined_data:
        time_interval = record['time_interval']
        period = record['period']

        if dry_run:
            upserted += 1
            upserted_records.append((time_interval, period))
            continue

        # Insert or update record
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

            cursor.execute(upsert_query, values)
            upserted += 1
            upserted_records.append((time_interval, period))

        except Exception as e:
            logger.error(f"Failed to upsert {time_interval} (Period {period}): {e}")
            conn.rollback()
            continue

    if not dry_run:
        conn.commit()

    cursor.close()
    return upserted, upserted_records


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

    # Calculate time period (UTC)
    period_start, period_end = get_three_hour_range()
    logger.info(f"Period (UTC): {period_start.strftime('%Y-%m-%d %H:%M')} to {period_end.strftime('%Y-%m-%d %H:%M')}")

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
            upserted, upserted_records = upload_to_database(conn, data_parser, logger, dry_run=False)

            logger.info(f"✓ Upload complete:")
            logger.info(f"  - Upserted: {upserted} records")

            if upserted_records:
                logger.info("")
                logger.info("Upserted intervals:")
                for time_interval, period in upserted_records:
                    logger.info(f"  • {time_interval} (Period {period})")

        finally:
            conn.close()
            logger.info("")
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
