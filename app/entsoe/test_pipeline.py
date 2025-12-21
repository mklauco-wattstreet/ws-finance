#!/usr/bin/env python3
"""
Test the ENTSO-E Imbalance Pipeline with historical data.

Usage:
    python3 test_pipeline.py <start_period> <end_period> [--dry-run]

Example:
    python3 test_pipeline.py 202511100800 202511101200
    python3 test_pipeline.py 202511100800 202511101200 --dry-run
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from entsoe.client import EntsoeClient
from entsoe.parsers import ImbalanceParser
from entsoe.entsoe_imbalance_pipeline import (
    setup_logging,
    connect_database,
    upload_to_database
)


def main():
    parser = argparse.ArgumentParser(
        description="Test ENTSO-E Imbalance Pipeline with historical data"
    )
    parser.add_argument('start_period', help='Start period (YYYYMMDDHHmm)')
    parser.add_argument('end_period', help='End period (YYYYMMDDHHmm)')
    parser.add_argument('--dry-run', action='store_true', help='Don\'t upload to database')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(debug=args.debug)

    # Parse datetime (assume UTC)
    period_start = datetime.strptime(args.start_period, '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
    period_end = datetime.strptime(args.end_period, '%Y%m%d%H%M').replace(tzinfo=timezone.utc)

    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║  ENTSO-E Imbalance Pipeline TEST                         ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.dry_run:
        logger.info("DRY RUN MODE - No data will be uploaded")

    logger.info("")
    logger.info(f"Test Period: {period_start.strftime('%Y-%m-%d %H:%M')} to {period_end.strftime('%Y-%m-%d %H:%M')}")

    # Setup file paths
    start_str = period_start.strftime('%Y%m%d%H%M')
    end_str = period_end.strftime('%Y%m%d%H%M')

    output_dir = Path(f'/app/scripts/entsoe/data/{period_start.year}/{period_start.month:02d}')
    output_dir.mkdir(parents=True, exist_ok=True)

    prices_file = output_dir / f'entsoe_imbalance_prices_{start_str}_{end_str}.xml'
    volumes_file = output_dir / f'entsoe_imbalance_volumes_{start_str}_{end_str}.xml'

    # Initialize client
    logger.info("")
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
    data_parser = ImbalanceParser()

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

    # Display summary
    if data_parser.combined_data:
        logger.info("")
        logger.info("Data Summary:")
        logger.info(f"  Trade Date: {data_parser.combined_data[0]['trade_date']}")
        logger.info(f"  First Period: {data_parser.combined_data[0]['period']} ({data_parser.combined_data[0]['time_interval']})")
        logger.info(f"  Last Period: {data_parser.combined_data[-1]['period']} ({data_parser.combined_data[-1]['time_interval']})")
        logger.info(f"  Total Records: {len(data_parser.combined_data)}")

    # Upload to database
    if not args.dry_run:
        logger.info("")
        logger.info("Connecting to database...")
        conn = connect_database(logger)

        if not conn:
            logger.error("Cannot proceed without database connection")
            sys.exit(1)

        try:
            # Upload data
            logger.info("")
            logger.info("Uploading to database...")
            inserted, skipped, inserted_records = upload_to_database(conn, data_parser, logger, dry_run=False)

            logger.info(f"✓ Upload complete:")
            logger.info(f"  - Inserted: {inserted} records")
            logger.info(f"  - Skipped (duplicates): {skipped} records")

            if inserted_records:
                logger.info("")
                logger.info("Inserted intervals:")
                for time_interval, period in inserted_records:
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
    logger.info("║  Test Pipeline Completed Successfully                     ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")

    sys.exit(0)


if __name__ == '__main__':
    main()
