#!/usr/bin/env python3
"""
Test script to fetch ENTSO-E data for specific time periods.

This script allows you to specify custom start and end dates/times
to fetch historical or specific data from ENTSO-E API.

Usage:
    python3 test_fetch.py --start "2024-01-01 00:00" --end "2024-01-01 23:45"
    python3 test_fetch.py --start "2024-11-10 06:00" --end "2024-11-10 12:00" --document-type A85
"""

import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from entsoe.entsoe_client import EntsoeClient


def setup_logging(debug=False):
    """Setup logging configuration."""
    log_level = logging.DEBUG if debug else logging.INFO

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    return logging.getLogger(__name__)


def parse_datetime(date_str):
    """
    Parse datetime string in various formats.

    Supported formats:
        - "2024-01-01 00:00"
        - "2024-01-01T00:00"
        - "2024-01-01"

    Args:
        date_str: Date/time string

    Returns:
        datetime object
    """
    # Try different formats
    formats = [
        '%Y-%m-%d %H:%M',
        '%Y-%m-%dT%H:%M',
        '%Y-%m-%d',
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    raise ValueError(f"Unable to parse date: {date_str}. Use format: YYYY-MM-DD HH:MM")


def save_xml_file(xml_content, document_type, period_start, period_end, output_dir, logger):
    """Save XML content to file."""
    # Create output directory structure: YYYY/MM/
    year_month_dir = output_dir / period_start.strftime('%Y') / period_start.strftime('%m')
    year_month_dir.mkdir(parents=True, exist_ok=True)

    # Create filename
    doc_type_name = {
        'A85': 'imbalance_prices',
        'A86': 'imbalance_volumes'
    }.get(document_type, document_type)

    filename = f"entsoe_{doc_type_name}_{period_start.strftime('%Y%m%d_%H%M')}_{period_end.strftime('%Y%m%d_%H%M')}.xml"
    file_path = year_month_dir / filename

    # Save XML content
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(xml_content)

    logger.info(f"✓ Saved to: {file_path}")
    return file_path


def fetch_and_save_data(client, document_type, period_start, period_end, output_dir, logger):
    """Fetch data from ENTSO-E API and save XML file."""

    doc_type_name = {
        'A85': 'Imbalance Prices',
        'A86': 'Imbalance Volumes'
    }.get(document_type, document_type)

    logger.info(f"{'=' * 60}")
    logger.info(f"Fetching {doc_type_name} ({document_type})")
    logger.info(f"{'=' * 60}")
    logger.info(f"Period: {period_start.strftime('%Y-%m-%d %H:%M')} to {period_end.strftime('%Y-%m-%d %H:%M')}")

    try:
        # Fetch XML data
        logger.info(f"Fetching data from ENTSO-E API...")
        xml_content = client.fetch_data(document_type, period_start, period_end)
        logger.info(f"✓ Received {len(xml_content)} bytes of XML data")

        if not xml_content:
            logger.warning(f"No data received from API")
            return False

        # Save XML file
        logger.info(f"Saving XML file...")
        file_path = save_xml_file(xml_content, document_type, period_start, period_end, output_dir, logger)

        logger.info(f"✓ Successfully fetched and saved data")
        return True

    except Exception as e:
        logger.error(f"✗ Failed to fetch/save data: {e}")
        if logger.level <= logging.DEBUG:
            logger.exception("Full exception:")
        return False


def main():
    """Main function."""
    parser_args = argparse.ArgumentParser(
        description="Test fetch ENTSO-E data for specific time periods",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch data for a specific day
  python3 test_fetch.py --start "2024-11-10 00:00" --end "2024-11-10 23:45"

  # Fetch only prices for a few hours
  python3 test_fetch.py --start "2024-11-10 06:00" --end "2024-11-10 12:00" --document-type A85

  # Fetch only volumes with debug output
  python3 test_fetch.py --start "2024-11-10 00:00" --end "2024-11-10 01:00" --document-type A86 --debug

  # Fetch both prices and volumes for a week (will be multiple API calls)
  python3 test_fetch.py --start "2024-11-01 00:00" --end "2024-11-07 23:45"
"""
    )

    parser_args.add_argument(
        '--start',
        required=True,
        help='Start date/time (format: "YYYY-MM-DD HH:MM" or "YYYY-MM-DD")'
    )
    parser_args.add_argument(
        '--end',
        required=True,
        help='End date/time (format: "YYYY-MM-DD HH:MM" or "YYYY-MM-DD")'
    )
    parser_args.add_argument(
        '--document-type',
        choices=['A85', 'A86', 'all'],
        default='all',
        help='Document type to fetch (A85=prices, A86=volumes, all=both)'
    )
    parser_args.add_argument(
        '--output-dir',
        type=str,
        default='/app/scripts/entsoe/data',
        help='Output directory for XML files (default: /app/scripts/entsoe/data)'
    )
    parser_args.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )

    args = parser_args.parse_args()

    # Setup logging
    logger = setup_logging(debug=args.debug)

    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║  ENTSO-E Test Fetch Script                               ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")

    # Parse dates
    try:
        period_start = parse_datetime(args.start)
        period_end = parse_datetime(args.end)

        logger.info(f"Requested period:")
        logger.info(f"  Start: {period_start.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"  End: {period_end.strftime('%Y-%m-%d %H:%M')}")
        logger.info("")

        if period_start >= period_end:
            logger.error("✗ Start date must be before end date")
            sys.exit(1)

    except ValueError as e:
        logger.error(f"✗ Date parsing error: {e}")
        sys.exit(1)

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")
    logger.info("")

    # Initialize client
    try:
        logger.info("Initializing ENTSO-E client...")
        client = EntsoeClient()
        logger.info(f"✓ Client initialized")
        logger.info(f"  Security token: {client.security_token[:10]}...")
        logger.info(f"  Control area: {client.control_area_domain}")
        logger.info("")

    except Exception as e:
        logger.error(f"✗ Initialization failed: {e}")
        sys.exit(1)

    # Fetch and save data
    success = True

    if args.document_type in ['A85', 'all']:
        if not fetch_and_save_data(
            client,
            'A85',
            period_start,
            period_end,
            output_dir,
            logger
        ):
            success = False

    logger.info("")

    if args.document_type in ['A86', 'all']:
        if not fetch_and_save_data(
            client,
            'A86',
            period_start,
            period_end,
            output_dir,
            logger
        ):
            success = False

    # Summary
    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info(f"║  {'Completed Successfully' if success else 'Completed with Errors':<56} ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
