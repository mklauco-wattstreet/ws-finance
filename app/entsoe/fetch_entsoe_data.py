#!/usr/bin/env python3
"""
Main script to fetch ENTSO-E data and save XML files.

This script is designed to run every 15 minutes via cron.
It fetches the preceding 1 hour of data and saves XML files.

Usage:
    python3 fetch_entsoe_data.py [--debug]

Options:
    --debug         Enable debug logging
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
    """
    Setup logging configuration.

    Args:
        debug: If True, set log level to DEBUG, otherwise INFO
    """
    log_level = logging.DEBUG if debug else logging.INFO

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    return logging.getLogger(__name__)


def save_xml_file(xml_content, document_type, period_start, period_end, output_dir, logger):
    """
    Save XML content to file.

    Args:
        xml_content: XML content as string
        document_type: Document type (A85 or A86)
        period_start: Start datetime
        period_end: End datetime
        output_dir: Output directory path
        logger: Logger instance

    Returns:
        Path: Path to saved file
    """
    # Create output directory structure: YYYY/MM/
    year_month_dir = output_dir / period_start.strftime('%Y') / period_start.strftime('%m')
    year_month_dir.mkdir(parents=True, exist_ok=True)

    # Create filename
    doc_type_name = {
        'A85': 'imbalance_prices',
        'A86': 'imbalance_volumes'
    }.get(document_type, document_type)

    filename = f"entsoe_{doc_type_name}_{period_start.strftime('%Y%m%d_%H%M')}_{period_end.strftime('%H%M')}.xml"
    file_path = year_month_dir / filename

    # Save XML content
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(xml_content)

    logger.info(f"✓ Saved to: {file_path}")
    return file_path


def fetch_and_save_data(client, document_type, data_type_name, output_dir, logger):
    """
    Fetch data from ENTSO-E API and save XML file.

    Args:
        client: EntsoeClient instance
        document_type: Document type (A85 or A86)
        data_type_name: Human-readable name for logging
        output_dir: Output directory path
        logger: Logger instance

    Returns:
        bool: True if successful, False otherwise
    """
    # Get preceding hour range
    period_start, period_end = client.get_preceding_hour_range()

    logger.info(f"{'=' * 60}")
    logger.info(f"Fetching {data_type_name}")
    logger.info(f"{'=' * 60}")
    logger.info(f"Period: {period_start.strftime('%Y-%m-%d %H:%M')} to {period_end.strftime('%Y-%m-%d %H:%M')}")

    try:
        # Fetch XML data
        logger.info(f"Fetching data from ENTSO-E API...")
        xml_content = client.fetch_data(document_type, period_start, period_end)
        logger.debug(f"Received {len(xml_content)} bytes of XML data")

        if not xml_content:
            logger.warning(f"No data received from API")
            return False

        # Save XML file
        logger.info(f"Saving XML file...")
        file_path = save_xml_file(xml_content, document_type, period_start, period_end, output_dir, logger)

        logger.info(f"✓ Successfully fetched and saved {data_type_name}")
        return True

    except Exception as e:
        logger.error(f"✗ Failed to fetch/save {data_type_name}: {e}")
        if logger.level <= logging.DEBUG:
            logger.exception("Full exception:")
        return False


def main():
    """Main function."""
    parser_args = argparse.ArgumentParser(
        description="Fetch ENTSO-E data and save XML files"
    )
    parser_args.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
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

    args = parser_args.parse_args()

    # Setup logging
    logger = setup_logging(debug=args.debug)

    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║  ENTSO-E Data Fetcher                                    ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")
    logger.info("")

    # Initialize client
    try:
        logger.info("Initializing ENTSO-E client...")
        client = EntsoeClient()
        logger.info(f"✓ Client initialized (control area: {client.control_area_domain})")
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
            'Imbalance Prices (A85)',
            output_dir,
            logger
        ):
            success = False

    if args.document_type in ['A86', 'all']:
        if not fetch_and_save_data(
            client,
            'A86',
            'Imbalance Volumes (A86)',
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
