#!/usr/bin/env python3
"""
Download intraday market reports from OTE-CR website.

Usage:
    python3 download_intraday_prices.py [START_DATE END_DATE] [--debug]

Examples:
    # Auto mode - downloads missing files from last download to yesterday
    python3 download_intraday_prices.py
    python3 download_intraday_prices.py --debug

    # Manual mode - specify date range
    python3 download_intraday_prices.py 2025-10-15 2025-10-31
    python3 download_intraday_prices.py 2025-10-15 2025-10-31 --debug

Notes:
    - Dates should be in YYYY-MM-DD format
    - In auto mode, downloads from last downloaded file date + 1 to yesterday
    - If no files exist, downloads from 2025-11-01 to yesterday
"""

import sys
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

from common import (
    setup_logging,
    parse_date,
    date_range,
    download_file,
    validate_date_range,
    print_banner,
    auto_determine_date_range,
    run_upload_script
)


def build_download_url(date):
    """
    Build the OTE-CR download URL for intraday market prices.

    URL pattern: https://www.ote-cr.cz/pubweb/attachments/27/{YYYY}/month{MM}/day{DD}/IM_15MIN_{DD}_{MM}_{YYYY}_EN.xlsx

    Args:
        date: datetime object

    Returns:
        str: Full download URL
    """
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')

    base_url = "https://www.ote-cr.cz/pubweb/attachments/27"
    url_path = f"{year}/month{month}/day{day}"
    filename = f"IM_15MIN_{day}_{month}_{year}_EN.xlsx"

    return f"{base_url}/{url_path}/{filename}"


def download_report(date, base_dir, logger):
    """
    Download intraday market report for a specific date.

    Args:
        date: datetime object
        base_dir: Base directory for organizing files (prices_intraday)
        logger: Logger instance

    Returns:
        bool: True if successful, False otherwise
    """
    # Create year/month directory structure
    year = date.strftime('%Y')
    month = date.strftime('%m')
    target_dir = base_dir / year / month
    target_dir.mkdir(parents=True, exist_ok=True)

    # Build download URL and filename
    url = build_download_url(date)
    filename = f"IM_15MIN_{date.strftime('%d_%m_%Y')}_EN.xlsx"
    target_file = target_dir / filename

    date_str = date.strftime('%Y-%m-%d')
    logger.info(f"\n{'─' * 60}")
    logger.info(f"Date: {date_str}")
    logger.info(f"Target directory: {target_dir}")

    return download_file(url, target_file, logger)


def main():
    """Main function."""
    # Parse command-line arguments
    debug_mode = '--debug' in sys.argv
    args = [arg for arg in sys.argv[1:] if arg != '--debug']

    # Check if running in auto mode or manual mode
    auto_mode = len(args) == 0
    manual_mode = len(args) == 2

    if not auto_mode and not manual_mode:
        print("Usage: python3 download_intraday_prices.py [START_DATE END_DATE] [--debug]")
        print("\nExamples:")
        print("  # Auto mode - downloads missing files")
        print("  python3 download_intraday_prices.py")
        print("  python3 download_intraday_prices.py --debug")
        print("\n  # Manual mode - specify date range")
        print("  python3 download_intraday_prices.py 2025-10-15 2025-10-31")
        print("  python3 download_intraday_prices.py 2025-10-15 2025-10-31 --debug")
        print("\nDates should be in YYYY-MM-DD format")
        sys.exit(1)

    # Setup logging
    logger = setup_logging(debug=debug_mode)

    # Get the script's directory (prices_intraday)
    script_dir = Path(__file__).parent.absolute()

    if auto_mode:
        print_banner("OTE-CR Intraday Market Report Downloader (AUTO)", debug_mode)
        logger.info("\nRunning in AUTO mode - determining date range automatically...\n")

        # Regex pattern to extract date from filename (DD_MM_YYYY)
        date_pattern = r'IM_15MIN_(\d{2})_(\d{2})_(\d{4})_EN\.xlsx'

        start_date, end_date = auto_determine_date_range(
            base_dir=script_dir,
            file_pattern="IM_15MIN_*.xlsx",
            date_pattern=date_pattern,
            logger=logger,
            minimum_date=datetime(2025, 11, 1)
        )

        if start_date is None or end_date is None:
            logger.info("\nNothing to download.")
            # Still run upload to process any existing files
            logger.info("Running upload for existing files...")
            # Use last 30 days as range to check for unuploaded files
            end_date_upload = datetime.now() - timedelta(days=1)
            start_date_upload = end_date_upload - timedelta(days=30)
            run_upload_script(
                upload_script_name='upload_intraday_prices.py',
                base_dir=script_dir,
                start_date=start_date_upload,
                end_date=end_date_upload,
                logger=logger
            )
            sys.exit(0)

    else:
        # Manual mode
        start_date_str = args[0]
        end_date_str = args[1]

        print_banner("OTE-CR Intraday Market Report Downloader (MANUAL)", debug_mode)
        logger.info(f"\nRunning in MANUAL mode")
        logger.info(f"Date range: {start_date_str} to {end_date_str}")

        # Parse dates
        start_date = parse_date(start_date_str)
        end_date = parse_date(end_date_str)

        # Validate date range
        validate_date_range(start_date, end_date)

    dates = list(date_range(start_date, end_date))
    logger.info(f"Total reports to download: {len(dates)}")
    logger.info(f"Base directory: {script_dir}")
    logger.info(f"Files will be organized in: YYYY/MM/ structure\n")

    successful = 0
    failed = 0

    try:
        # Download reports for each date
        for i, date in enumerate(dates, 1):
            logger.info(f"\nProgress: {i}/{len(dates)}")

            success = download_report(date, script_dir, logger)

            if success:
                successful += 1
                logger.info("✓ Download successful")
            else:
                failed += 1

            # Wait random time between 1-4 seconds between downloads (except for the last one)
            if i < len(dates):
                wait_time = random.randint(1, 4)
                logger.debug(f"Waiting {wait_time} seconds before next download...")
                time.sleep(wait_time)

        # Summary
        logger.info(f"\n{'═' * 60}")
        logger.info("DOWNLOAD SUMMARY")
        logger.info(f"{'═' * 60}")
        logger.info(f"Total reports processed: {len(dates)}")
        logger.info(f"Successful downloads: {successful}")
        logger.info(f"Failed downloads: {failed}")
        logger.info(f"Base directory: {script_dir}")
        logger.info(f"{'═' * 60}\n")

        # Upload downloaded files to database (runs even if some downloads failed)
        if successful > 0:
            run_upload_script(
                upload_script_name='upload_intraday_prices.py',
                base_dir=script_dir,
                start_date=start_date,
                end_date=end_date,
                logger=logger
            )
        else:
            logger.warning("No files were downloaded successfully. Skipping upload.")

    except KeyboardInterrupt:
        logger.warning("\n\nDownload interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nFatal error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
