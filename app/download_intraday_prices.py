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

import sentry_init  # noqa: F401 - must be first to capture errors
sentry_init.set_module("ote")
import sys
import os
import random
import subprocess
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
        Path to downloaded file on success, None on failure
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

    if download_file(url, target_file, logger):
        return target_file
    return None


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

        # Regex pattern to extract date from filename (DD_MM_YYYY)
        date_pattern = r'IM_15MIN_(\d{2})_(\d{2})_(\d{4})_EN\.xlsx'

        start_date, end_date = auto_determine_date_range(
            base_dir=script_dir,
            file_pattern="IM_15MIN_*.xlsx",
            date_pattern=date_pattern,
            logger=logger,
            minimum_date=datetime(2025, 11, 1),
            end_date_offset=0,  # Intraday: fetch TODAY's data (updated continuously)
            redownload_latest=True  # Re-download today's file to get latest updates
        )

        if start_date is None or end_date is None:
            sys.exit(0)

    else:
        # Manual mode
        start_date_str = args[0]
        end_date_str = args[1]

        print_banner("OTE-CR Intraday Market Report Downloader (MANUAL)", debug_mode)

        # Parse dates
        start_date = parse_date(start_date_str)
        end_date = parse_date(end_date_str)

        # Validate date range
        validate_date_range(start_date, end_date)

        logger.info(f"OTE Intraday MANUAL {start_date.strftime('%Y-%m-%d')}..{end_date.strftime('%Y-%m-%d')}")

    dates = list(date_range(start_date, end_date))

    successful = 0
    failed = 0
    downloaded_files = []

    try:
        # Download reports for each date
        for i, date in enumerate(dates, 1):
            result = download_report(date, script_dir, logger)

            if result:
                successful += 1
                downloaded_files.append(result)
            else:
                failed += 1

            # Wait random time between 1-4 seconds between downloads (except for the last one)
            if i < len(dates):
                wait_time = random.randint(1, 4)
                logger.debug(f"Waiting {wait_time}s...")
                time.sleep(wait_time)

        # One-line summary combining download + upload
        summary = f"OTE Intraday: downloaded {successful}/{len(dates)}"
        if failed > 0:
            summary += f" ({failed} failed)"

        if downloaded_files:
            # Upload only the files we just downloaded
            total_uploaded = 0
            for file_path in downloaded_files:
                result = subprocess.run(
                    ['/usr/local/bin/python3', str(script_dir / 'upload_intraday_prices.py'), str(file_path)],
                    cwd=script_dir,
                    capture_output=True,
                    text=True,
                    timeout=60,
                    env=os.environ.copy()
                )
                if result.returncode == 0:
                    total_uploaded += 1
                else:
                    logger.warning(f"Upload failed: {file_path.name}")
            summary += f" | uploaded {total_uploaded}/{len(downloaded_files)}"
        else:
            summary += " | skipped upload"
        logger.info(summary)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
