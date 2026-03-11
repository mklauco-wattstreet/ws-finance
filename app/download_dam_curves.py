#!/usr/bin/env python3
"""
Download DAM matching curve XML reports from OTE-CR website.

Usage:
    python3 download_dam_curves.py [START_DATE END_DATE] [--debug]

Examples:
    # Auto mode - downloads missing files from last download to yesterday
    python3 download_dam_curves.py
    python3 download_dam_curves.py --debug

    # Manual mode - specify date range
    python3 download_dam_curves.py 2026-01-01 2026-03-01
    python3 download_dam_curves.py 2026-01-01 2026-03-01 --debug

Notes:
    - Dates should be in YYYY-MM-DD format
    - In auto mode, downloads from last downloaded file date + 1 to yesterday
    - If no files exist, downloads from 2026-01-01 to yesterday
"""

import sentry_init  # noqa: F401 - must be first to capture errors
sentry_init.set_module("ote")
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
    Build the OTE-CR download URL for DAM matching curve XML.

    URL format: https://www.ote-cr.cz/pubweb/attachments/03/{YYYY}/month{MM}/day{DD}/MC_{DD}_{MM}_{YYYY}_EN.xml

    Args:
        date: datetime object

    Returns:
        str: Full download URL
    """
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')

    base_url = "https://www.ote-cr.cz/pubweb/attachments/03"
    url_path = f"{year}/month{month}/day{day}"
    filename = f"MC_{day}_{month}_{year}_EN.xml"

    return f"{base_url}/{url_path}/{filename}"


def get_filename(date):
    """
    Get the filename for a specific date.

    Args:
        date: datetime object

    Returns:
        str: Filename
    """
    return f"MC_{date.strftime('%d_%m_%Y')}_EN.xml"


def download_report(date, base_dir, logger):
    """
    Download DAM matching curve XML for a specific date.

    Args:
        date: datetime object
        base_dir: Base directory for organizing files
        logger: Logger instance

    Returns:
        bool: True if successful, False otherwise
    """
    year = date.strftime('%Y')
    month = date.strftime('%m')
    target_dir = base_dir / year / month
    target_dir.mkdir(parents=True, exist_ok=True)

    url = build_download_url(date)
    filename = get_filename(date)
    target_file = target_dir / filename

    if target_file.exists() and target_file.stat().st_size > 0:
        logger.debug(f"Already exists: {filename}")
        return True

    return download_file(url, target_file, logger)


def main():
    """Main function."""
    debug_mode = '--debug' in sys.argv
    no_delay = '--no-delay' in sys.argv
    args = [arg for arg in sys.argv[1:] if arg not in ('--debug', '--no-delay')]

    auto_mode = len(args) == 0
    manual_mode = len(args) == 2

    if not auto_mode and not manual_mode:
        print("Usage: python3 download_dam_curves.py [START_DATE END_DATE] [--debug]")
        print("\nExamples:")
        print("  # Auto mode - downloads missing files")
        print("  python3 download_dam_curves.py")
        print("  python3 download_dam_curves.py --debug")
        print("\n  # Manual mode - specify date range")
        print("  python3 download_dam_curves.py 2026-01-01 2026-03-01")
        print("  python3 download_dam_curves.py 2026-01-01 2026-03-01 --debug")
        print("\nDates should be in YYYY-MM-DD format")
        sys.exit(1)

    logger = setup_logging(debug=debug_mode)
    script_dir = Path(__file__).parent.absolute()

    if auto_mode:
        print_banner("OTE-CR DAM Matching Curve Downloader (AUTO)", debug_mode)
        logger.info("OTE DAM Curves AUTO mode")

        date_pattern = r'(\d{2})_(\d{2})_(\d{4})_EN\.xml'

        start_date, end_date = auto_determine_date_range(
            base_dir=script_dir,
            file_pattern="MC_*.xml",
            date_pattern=date_pattern,
            logger=logger,
            minimum_date=datetime(2026, 1, 1),
            end_date_offset=1  # tomorrow (day-ahead auction results published ~14:00 today)
        )

        if start_date is None or end_date is None:
            end_date_upload = datetime.now() - timedelta(days=1)
            start_date_upload = end_date_upload - timedelta(days=30)
            run_upload_script(
                upload_script_name='upload_dam_curves.py',
                base_dir=script_dir,
                start_date=start_date_upload,
                end_date=end_date_upload,
                logger=logger
            )
            sys.exit(0)

    else:
        start_date_str = args[0]
        end_date_str = args[1]

        print_banner("OTE-CR DAM Matching Curve Downloader (MANUAL)", debug_mode)

        start_date = parse_date(start_date_str)
        end_date = parse_date(end_date_str)
        validate_date_range(start_date, end_date)

        logger.info(f"OTE DAM Curves MANUAL {start_date.strftime('%Y-%m-%d')}..{end_date.strftime('%Y-%m-%d')}")

    dates = list(date_range(start_date, end_date))

    successful = 0
    failed = 0

    try:
        for i, date in enumerate(dates, 1):
            success = download_report(date, script_dir, logger)

            if success:
                successful += 1
            else:
                failed += 1

            if i < len(dates) and not no_delay:
                wait_time = random.randint(1, 3)
                logger.debug(f"Waiting {wait_time}s...")
                time.sleep(wait_time)

        summary = f"OTE DAM Curves: downloaded {successful}/{len(dates)}"
        if failed > 0:
            summary += f" ({failed} failed)"
        logger.info(summary)

        if successful > 0:
            run_upload_script(
                upload_script_name='upload_dam_curves.py',
                base_dir=script_dir,
                start_date=start_date,
                end_date=end_date,
                logger=logger
            )
        else:
            logger.warning("No files downloaded. Skipping upload.")

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
