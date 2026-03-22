#!/usr/bin/env python3
"""
Download day-ahead 60-minute price reports from OTE-CR website.

Usage:
    python3 download_day_ahead_60min_prices.py [START_DATE END_DATE] [--debug]

Examples:
    # Auto mode - downloads missing files from last download to yesterday
    python3 download_day_ahead_60min_prices.py
    python3 download_day_ahead_60min_prices.py --debug

    # Manual mode - specify date range
    python3 download_day_ahead_60min_prices.py 2026-03-01 2026-03-22
    python3 download_day_ahead_60min_prices.py 2026-03-01 2026-03-22 --debug

Notes:
    - Dates should be in YYYY-MM-DD format
    - In auto mode, downloads from last downloaded file date + 1 to yesterday
    - If no files exist, downloads from 2025-11-01 to yesterday
    - Historical files may be .xls instead of .xlsx (automatic fallback)
"""

import sentry_init  # noqa: F401 - must be first to capture errors
sentry_init.set_module("ote")
import sys
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
    Build the OTE-CR download URL for a specific date.

    URL pattern: https://www.ote-cr.cz/pubweb/attachments/01/{YYYY}/month{MM}/day{DD}/DM_60MIN_{DD}_{MM}_{YYYY}_EN.xlsx

    Args:
        date: datetime object

    Returns:
        str: Full download URL
    """
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')

    base_url = "https://www.ote-cr.cz/pubweb/attachments/01"
    url_path = f"{year}/month{month}/day{day}"
    filename = f"DM_60MIN_{day}_{month}_{year}_EN.xlsx"

    return f"{base_url}/{url_path}/{filename}"


def get_filename(date):
    """
    Get the filename for a specific date.

    Args:
        date: datetime object

    Returns:
        str: Filename
    """
    return f"DM_60MIN_{date.strftime('%d_%m_%Y')}_EN.xlsx"


def download_report(date, base_dir, logger):
    """
    Download day-ahead 60min report for a specific date.
    Falls back to .xls if .xlsx returns 404.

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

    success = download_file(url, target_file, logger)
    if not success:
        url_xls = url.replace('.xlsx', '.xls')
        filename_xls = filename.replace('.xlsx', '.xls')
        target_file_xls = target_dir / filename_xls
        success = download_file(url_xls, target_file_xls, logger)

    return success


def main():
    """Main function."""
    debug_mode = '--debug' in sys.argv
    args = [arg for arg in sys.argv[1:] if arg != '--debug']

    auto_mode = len(args) == 0
    manual_mode = len(args) == 2

    if not auto_mode and not manual_mode:
        print("Usage: python3 download_day_ahead_60min_prices.py [START_DATE END_DATE] [--debug]")
        print("\nExamples:")
        print("  # Auto mode - downloads missing files")
        print("  python3 download_day_ahead_60min_prices.py")
        print("  python3 download_day_ahead_60min_prices.py --debug")
        print("\n  # Manual mode - specify date range")
        print("  python3 download_day_ahead_60min_prices.py 2026-03-01 2026-03-22")
        print("  python3 download_day_ahead_60min_prices.py 2026-03-01 2026-03-22 --debug")
        print("\nDates should be in YYYY-MM-DD format")
        sys.exit(1)

    logger = setup_logging(debug=debug_mode)

    script_dir = Path(__file__).parent.absolute()

    if auto_mode:
        print_banner("OTE-CR Day-Ahead 60min Price Report Downloader (AUTO)", debug_mode)

        date_pattern = r'(\d{2})_(\d{2})_(\d{4})_EN\.xlsx?'

        start_date, end_date = auto_determine_date_range(
            base_dir=script_dir,
            file_pattern="DM_60MIN_*.xlsx",
            date_pattern=date_pattern,
            logger=logger,
            minimum_date=datetime(2025, 11, 1)
        )

        if start_date is None or end_date is None:
            end_date_upload = datetime.now() - timedelta(days=1)
            start_date_upload = end_date_upload - timedelta(days=30)
            _, upload_lines = run_upload_script(
                upload_script_name='upload_day_ahead_60min_prices.py',
                base_dir=script_dir,
                start_date=start_date_upload,
                end_date=end_date_upload,
                logger=logger
            )
            for line in upload_lines:
                logger.info(line)
            sys.exit(0)

    else:
        start_date_str = args[0]
        end_date_str = args[1]

        print_banner("OTE-CR Day-Ahead 60min Price Report Downloader (MANUAL)", debug_mode)

        start_date = parse_date(start_date_str)
        end_date = parse_date(end_date_str)

        validate_date_range(start_date, end_date)

        logger.info(f"OTE DayAhead60 MANUAL {start_date.strftime('%Y-%m-%d')}..{end_date.strftime('%Y-%m-%d')}")

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

            if i < len(dates):
                time.sleep(0.1)

        summary = f"OTE DayAhead60: downloaded {successful}/{len(dates)}"
        if failed > 0:
            summary += f" ({failed} failed)"

        if successful > 0:
            _, upload_lines = run_upload_script(
                upload_script_name='upload_day_ahead_60min_prices.py',
                base_dir=script_dir,
                start_date=start_date,
                end_date=end_date,
                logger=logger
            )
            upload_summary = next((l for l in upload_lines if 'upload:' in l.lower()), None)
            if upload_summary:
                summary += f" | {upload_summary}"
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
