#!/usr/bin/env python3
"""
One-time backfill: Download VDT_STANDARD_OBCHODY Excel files from OTE-CR.

Usage:
    python3 backfill_download_pblctradeconf.py [START_DATE END_DATE]

Defaults to 2025-01-01 to 2025-10-17 (DB has WS data from 2025-10-18 onward).

URL pattern:
    https://www.ote-cr.cz/pubweb/attachments/27/{YYYY}/month{MM}/day{DD}/VDT_STANDARD_OBCHODY_{DD}_{MM}_{YYYY}_CZ.xlsx
"""

import sys
import random
import time
from datetime import datetime
from pathlib import Path

from common import (
    setup_logging,
    parse_date,
    date_range,
    download_file,
    validate_date_range,
    print_banner,
)

DEFAULT_START = "2025-01-01"
DEFAULT_END = "2025-10-17"
BASE_SUBDIR = "pblctradeconf"


def build_download_url(date):
    """Build OTE-CR download URL for VDT_STANDARD_OBCHODY."""
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')
    base_url = "https://www.ote-cr.cz/pubweb/attachments/27"
    filename = f"VDT_STANDARD_OBCHODY_{day}_{month}_{year}_CZ.xlsx"
    return f"{base_url}/{year}/month{month}/day{day}/{filename}"


def build_filename(date):
    return f"VDT_STANDARD_OBCHODY_{date.strftime('%d_%m_%Y')}_CZ.xlsx"


def main():
    debug_mode = '--debug' in sys.argv
    args = [arg for arg in sys.argv[1:] if arg != '--debug']

    if len(args) == 0:
        start_date = parse_date(DEFAULT_START)
        end_date = parse_date(DEFAULT_END)
    elif len(args) == 2:
        start_date = parse_date(args[0])
        end_date = parse_date(args[1])
    else:
        print("Usage: python3 backfill_download_pblctradeconf.py [START_DATE END_DATE]")
        sys.exit(1)

    validate_date_range(start_date, end_date)
    logger = setup_logging(debug=debug_mode)
    print_banner("pblctradeconf Excel Backfill Downloader", debug_mode)

    script_dir = Path(__file__).parent.absolute()
    base_dir = script_dir / BASE_SUBDIR

    dates = list(date_range(start_date, end_date))
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Total files to download: {len(dates)}")

    successful = 0
    skipped = 0
    failed = 0

    try:
        for i, date in enumerate(dates, 1):
            year = date.strftime('%Y')
            month = date.strftime('%m')
            target_dir = base_dir / year / month
            target_dir.mkdir(parents=True, exist_ok=True)

            filename = build_filename(date)
            target_file = target_dir / filename

            if target_file.exists() and target_file.stat().st_size > 0:
                logger.debug(f"[{i}/{len(dates)}] Already exists: {filename}")
                skipped += 1
                continue

            url = build_download_url(date)
            logger.info(f"[{i}/{len(dates)}] {date.strftime('%Y-%m-%d')}")

            if download_file(url, target_file, logger):
                successful += 1
            else:
                failed += 1

            if i < len(dates):
                wait_time = random.randint(1, 4)
                time.sleep(wait_time)

        logger.info(f"\n{'=' * 60}")
        logger.info(f"DOWNLOAD SUMMARY")
        logger.info(f"{'=' * 60}")
        logger.info(f"Total dates:  {len(dates)}")
        logger.info(f"Downloaded:   {successful}")
        logger.info(f"Skipped:      {skipped}")
        logger.info(f"Failed/404:   {failed}")
        logger.info(f"{'=' * 60}")

    except KeyboardInterrupt:
        logger.warning("\nDownload interrupted by user")
        sys.exit(1)


if __name__ == '__main__':
    main()
