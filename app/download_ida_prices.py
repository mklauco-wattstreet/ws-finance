#!/usr/bin/env python3
"""
Download OTE Intraday Auction (IDA) price reports from OTE-CR website.

Usage:
    python3 download_ida_prices.py --ida N [START_DATE END_DATE] [--debug]

Examples:
    # Auto mode - downloads missing files for IDA1
    python3 download_ida_prices.py --ida 1
    python3 download_ida_prices.py --ida 1 --debug

    # Manual mode - specify date range
    python3 download_ida_prices.py --ida 1 2026-03-01 2026-03-21
    python3 download_ida_prices.py --ida 2 2026-03-01 2026-03-21 --debug

Notes:
    - Dates should be in YYYY-MM-DD format
    - --ida N is required (N = 1, 2, or 3)
    - In auto mode, downloads from last downloaded file date + 1 to yesterday
    - If no files exist, downloads from 2026-03-01 to yesterday
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


def build_download_url(date, ida_idx):
    """
    Build the OTE-CR download URL for IDA prices.

    URL pattern: https://www.ote-cr.cz/pubweb/attachments/431/{YYYY}/month{MM}/day{DD}/IDA{n}_{DD}_{MM}_{YYYY}_EN.xlsx
    """
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')

    base_url = "https://www.ote-cr.cz/pubweb/attachments/431"
    url_path = f"{year}/month{month}/day{day}"
    filename = f"IDA{ida_idx}_{day}_{month}_{year}_EN.xlsx"

    return f"{base_url}/{url_path}/{filename}"


def download_report(date, base_dir, ida_idx, logger):
    """
    Download IDA report for a specific date.

    Args:
        date: datetime object
        base_dir: Base directory for IDA files (e.g. script_dir/IDA1)
        ida_idx: IDA index (1, 2, or 3)
        logger: Logger instance

    Returns:
        bool: True if successful, False otherwise
    """
    year = date.strftime('%Y')
    month = date.strftime('%m')
    target_dir = base_dir / year / month
    target_dir.mkdir(parents=True, exist_ok=True)

    url = build_download_url(date, ida_idx)
    filename = f"IDA{ida_idx}_{date.strftime('%d_%m_%Y')}_EN.xlsx"
    target_file = target_dir / filename

    return download_file(url, target_file, logger)


def run_ida_upload(script_dir, ida_base_dir, ida_idx, start_date, end_date, logger):
    """
    Run upload script for the IDA date range.
    Uploads files from all year/month directories between start and end date.

    Returns:
        list: Upload output lines
    """
    import os
    import subprocess

    year_months = set()
    current = start_date
    while current <= end_date:
        year_month = f"{current.strftime('%Y')}/{current.strftime('%m')}"
        year_months.add(year_month)
        current += timedelta(days=1)

    upload_lines = []
    for year_month in sorted(year_months):
        dir_path = ida_base_dir / year_month

        if not dir_path.exists():
            logger.warning(f"Upload dir not found: {dir_path}")
            continue

        try:
            upload_script = script_dir / 'upload_ida_prices.py'
            # Pass relative path from script_dir: IDA{n}/YYYY/MM
            rel_path = f"IDA{ida_idx}/{year_month}"
            result = subprocess.run(
                ['/usr/local/bin/python3', str(upload_script), rel_path, '--ida', str(ida_idx)],
                cwd=script_dir,
                capture_output=True,
                text=True,
                timeout=300,
                env=os.environ.copy()
            )

            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        upload_lines.append(line.strip())

            if result.returncode != 0:
                logger.error(f"Upload failed for IDA{ida_idx} {year_month}")
                if result.stderr:
                    logger.error(f"  stderr: {result.stderr.strip()}")

        except subprocess.TimeoutExpired:
            logger.error(f"Upload timeout for IDA{ida_idx} {year_month}")
        except Exception as e:
            logger.error(f"Upload error for IDA{ida_idx} {year_month}: {e}")

    return upload_lines


def main():
    """Main function."""
    # Parse arguments
    debug_mode = '--debug' in sys.argv
    raw_args = [arg for arg in sys.argv[1:] if arg != '--debug']

    # Extract --ida N
    ida_idx = None
    date_args = []
    i = 0
    while i < len(raw_args):
        if raw_args[i] == '--ida' and i + 1 < len(raw_args):
            try:
                ida_idx = int(raw_args[i + 1])
            except ValueError:
                print("Error: --ida must be followed by 1, 2, or 3")
                sys.exit(1)
            i += 2
        else:
            date_args.append(raw_args[i])
            i += 1

    if ida_idx not in (1, 2, 3):
        print("Error: --ida N is required (N must be 1, 2, or 3)")
        print("\nUsage: python3 download_ida_prices.py --ida N [START_DATE END_DATE] [--debug]")
        print("\nExamples:")
        print("  python3 download_ida_prices.py --ida 1")
        print("  python3 download_ida_prices.py --ida 1 2026-03-01 2026-03-21")
        sys.exit(1)

    auto_mode = len(date_args) == 0
    manual_mode = len(date_args) == 2

    if not auto_mode and not manual_mode:
        print("Usage: python3 download_ida_prices.py --ida N [START_DATE END_DATE] [--debug]")
        sys.exit(1)

    logger = setup_logging(debug=debug_mode)

    script_dir = Path(__file__).parent.absolute()
    ida_base_dir = script_dir / f"IDA{ida_idx}"

    if auto_mode:
        print_banner(f"OTE-CR IDA{ida_idx} Downloader (AUTO)", debug_mode)

        date_pattern = rf'IDA{ida_idx}_(\d{{2}})_(\d{{2}})_(\d{{4}})_EN\.xlsx'
        file_pattern = f"IDA{ida_idx}_*.xlsx"

        start_date, end_date = auto_determine_date_range(
            base_dir=ida_base_dir,
            file_pattern=file_pattern,
            date_pattern=date_pattern,
            logger=logger,
            minimum_date=datetime(2026, 3, 1),
            end_date_offset=0,
            redownload_latest=False
        )

        if start_date is None or end_date is None:
            sys.exit(0)

    else:
        print_banner(f"OTE-CR IDA{ida_idx} Downloader (MANUAL)", debug_mode)

        start_date = parse_date(date_args[0])
        end_date = parse_date(date_args[1])
        validate_date_range(start_date, end_date)

        logger.info(f"OTE IDA{ida_idx} MANUAL {start_date.strftime('%Y-%m-%d')}..{end_date.strftime('%Y-%m-%d')}")

    dates = list(date_range(start_date, end_date))

    successful = 0
    failed = 0

    try:
        for i, date in enumerate(dates, 1):
            success = download_report(date, ida_base_dir, ida_idx, logger)

            if success:
                successful += 1
            else:
                failed += 1

            if i < len(dates):
                time.sleep(0.1)

        summary = f"OTE IDA{ida_idx}: downloaded {successful}/{len(dates)}"
        if failed > 0:
            summary += f" ({failed} failed)"

        if successful > 0:
            upload_lines = run_ida_upload(script_dir, ida_base_dir, ida_idx, start_date, end_date, logger)
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
