#!/usr/bin/env python3
"""
Download Slovak OKTE intraday market (IDM) reports.

Usage:
    python3 download_okte_idm.py [START_DATE END_DATE] [--days N] [--debug]

Examples:
    # Auto mode - trailing 3 days (today plus the 2 before it), re-fetched
    python3 download_okte_idm.py
    python3 download_okte_idm.py --days 7        # wider self-heal window
    python3 download_okte_idm.py --debug

    # Manual mode - specify date range (backfill)
    python3 download_okte_idm.py 2026-08-01 2026-08-10
    python3 download_okte_idm.py 2026-08-01 2026-08-10 --debug

Notes:
    - Dates should be in YYYY-MM-DD format
    - Auto mode covers a trailing window so an outage self-heals; a single
      request serves the whole window, so it costs no more than fetching today.
      Longer gaps still need a manual range.
    - The OKTE API returns a ZIP with two members ("15 min.csv", "60 min.csv")
      covering the whole requested range in one file each.
    - Date range is chunked into windows of at most 7 days, one zip per chunk.
"""

import sentry_init  # noqa: F401 - must be first to capture errors
sentry_init.set_module("okte")
import sys
import os
import re
import subprocess
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

from common import (
    setup_logging,
    parse_date,
    validate_date_range,
    download_file,
    print_banner,
)

BASE_URL = "https://isot.okte.sk/api/v1/idm/report"

# Auto mode fetches a trailing window, not just today: OKTE keeps revising a
# delivery day as it trades, and a container outage would otherwise leave a
# permanent hole (nothing ever revisits a past day). Upserts make it idempotent.
AUTO_DAYS_DEFAULT = 3


def build_download_url(start_date, end_date):
    """Build the OKTE IDM report URL for a date range."""
    return (
        f"{BASE_URL}?deliverydayfrom={start_date.strftime('%Y-%m-%d')}"
        f"&deliverydayto={end_date.strftime('%Y-%m-%d')}&lang=sk-SK&format=csv"
    )


def chunk_ranges(start_date, end_date, chunk_days=7):
    """Split a date range into windows of at most chunk_days."""
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def download_chunk(start_date, end_date, base_dir, logger):
    """
    Download one zip covering [start_date, end_date] and extract both members.

    Returns:
        list of Path to extracted CSV files (0, 1 or 2 entries)
    """
    year = start_date.strftime('%Y')
    month = start_date.strftime('%m')
    target_dir = base_dir / 'okte' / year / month
    target_dir.mkdir(parents=True, exist_ok=True)

    day_suffix = start_date.strftime('%d_%m_%Y')
    zip_path = target_dir / f"IDM_{day_suffix}.zip.tmp"
    url = build_download_url(start_date, end_date)

    logger.info(
        f"  Fetching {start_date.strftime('%Y-%m-%d')}..{end_date.strftime('%Y-%m-%d')} "
        f"({(end_date - start_date).days + 1} days)"
    )

    if not download_file(url, zip_path, logger):
        logger.error(f"  Download FAILED for {start_date.strftime('%Y-%m-%d')}..{end_date.strftime('%Y-%m-%d')}")
        return []

    extracted = []
    try:
        with zipfile.ZipFile(zip_path) as zf:
            member_map = {
                "15 min.csv": target_dir / f"IDM_15MIN_{day_suffix}.csv",
                "60 min.csv": target_dir / f"IDM_60MIN_{day_suffix}.csv",
            }
            for member_name, target_file in member_map.items():
                if member_name in zf.namelist():
                    with zf.open(member_name) as src, open(target_file, 'wb') as dst:
                        payload = src.read()
                        dst.write(payload)
                    logger.info(f"    extracted {target_file.name} ({len(payload):,} bytes)")
                    extracted.append(target_file)
                else:
                    logger.warning(f"    zip missing member: {member_name}")
    except zipfile.BadZipFile:
        logger.error(f"Bad zip file for range {start_date.date()}..{end_date.date()}")
    finally:
        zip_path.unlink(missing_ok=True)

    return extracted


def main():
    """Main function."""
    debug_mode = '--debug' in sys.argv

    # --days N: size of the trailing window used by auto mode (default 3).
    # One request covers the whole window, so widening it costs nothing extra
    # and lets a short outage self-heal on the next cron tick.
    auto_days = AUTO_DAYS_DEFAULT
    argv = sys.argv[1:]
    if '--days' in argv:
        idx = argv.index('--days')
        try:
            auto_days = int(argv[idx + 1])
            if auto_days < 1:
                raise ValueError
        except (IndexError, ValueError):
            print("Error: --days requires a positive integer")
            sys.exit(1)
        del argv[idx:idx + 2]

    args = [arg for arg in argv if arg != '--debug']

    auto_mode = len(args) == 0
    manual_mode = len(args) == 2

    if not auto_mode and not manual_mode:
        print("Usage: python3 download_okte_idm.py [START_DATE END_DATE] [--debug]")
        print("\nExamples:")
        print("  python3 download_okte_idm.py")
        print("  python3 download_okte_idm.py --debug")
        print("  python3 download_okte_idm.py 2026-08-01 2026-08-10")
        print("\nDates should be in YYYY-MM-DD format")
        sys.exit(1)

    logger = setup_logging(debug=debug_mode)
    script_dir = Path(__file__).parent.absolute()

    if auto_mode:
        print_banner("OKTE Intraday Market Downloader (AUTO)", debug_mode)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=auto_days - 1)
        logger.info(
            f"OKTE IDM AUTO {start_date.strftime('%Y-%m-%d')}"
            f"..{end_date.strftime('%Y-%m-%d')} (trailing {auto_days} days)"
        )
    else:
        print_banner("OKTE Intraday Market Downloader (MANUAL)", debug_mode)
        start_date = parse_date(args[0])
        end_date = parse_date(args[1])
        validate_date_range(start_date, end_date)
        logger.info(f"OKTE IDM MANUAL {start_date.strftime('%Y-%m-%d')}..{end_date.strftime('%Y-%m-%d')}")

    chunks = list(chunk_ranges(start_date, end_date))
    total_days = (end_date - start_date).days + 1
    started_at = time.time()

    logger.info(f"Range spans {total_days} day(s) in {len(chunks)} chunk(s) of <= 7 days")

    failed_chunks = 0
    uploaded_files = 0
    failed_uploads = 0
    total_rows = 0

    try:
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            logger.info(f"[chunk {i}/{len(chunks)}]")
            files = download_chunk(chunk_start, chunk_end, script_dir, logger)

            if not files:
                failed_chunks += 1
            else:
                # Upload immediately so a long backfill lands data as it goes
                # rather than only at the very end.
                for file_path in files:
                    result = subprocess.run(
                        ['/usr/local/bin/python3', str(script_dir / 'upload_okte_idm.py'), str(file_path)],
                        cwd=script_dir,
                        capture_output=True,
                        text=True,
                        timeout=120,
                        env=os.environ.copy()
                    )
                    if result.returncode == 0:
                        uploaded_files += 1
                        for line in result.stdout.strip().splitlines():
                            logger.info(f"    {line}")
                            match = re.search(r'(\d+) rows upserted', line)
                            if match:
                                total_rows += int(match.group(1))
                    else:
                        failed_uploads += 1
                        logger.error(
                            f"    UPLOAD FAILED {file_path.name}: "
                            f"{(result.stderr or result.stdout).strip()}"
                        )

            if i < len(chunks):
                logger.debug("Waiting 2s before next chunk...")
                time.sleep(2)

        elapsed = time.time() - started_at
        logger.info("-" * 60)
        logger.info(
            f"OKTE IDM backfill done: {len(chunks) - failed_chunks}/{len(chunks)} chunks OK"
            + (f", {failed_chunks} FAILED" if failed_chunks else "")
        )
        logger.info(
            f"  files uploaded: {uploaded_files}"
            + (f", {failed_uploads} FAILED" if failed_uploads else "")
        )
        logger.info(f"  rows upserted:  {total_rows:,}")
        logger.info(f"  elapsed:        {elapsed:.1f}s")

        if failed_chunks or failed_uploads:
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
