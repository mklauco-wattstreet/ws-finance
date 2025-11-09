#!/usr/bin/env python3
"""
Common utilities shared across download scripts.
"""

import sys
import os
import logging
import urllib.request
import urllib.error
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path


def setup_logging(debug=False):
    """
    Setup logging configuration.

    Args:
        debug: If True, set log level to DEBUG, otherwise INFO

    Returns:
        Logger instance
    """
    log_level = logging.DEBUG if debug else logging.INFO

    # Create logger
    logger = logging.getLogger('ote_downloader')
    logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates
    logger.handlers = []

    # Create console handler with formatting
    handler = logging.StreamHandler()
    handler.setLevel(log_level)

    # Create formatter
    if debug:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def parse_date(date_str):
    """
    Parse date string in YYYY-MM-DD format.

    Args:
        date_str: Date string in YYYY-MM-DD format

    Returns:
        datetime object

    Raises:
        SystemExit: If date format is invalid
    """
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        print(f"Error: Invalid date format '{date_str}'. Expected YYYY-MM-DD")
        sys.exit(1)


def date_range(start_date, end_date):
    """
    Generate dates between start_date and end_date (inclusive).

    Args:
        start_date: Start datetime object
        end_date: End datetime object

    Yields:
        datetime objects for each date in range
    """
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def download_file(url, target_path, logger, timeout=30):
    """
    Download a file from URL to target path.

    Args:
        url: URL to download from
        target_path: Path object where file should be saved
        logger: Logger instance for logging
        timeout: Request timeout in seconds

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create request with headers
        req = urllib.request.Request(
            url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )

        # Download the file
        logger.debug(f"Downloading from: {url}")
        with urllib.request.urlopen(req, timeout=timeout) as response:
            file_data = response.read()

            # Save the file
            with open(target_path, 'wb') as f:
                f.write(file_data)

            file_size = len(file_data)
            logger.info(f"Download completed ({file_size:,} bytes)")
            logger.info(f"File saved to: {target_path}")
            return True

    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.warning(f"File not found (404) - The report might not be available")
        else:
            logger.warning(f"HTTP Error {e.code}")
        return False

    except urllib.error.URLError as e:
        logger.warning(f"Connection error: {str(e.reason)}")
        return False

    except TimeoutError:
        logger.warning(f"Download timeout")
        return False

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return False


def validate_date_range(start_date, end_date):
    """
    Validate that start_date is before or equal to end_date.

    Args:
        start_date: Start datetime object
        end_date: End datetime object

    Raises:
        SystemExit: If validation fails
    """
    if start_date > end_date:
        print("Error: Start date must be before or equal to end date")
        sys.exit(1)


def print_banner(title, debug_mode=False):
    """
    Print a formatted banner.

    Args:
        title: Title to display in banner
        debug_mode: If True, add debug mode indicator
    """
    print(f"╔══════════════════════════════════════════════════════════╗")
    print(f"║  {title:<56}║")
    if debug_mode:
        print(f"║  DEBUG MODE - Verbose logging enabled                   ║")
    print(f"╚══════════════════════════════════════════════════════════╝")


def extract_date_from_filename(filename, pattern):
    """
    Extract date from filename using regex pattern.

    Args:
        filename: Filename to parse
        pattern: Regex pattern with groups for day, month, year

    Returns:
        datetime object or None if parsing fails
    """
    match = re.search(pattern, filename)
    if match:
        day, month, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError:
            return None
    return None


def find_last_downloaded_file(base_dir, file_pattern, date_pattern, logger):
    """
    Find the most recent downloaded file in directory structure.

    Args:
        base_dir: Base directory to search (Path object)
        file_pattern: Glob pattern to match files (e.g., "DM_*.xlsx")
        date_pattern: Regex pattern to extract date from filename
        logger: Logger instance

    Returns:
        datetime object of the last downloaded file, or None if no files found
    """
    # Search recursively for matching files
    all_files = list(base_dir.glob(f"**/{file_pattern}"))

    if not all_files:
        logger.debug(f"No existing files found matching pattern: {file_pattern}")
        return None

    # Extract dates from all files and find the most recent
    latest_date = None
    latest_file = None

    for file_path in all_files:
        file_date = extract_date_from_filename(file_path.name, date_pattern)
        if file_date:
            if latest_date is None or file_date > latest_date:
                latest_date = file_date
                latest_file = file_path

    if latest_date:
        logger.info(f"Last downloaded file: {latest_file.name}")
        logger.info(f"Last download date: {latest_date.strftime('%Y-%m-%d')}")
    else:
        logger.debug("No valid dates found in existing files")

    return latest_date


def auto_determine_date_range(base_dir, file_pattern, date_pattern, logger, minimum_date=None):
    """
    Automatically determine the date range for downloads.

    Finds the last downloaded file and determines what dates to download.
    If no files exist, downloads from minimum_date to yesterday.

    Args:
        base_dir: Base directory to search
        file_pattern: Glob pattern to match files
        date_pattern: Regex pattern to extract date
        logger: Logger instance
        minimum_date: Minimum date to start from if no files exist (datetime object)

    Returns:
        tuple: (start_date, end_date) as datetime objects
    """
    # Yesterday is the default end date (files are usually published next day)
    yesterday = datetime.now() - timedelta(days=1)

    # Find last downloaded file
    last_date = find_last_downloaded_file(base_dir, file_pattern, date_pattern, logger)

    if last_date:
        # Start from the day after the last download
        start_date = last_date + timedelta(days=1)

        # If last download was yesterday or today, nothing to download
        if start_date > yesterday:
            logger.info(f"Already up to date. Last download: {last_date.strftime('%Y-%m-%d')}")
            return None, None

        end_date = yesterday
        logger.info(f"Auto-determined date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"Gap detected: {(end_date - start_date).days + 1} days to download")

    else:
        # No files exist - download from minimum date to yesterday
        if minimum_date is None:
            raise ValueError("minimum_date must be specified when no files exist")

        start_date = minimum_date
        end_date = yesterday
        days_count = (end_date - start_date).days + 1
        logger.info(f"No existing files found. Downloading from minimum date: {start_date.strftime('%Y-%m-%d')}")
        logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({days_count} days)")

    return start_date, end_date


def run_upload_script(upload_script_name, base_dir, start_date, end_date, logger):
    """
    Run upload script for the date range that was downloaded.

    Uploads files from all year/month directories between start and end date.

    Args:
        upload_script_name: Name of upload script (e.g., 'upload_day_ahead_prices.py')
        base_dir: Base directory where files are stored
        start_date: Start date of download range
        end_date: End date of download range
        logger: Logger instance

    Returns:
        bool: True if upload succeeded, False otherwise
    """
    logger.info(f"\n{'═' * 60}")
    logger.info(f"Starting Upload Process")
    logger.info(f"{'═' * 60}\n")

    # Get unique year/month combinations for the date range
    year_months = set()
    current = start_date
    while current <= end_date:
        year_month = f"{current.strftime('%Y')}/{current.strftime('%m')}"
        year_months.add(year_month)
        current += timedelta(days=1)

    # Run upload for each year/month directory
    all_success = True
    for year_month in sorted(year_months):
        dir_path = base_dir / year_month

        if not dir_path.exists():
            logger.warning(f"Directory not found: {dir_path} - Skipping")
            continue

        logger.info(f"Running upload for directory: {year_month}")

        try:
            # Execute upload script as subprocess
            script_path = base_dir / upload_script_name
            result = subprocess.run(
                ['/usr/local/bin/python3', str(script_path), year_month],
                cwd=base_dir,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                env=os.environ.copy()  # Pass environment variables to subprocess
            )

            # Log output from upload script
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    logger.info(f"  {line}")

            if result.returncode != 0:
                logger.error(f"Upload failed for {year_month}")
                if result.stderr:
                    logger.error(f"Error output: {result.stderr}")
                all_success = False
            else:
                logger.info(f"✓ Upload completed for {year_month}")

        except subprocess.TimeoutExpired:
            logger.error(f"Upload timeout for {year_month}")
            all_success = False
        except Exception as e:
            logger.error(f"Upload error for {year_month}: {e}")
            all_success = False

    logger.info(f"\n{'═' * 60}")
    logger.info(f"Upload Process {'Completed' if all_success else 'Completed with Errors'}")
    logger.info(f"{'═' * 60}\n")

    return all_success
