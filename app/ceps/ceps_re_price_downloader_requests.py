#!/usr/bin/env python3
"""
CEPS RE Price Downloader - Direct AJAX with requests
Downloads CEPS Actual Reserve Energy (RE) pricing data using direct HTTP requests.

Strategy:
1. Use Selenium to establish PHP session (get PHPSESSID cookie)
2. Extract cookies and make direct AJAX call with requests library
3. Bypass all JavaScript complexity

This approach is more reliable than JavaScript injection.
"""

import sys
import time
import shutil
import requests
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import setup_logging


def init_browser():
    """Initialize Chrome browser for headless execution."""
    chrome_options = Options()
    chrome_options.binary_location = "/usr/bin/chromium"

    # Headless mode
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.implicitly_wait(10)

    return driver


def get_session_cookies(logger):
    """
    Establish browser session and extract cookies.

    Returns:
        dict: Cookies as {name: value}
    """
    logger.info("Establishing browser session to get cookies...")

    driver = None
    try:
        driver = init_browser()

        # Navigate to data page
        driver.get("https://www.ceps.cz/cs/data")
        time.sleep(2)

        # Accept cookies if needed
        try:
            cookie_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "c-p-bn"))
            )
            driver.execute_script("arguments[0].click();", cookie_button)
            logger.info("  Cookies accepted")
            time.sleep(1)
        except:
            logger.info("  No cookie dialog")

        # Extract all cookies
        selenium_cookies = driver.get_cookies()
        cookies = {cookie['name']: cookie['value'] for cookie in selenium_cookies}

        # Check for PHPSESSID
        if 'PHPSESSID' in cookies:
            logger.info(f"✓ Session established: PHPSESSID={cookies['PHPSESSID'][:8]}...")
        else:
            logger.warning("⚠ No PHPSESSID cookie found")

        return cookies

    finally:
        if driver:
            driver.quit()


def download_ceps_re_price_direct(start_date, end_date, logger):
    """
    Download CEPS RE price data using direct AJAX request.

    Args:
        start_date: Start datetime
        end_date: End datetime
        logger: Logger instance

    Returns:
        Path to downloaded file, or None if failed
    """
    data_tag = "AktualniCenaRE"

    logger.info("=" * 60)
    logger.info("CEPS RE Price Downloader (Direct AJAX)")
    logger.info(f"Tag: {data_tag}")
    logger.info(f"Date: {start_date.date()} to {end_date.date()}")
    logger.info("=" * 60)

    # Step 1: Get session cookies
    cookies = get_session_cookies(logger)

    # Step 2: Build AJAX request parameters
    logger.info("Building AJAX request...")

    # Format dates with T separator (as seen in browser)
    date_from_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    date_to_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    # Build query parameters based on browser network analysis
    params = {
        'do': 'loadGraphData',
        'method': data_tag,
        'graph_id': '1040',
        'move_graph': 'day',
        'download': 'csv',
        'date_from': date_from_str,
        'date_to': date_to_str,
        'date_type': 'day',
        'interval': 'false'
    }

    logger.info(f"  method: {params['method']}")
    logger.info(f"  date_from: {params['date_from']}")
    logger.info(f"  date_to: {params['date_to']}")

    # Step 3: Make direct AJAX request
    logger.info("Making direct AJAX request...")

    url = "https://www.ceps.cz/cs/data"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'cs,en;q=0.9',
        'Referer': 'https://www.ceps.cz/cs/data',
    }

    try:
        response = requests.get(url, params=params, cookies=cookies, headers=headers, timeout=30)

        logger.info(f"  Response status: {response.status_code}")
        logger.info(f"  Content-Type: {response.headers.get('Content-Type', 'unknown')}")
        logger.info(f"  Content length: {len(response.content)} bytes")

        if response.status_code != 200:
            logger.error(f"✗ HTTP error: {response.status_code}")
            return None

        # Check if we got CSV data
        content_type = response.headers.get('Content-Type', '')
        if 'text/csv' in content_type or 'application/csv' in content_type:
            logger.info("✓ Received CSV data")
        elif response.content.startswith(b'\xef\xbb\xbf'):  # UTF-8 BOM (CSV marker)
            logger.info("✓ Detected CSV data (UTF-8 BOM)")
        else:
            # Check first few lines
            first_lines = response.text[:200]
            if 'Verze dat' in first_lines or 'Datum' in first_lines:
                logger.info("✓ Detected CSV data (header check)")
            else:
                logger.error("✗ Response doesn't appear to be CSV")
                logger.error(f"  First 200 chars: {first_lines}")
                return None

        # Step 4: Save to file
        dest_dir = Path(f"/app/scripts/ceps/{start_date.year}/{start_date.month:02d}")
        dest_dir.mkdir(parents=True, exist_ok=True)

        if start_date.date() == end_date.date():
            date_str = start_date.strftime("%Y%m%d")
        else:
            date_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

        timestamp = datetime.now().strftime("%H%M%S")
        dest_file = dest_dir / f"data_{data_tag}_{date_str}_{timestamp}.csv"

        with open(dest_file, 'wb') as f:
            f.write(response.content)

        logger.info(f"✓ Saved to: {dest_file}")

        # Step 5: Verify file content
        try:
            with open(dest_file, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                second_line = f.readline().strip()
                logger.info(f"  Header: {first_line[:100]}")
                logger.info(f"  Metadata: {second_line[:100]}")

                # Check if it contains expected date
                date_check = start_date.strftime("%d.%m.%Y")
                if date_check in second_line:
                    logger.info(f"✓ Verified: File contains expected date {date_check}")
                    logger.info("=" * 60)
                    logger.info("✓ SUCCESS")
                    logger.info("=" * 60)
                    return dest_file
                else:
                    logger.error("=" * 60)
                    logger.error(f"✗ VALIDATION FAILED: Expected date {date_check} not found in CSV")
                    logger.error(f"  Requested: {start_date.date()} to {end_date.date()}")
                    logger.error(f"  Metadata: {second_line[:200]}")
                    logger.error("  The CEPS website returned wrong data!")
                    logger.error("=" * 60)
                    # Delete the wrong file
                    dest_file.unlink()
                    logger.info(f"  Deleted wrong file: {dest_file}")
                    return None
        except Exception as e:
            logger.error(f"✗ Error reading file: {e}")
            return None

    except requests.exceptions.Timeout:
        logger.error("✗ Request timeout")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Request failed: {e}")
        return None


def main():
    """Main entry point."""
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description='CEPS RE Price Downloader (Direct AJAX)')
    parser.add_argument('--start-date', type=str, default=None,
                       help='Start date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--end-date', type=str, default=None,
                       help='End date in YYYY-MM-DD format (default: same as start-date)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(debug=args.debug)

    # Parse dates
    try:
        if args.start_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        else:
            start_date = datetime.combine(date.today(), datetime.min.time())

        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        else:
            end_date = start_date

        # Set time to cover full day
        start_date = start_date.replace(hour=0, minute=0, second=0)
        end_date = end_date.replace(hour=23, minute=59, second=59)

    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        logger.error("Date format must be YYYY-MM-DD (e.g., 2026-01-04)")
        sys.exit(1)

    exit_code = 0

    try:
        # Download data
        downloaded_file = download_ceps_re_price_direct(start_date, end_date, logger)

        if not downloaded_file:
            exit_code = 1

    except KeyboardInterrupt:
        logger.info("\nInterrupted")
        exit_code = 130
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
