#!/usr/bin/env python3
"""
CEPS SVR Activation Downloader - Selenium + JavaScript Injection
Downloads CEPS SVR (Secondary Reserve) activation data for Czech Republic.

Data Tag: AktivaceSVRvCR
Source: https://www.ceps.cz/cs/data#AktivaceSVRvCR

Parameters:
- aggregation: MI (minute), QH (15min), HR (hour), DY (day), etc.
- function: AVG, SUM, MIN, MAX
- para1: all | aFRR+ | aFRR- | mFRR+ | mFRR- | RR+ | RR- | MFRR5

This follows the proven hybrid approach from ceps_hybrid_downloader.py
"""

import sys
import time
import shutil
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

    # Download settings
    download_dir = "/app/downloads/ceps"
    Path(download_dir).mkdir(parents=True, exist_ok=True)

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Headless mode
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    # Disable caching to force fresh requests
    chrome_options.add_argument("--disable-application-cache")
    chrome_options.add_argument("--disk-cache-size=0")

    service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.implicitly_wait(10)

    return driver


def download_ceps_svr_activation(driver, start_date, end_date, logger, aggregation="MI", function="AVG", para1="all"):
    """
    Download CEPS SVR activation data using hybrid approach: Selenium + JavaScript.

    Args:
        driver: Selenium WebDriver instance
        start_date: Start datetime
        end_date: End datetime
        logger: Logger instance
        aggregation: Aggregation type (MI, QH, HR, DY, etc.)
        function: Aggregation function (AVG, SUM, MIN, MAX)
        para1: Type of power plant (all | aFRR+ | aFRR- | mFRR+ | mFRR- | RR+ | RR- | MFRR5)

    Returns:
        Path to downloaded file, or None if failed
    """
    data_tag = "AktivaceSVRvCR"

    logger.info("=" * 60)
    logger.info("CEPS SVR Activation Downloader (Selenium + JavaScript)")
    logger.info(f"Tag: {data_tag}")
    logger.info(f"Date: {start_date.date()} to {end_date.date()}")
    logger.info(f"Aggregation: {aggregation}, Function: {function}, Type: {para1}")
    logger.info("=" * 60)

    # Graph ID for AktivaceSVRvCR (all datasets use 1040)
    graph_id = 1040

    # Step 0: Clean download directory to ensure no leftover files
    download_dir = Path("/app/downloads/ceps")
    for old_file in list(download_dir.glob("*.csv")) + list(download_dir.glob("*.txt")):
        try:
            old_file.unlink()
            logger.info(f"  Removed old file: {old_file.name}")
        except:
            pass

    # Step 1: Navigate to Czech data page to establish session
    logger.info("Step 1: Establishing browser session...")

    # First, clear any existing cookies/session to start fresh
    driver.delete_all_cookies()
    logger.info("  Cleared cookies")

    # Navigate to page first to establish context
    # CRITICAL: Anchor (#data_tag) triggers JavaScript to initialize the correct dataset
    url = f"https://www.ceps.cz/cs/data#{data_tag}"
    logger.info(f"  Navigating to: {url}")
    driver.get(url)
    time.sleep(2)

    # Clear browser storage (localStorage, sessionStorage, cache)
    driver.execute_script("window.localStorage.clear();")
    driver.execute_script("window.sessionStorage.clear();")
    logger.info("  Cleared localStorage and sessionStorage")

    # Reload page with fresh session
    logger.info("  Reloading page with fresh session...")
    driver.get(url)
    time.sleep(3)  # Let page load completely

    # Accept cookies if needed
    logger.info("Step 2: Handling cookie consent...")
    try:
        cookie_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "c-p-bn"))
        )
        driver.execute_script("arguments[0].click();", cookie_button)
        logger.info("✓ Cookies accepted")
        time.sleep(1)
    except:
        logger.info("  No cookie dialog (already accepted or not shown)")

    # Check if we have a session cookie
    cookies = driver.get_cookies()
    phpsessid = next((c for c in cookies if c['name'] == 'PHPSESSID'), None)
    if phpsessid:
        logger.info(f"✓ PHP session established: {phpsessid['value'][:8]}...")
    else:
        logger.warning("⚠ No PHPSESSID cookie - this may cause issues")

    # Step 3: Wait for page initialization to complete
    logger.info("Step 3: Waiting for page initialization...")

    # Wait for the loading icon to appear and disappear (indicates AJAX call completed)
    try:
        # Wait up to 10 seconds for page to finish loading
        time.sleep(5)  # Give page time to start initialization
        logger.info("  Page initialization complete")
    except Exception as e:
        logger.warning(f"  Could not detect page initialization: {e}")

    # Step 4: Inject JavaScript to call filterData function directly
    logger.info("Step 4: Injecting JavaScript to download data...")

    # Format dates as the website expects (SPACE separator, not T)
    date_from_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    date_to_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

    # Determine date_type
    if start_date.date() == end_date.date():
        date_type = "day"
        move_graph = "day"
    else:
        date_type = "day"
        move_graph = "day"

    logger.info(f"  Date from: {date_from_str}")
    logger.info(f"  Date to: {date_to_str}")
    logger.info(f"  Method: {data_tag}")
    logger.info(f"  Graph ID: {graph_id}")

    # Build the filter_data object in JavaScript
    # CRITICAL FIX: Monkey-patch serializeFilters to force our dates AND method
    js_code = f"""
    console.log('CEPS SVR Activation Downloader - Monkey-patching serializeFilters...');

    // Save original serializeFilters function
    var originalSerializeFilters = serializeFilters;

    // Our forced parameters
    var forcedDateFrom = "{date_from_str}".replace(" ", "T");
    var forcedDateTo = "{date_to_str}".replace(" ", "T");
    var forcedMethod = "{data_tag}";

    // Replace serializeFilters with our version that forces dates and method
    window.serializeFilters = function(filters, others) {{
        console.log('PATCHED serializeFilters called');
        console.log('  Original filters:', filters);
        console.log('  Original others:', others);

        // Call original function
        var result = originalSerializeFilters(filters, others);

        // FORCE our parameters into the result
        result.date_from = forcedDateFrom;
        result.date_to = forcedDateTo;
        result.method = forcedMethod;

        console.log('  Forced date_from:', result.date_from);
        console.log('  Forced date_to:', result.date_to);
        console.log('  Forced method:', result.method);
        console.log('  Final AJAX params:', result);

        return result;
    }};

    // Build filter_data object
    var filter_data = {{
        dateFrom: "{date_from_str}",
        dateTo: "{date_to_str}",
        dateType: "{date_type}",
        agregation: "{aggregation}",
        interval: "false",
        version: "RT",
        function: "{function}"
    }};

    var method = "{data_tag}";
    var graph_id = {graph_id};
    var move_graph = "{move_graph}";

    console.log('Filter data:', filter_data);

    if (typeof filterData !== 'function') {{
        console.error('filterData function not found');
        return 'ERROR: filterData not found';
    }}

    // Now call filterData normally
    // Our patched serializeFilters will inject the correct dates and method
    console.log('Calling filterData (serializeFilters is patched)...');
    filterData(filter_data, method, move_graph, "", "txt");

    return 'SUCCESS - Download triggered (check console for details)';
    """

    try:
        result = driver.execute_script(js_code)
        logger.info(f"✓ JavaScript executed: {result}")

        # Capture browser console logs for debugging
        if logger.level <= 10:  # DEBUG level
            time.sleep(0.5)  # Give JS time to log
            logs = driver.get_log('browser')
            if logs:
                logger.debug("Browser console logs:")
                for log in logs[-20:]:  # Last 20 log entries
                    logger.debug(f"  [{log['level']}] {log['message']}")
    except Exception as e:
        logger.error(f"✗ JavaScript execution failed: {e}")
        return None

    # Step 5: Wait for download to complete
    logger.info("Step 5: Waiting for download to complete...")
    download_dir = Path("/app/downloads/ceps")

    # Wait for file to appear (up to 30 seconds)
    max_wait = 30
    for i in range(max_wait):
        time.sleep(1)
        txt_files = list(download_dir.glob("*.txt"))
        if txt_files:
            # Get most recent file
            latest_file = max(txt_files, key=lambda p: p.stat().st_mtime)
            logger.info(f"✓ File downloaded: {latest_file}")

            # Move to organized directory
            dest_dir = Path(f"/app/downloads/ceps/{start_date.year}/{start_date.month:02d}")
            dest_dir.mkdir(parents=True, exist_ok=True)

            if start_date.date() == end_date.date():
                date_str = start_date.strftime("%Y%m%d")
            else:
                date_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

            timestamp = datetime.now().strftime("%H%M%S")
            dest_file = dest_dir / f"data_{data_tag}_{date_str}_{timestamp}.csv"

            shutil.move(str(latest_file), str(dest_file))
            logger.info(f"✓ File moved to: {dest_file}")

            # Verify file content - CRITICAL: Fail if dates don't match
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

    logger.error(f"✗ Timeout: No file downloaded after {max_wait} seconds")
    return None


def main():
    """Main entry point."""
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description='CEPS SVR Activation Downloader')
    parser.add_argument('--start-date', type=str, default=None,
                       help='Start date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--end-date', type=str, default=None,
                       help='End date in YYYY-MM-DD format (default: same as start-date)')
    parser.add_argument('--aggregation', type=str, default='MI',
                       choices=['MI', 'QH', 'HR', 'DY', 'WE', 'MO', 'YR'],
                       help='Aggregation type (default: MI - minute)')
    parser.add_argument('--function', type=str, default='AVG',
                       choices=['AVG', 'SUM', 'MIN', 'MAX'],
                       help='Aggregation function (default: AVG)')
    parser.add_argument('--para1', type=str, default='all',
                       help='Type of power plant (default: all)')
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

    driver = None
    exit_code = 0

    try:
        # Initialize browser
        driver = init_browser()

        # Download data
        downloaded_file = download_ceps_svr_activation(
            driver,
            start_date,
            end_date,
            logger,
            aggregation=args.aggregation,
            function=args.function,
            para1=args.para1
        )

        if not downloaded_file:
            exit_code = 1

    except KeyboardInterrupt:
        logger.info("\nInterrupted")
        exit_code = 130
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        exit_code = 1
    finally:
        if driver:
            driver.quit()

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
