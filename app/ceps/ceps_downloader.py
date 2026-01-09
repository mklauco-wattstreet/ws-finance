#!/usr/bin/env python3
"""
CEPS Data Downloader
Downloads CSV data from CEPS (Czech Electricity Power System) website using headless browser.

This is a separate headless browser instance from the OTE production downloader.
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
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import setup_logging
from ceps.constants import CEPS_BASE_URL, TAG_TO_DISPLAY


# Czech month names mapping
CZECH_MONTHS = {
    1: "leden",
    2: "únor",
    3: "březen",
    4: "duben",
    5: "květen",
    6: "červen",
    7: "červenec",
    8: "srpen",
    9: "září",
    10: "říjen",
    11: "listopad",
    12: "prosinec"
}


def take_screenshot(driver, name, data_tag, logger):
    """Take screenshot with timestamp for debugging in tag-specific subfolder."""
    try:
        # Create screenshot directory: ceps/{data_tag}/
        screenshot_dir = Path(f"/app/scripts/ceps/{data_tag}")
        screenshot_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%H%M%S")
        filename = screenshot_dir / f"{timestamp}_{name}.png"
        driver.save_screenshot(str(filename))
        logger.info(f"📸 Screenshot: {filename}")
    except Exception as e:
        logger.debug(f"Screenshot failed: {e}")


def cleanup_old_screenshots(data_tag, logger):
    """Delete all CEPS screenshots from previous runs for specific tag."""
    import glob
    screenshot_dir = Path(f"/app/scripts/ceps/{data_tag}")
    if screenshot_dir.exists():
        screenshot_files = list(screenshot_dir.glob("*.png"))
        if screenshot_files:
            for f in screenshot_files:
                try:
                    f.unlink()
                except:
                    pass
            logger.info(f"Cleaned up {len(screenshot_files)} old screenshot(s) for {data_tag}")


def init_browser():
    """Initialize Chrome browser for headless execution."""
    chrome_options = Options()
    chrome_options.binary_location = "/usr/bin/chromium"

    # Use separate browser profile for CEPS
    chrome_options.add_argument("--user-data-dir=/app/browser-profile-ceps")

    # Download settings - use dedicated CEPS downloads directory
    download_dir = "/app/downloads/ceps"
    Path(download_dir).mkdir(parents=True, exist_ok=True)

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Headless mode for production
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--lang=cs")  # Czech language

    service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.implicitly_wait(10)

    return driver


def download_ceps_data(driver, data_tag: str, start_date: datetime, end_date: datetime, logger) -> Path:
    """
    Download CEPS data for a specific tag and date range.

    Algorithm (when start_date == end_date):
    1. Visit the page with the anchor tag
    2. Click cookie consent "Příjmout vše" if appears
    3. Verify heading element exists
    4. Click "Nastavení filtru", wait 5 seconds
    5. Verify "Nastavte si časové období" exists
    6. Click on "den" radio label, wait 5 seconds
    7. Select year from dropdown
    8. Select month from dropdown
    9. Select day from dropdown (TODO)
    10. Click CSV download button, wait 15 seconds
    11. Rename and save file to app/ceps/YYYY/MM

    Args:
        driver: Selenium WebDriver instance
        data_tag: The CEPS data tag (e.g., "AktualniSystemovaOdchylkaCR")
        start_date: Start date for data download
        end_date: End date for data download
        logger: Logger instance

    Returns:
        Path to downloaded file, or None if failed
    """
    wait = WebDriverWait(driver, 20)

    try:
        # Get display name for verification
        display_name = TAG_TO_DISPLAY.get(data_tag, data_tag)

        # Step 1: Visit the page and clear cookies
        url = f"{CEPS_BASE_URL}#{data_tag}"
        logger.info(f"Step 1: Navigating to: {url}")
        driver.get(url)

        # Delete all cookies to avoid IP blocking
        logger.info("Deleting all cookies to avoid IP blocking...")
        driver.delete_all_cookies()
        logger.info("✓ All cookies deleted")

        # Reload page after clearing cookies
        driver.get(url)
        time.sleep(3)  # Let page load and anchor navigate
        take_screenshot(driver, "01_page_loaded", data_tag, logger)

        # Step 2: Handle cookie consent if it appears
        logger.info("Step 2: Checking for cookie consent...")
        try:
            cookie_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "c-p-bn"))
            )
            cookie_button.click()
            logger.info("✓ Clicked 'Příjmout vše' cookie consent")
            time.sleep(2)  # Wait for consent dialog to close
        except TimeoutException:
            logger.info("✓ No cookie consent dialog found (already accepted)")

        take_screenshot(driver, "02_cookie_consent_handled", data_tag, logger)

        # Step 2b: Verify page loaded correctly (check for basic-graph to detect IP blocking)
        logger.info("Step 2b: Verifying page access (checking for basic-graph element)...")
        try:
            basic_graph = wait.until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='basic-graph']"))
            )
            logger.info("✓ Page loaded successfully - basic-graph element found")
        except TimeoutException:
            logger.error("✗ BLOCKED: basic-graph element not found - possible IP blocking!")
            take_screenshot(driver, "02b_ip_blocked", data_tag, logger)
            logger.error("The page did not load the data graphs. This usually indicates:")
            logger.error("  - IP blocking is active")
            logger.error("  - The page structure has changed")
            logger.error("  - Network connectivity issues")
            return None

        take_screenshot(driver, "02b_page_verified", data_tag, logger)

        # Step 3: Verify heading exists
        logger.info(f"Step 3: Verifying heading: {display_name}")
        try:
            heading = wait.until(
                EC.presence_of_element_located((By.XPATH, f"//p[contains(text(), '{display_name}')]"))
            )
            logger.info(f"✓ Heading found: {heading.text}")
        except TimeoutException:
            logger.error(f"✗ Heading not found: {display_name}")
            take_screenshot(driver, "03_heading_not_found", data_tag, logger)
            return None

        take_screenshot(driver, "03_heading_verified", data_tag, logger)

        # Step 4: Click "Nastavení filtru"
        logger.info("Step 4: Clicking 'Nastavení filtru'...")
        try:
            filter_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Nastavení filtru')]"))
            )
            filter_button.click()
            logger.info("✓ Clicked 'Nastavení filtru'")
        except TimeoutException:
            logger.error("✗ 'Nastavení filtru' button not found")
            take_screenshot(driver, "04_filter_button_not_found", data_tag, logger)
            return None

        time.sleep(5)  # Wait as specified
        take_screenshot(driver, "04_filter_clicked", data_tag, logger)

        # Step 5: Verify "Nastavte si časové období" exists
        logger.info("Step 5: Verifying 'Nastavte si časové období'...")
        try:
            time_period_text = wait.until(
                EC.presence_of_element_located((By.XPATH, "//p[contains(text(), 'Nastavte si časové období')]"))
            )
            logger.info(f"✓ Time period settings found: {time_period_text.text}")
        except TimeoutException:
            logger.error("✗ 'Nastavte si časové období' not found")
            take_screenshot(driver, "05_time_period_not_found", data_tag, logger)
            return None

        take_screenshot(driver, "05_time_period_verified", data_tag, logger)

        # Step 6: Click on "den" radio label
        logger.info("Step 6: Clicking 'den' radio button...")
        try:
            # Find the radio button element
            day_radio = wait.until(
                EC.presence_of_element_located((By.ID, "day"))
            )

            # Scroll element into view
            logger.info("Scrolling radio button into view...")
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", day_radio)
            time.sleep(1)  # Wait for scroll to complete

            # Try to click using JavaScript (most reliable for intercepted elements)
            logger.info("Clicking 'den' radio using JavaScript...")
            driver.execute_script("arguments[0].click();", day_radio)
            logger.info("✓ Clicked 'den' radio button")

            # Verify the radio is checked
            is_checked = driver.execute_script("return arguments[0].checked;", day_radio)
            if is_checked:
                logger.info("✓ Radio button 'den' is now checked")
            else:
                logger.warning("⚠ Radio button may not be checked, attempting alternative method...")
                # Try clicking the label as backup
                day_label = driver.find_element(By.XPATH, "//label[@for='day']")
                driver.execute_script("arguments[0].click();", day_label)
                time.sleep(0.5)
                is_checked = driver.execute_script("return arguments[0].checked;", day_radio)
                if is_checked:
                    logger.info("✓ Radio button 'den' is now checked (via label)")
                else:
                    logger.error("✗ Failed to check 'den' radio button")
                    take_screenshot(driver, "06_day_radio_not_checked", data_tag, logger)
                    return None

        except Exception as e:
            logger.error(f"✗ Failed to click 'den' radio: {e}")
            take_screenshot(driver, "06_day_radio_error", data_tag, logger)
            import traceback
            logger.error(traceback.format_exc())
            return None

        time.sleep(5)  # Wait as specified
        take_screenshot(driver, "06_day_radio_clicked", data_tag, logger)

        # Check if we need to select specific dates
        if start_date.date() == end_date.date():
            logger.info("Single date mode: selecting specific date...")

            # Step 7: Select year
            logger.info(f"Step 7: Selecting year {start_date.year}...")
            try:
                # Find the year dropdown (year_from)
                year_dropdown = wait.until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'year_from')]//span[@class='select-pick']"))
                )

                # Scroll into view and click to open dropdown
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", year_dropdown)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", year_dropdown)
                logger.info("✓ Opened year dropdown")
                time.sleep(2)  # Wait for dropdown to render

                take_screenshot(driver, "07_year_dropdown_opened", data_tag, logger)

                # Click on the specific year
                year_item = wait.until(
                    EC.presence_of_element_located((By.XPATH, f"//div[contains(@class, 'year_from')]//li[@data-filter-value='{start_date.year}']"))
                )
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", year_item)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", year_item)
                logger.info(f"✓ Selected year {start_date.year}")
                time.sleep(2)

                take_screenshot(driver, "07_year_selected", data_tag, logger)

            except Exception as e:
                logger.error(f"✗ Failed to select year: {e}")
                take_screenshot(driver, "07_year_selection_error", data_tag, logger)
                import traceback
                logger.error(traceback.format_exc())
                return None

            # Step 8: Select month
            month_name = CZECH_MONTHS[start_date.month]
            month_value = f"{start_date.month:02d}"
            logger.info(f"Step 8: Selecting month {month_value} ({month_name})...")
            try:
                # Find the month dropdown (month_from)
                month_dropdown = wait.until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'month_from')]//span[@class='select-pick']"))
                )

                # Scroll into view and click to open dropdown
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", month_dropdown)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", month_dropdown)
                logger.info("✓ Opened month dropdown")
                time.sleep(2)  # Wait for dropdown to render

                take_screenshot(driver, "08_month_dropdown_opened", data_tag, logger)

                # Click on the specific month
                month_item = wait.until(
                    EC.presence_of_element_located((By.XPATH, f"//div[contains(@class, 'month_from')]//li[@data-filter-value='{month_value}']"))
                )
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", month_item)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", month_item)
                logger.info(f"✓ Selected month {month_value} ({month_name})")
                time.sleep(2)

                take_screenshot(driver, "08_month_selected", data_tag, logger)

            except Exception as e:
                logger.error(f"✗ Failed to select month: {e}")
                take_screenshot(driver, "08_month_selection_error", data_tag, logger)
                import traceback
                logger.error(traceback.format_exc())
                return None

            # Step 9: Select day (TODO - implement when needed)
            logger.info(f"Step 9: Day selection not yet implemented (would select day {start_date.day})")
            # TODO: Implement day selection similar to year/month

        else:
            logger.info("Date range mode: not yet implemented")
            logger.error("✗ Date range downloads (start_date != end_date) are not supported yet")
            return None

        # Step 10: Click CSV download button
        logger.info("Step 10: Clicking CSV download button...")
        try:
            # Find the CSV download list item
            csv_download = wait.until(
                EC.presence_of_element_located((By.XPATH, "//li[@data-download-type='csv']"))
            )

            # Scroll element into view
            logger.info("Scrolling CSV download button into view...")
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", csv_download)
            time.sleep(1)  # Wait for scroll to complete

            # Click using JavaScript (most reliable for intercepted elements)
            logger.info("Clicking CSV download button using JavaScript...")
            driver.execute_script("arguments[0].click();", csv_download)
            logger.info("✓ Clicked CSV download button")

        except TimeoutException:
            logger.error("✗ CSV download button not found")
            take_screenshot(driver, "10_csv_button_not_found", data_tag, logger)
            return None
        except Exception as e:
            logger.error(f"✗ Failed to click CSV download button: {e}")
            take_screenshot(driver, "10_csv_button_error", data_tag, logger)
            import traceback
            logger.error(traceback.format_exc())
            return None

        take_screenshot(driver, "10_csv_button_clicked", data_tag, logger)

        # Wait for download to complete
        logger.info("Waiting for download to complete (15 seconds)...")
        time.sleep(15)
        take_screenshot(driver, "10_download_complete", data_tag, logger)

        # Step 11: Find the downloaded file and move it to the proper location
        logger.info("Step 11: Moving downloaded file to destination...")
        download_dir = Path("/app/downloads/ceps")
        csv_files = list(download_dir.glob("*.csv"))

        if not csv_files:
            logger.error("✗ No CSV file found in downloads directory")
            take_screenshot(driver, "08_no_file_downloaded", data_tag, logger)
            return None

        # Get the most recently downloaded file
        latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Downloaded file: {latest_file.name}")

        # Create destination directory: app/ceps/YYYY/MM based on start_date
        dest_dir = Path(f"/app/scripts/ceps/{start_date.year}/{start_date.month:02d}")
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Rename file with date range
        if start_date.date() == end_date.date():
            # Single date
            date_str = start_date.strftime("%Y%m%d")
        else:
            # Date range
            date_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

        timestamp = datetime.now().strftime("%H%M%S")
        dest_file = dest_dir / f"data_{data_tag}_{date_str}_{timestamp}.csv"

        shutil.move(str(latest_file), str(dest_file))
        logger.info(f"✓ File saved: {dest_file}")

        return dest_file

    except Exception as e:
        logger.error(f"Unexpected error during download: {e}")
        take_screenshot(driver, "99_unexpected_error", data_tag, logger)
        import traceback
        logger.error(traceback.format_exc())
        return None


def main():
    """Main entry point for CEPS downloader."""
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description='CEPS Data Downloader')
    parser.add_argument('--tag', type=str, default='AktualniSystemovaOdchylkaCR',
                       help='CEPS data tag to download')
    parser.add_argument('--start-date', type=str, default=None,
                       help='Start date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--end-date', type=str, default=None,
                       help='End date in YYYY-MM-DD format (default: today)')
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
            end_date = datetime.combine(date.today(), datetime.min.time())

    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        logger.error("Date format must be YYYY-MM-DD (e.g., 2026-01-05)")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("CEPS Data Downloader")
    logger.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Tag: {args.tag}")
    logger.info(f"Start Date: {start_date.strftime('%Y-%m-%d')}")
    logger.info(f"End Date: {end_date.strftime('%Y-%m-%d')}")
    logger.info("=" * 60)

    # Clean up old screenshots for this tag
    cleanup_old_screenshots(args.tag, logger)

    driver = None
    exit_code = 0

    try:
        # Initialize browser
        logger.info("Starting browser...")
        driver = init_browser()

        # Download data
        downloaded_file = download_ceps_data(driver, args.tag, start_date, end_date, logger)

        if downloaded_file:
            logger.info("=" * 60)
            logger.info("✓ SUCCESS - Data downloaded")
            logger.info(f"File: {downloaded_file}")
            logger.info("=" * 60)
        else:
            logger.error("=" * 60)
            logger.error("✗ FAILED - Download unsuccessful")
            logger.error("=" * 60)
            exit_code = 1

    except KeyboardInterrupt:
        logger.info("\nInterrupted")
        exit_code = 130
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        exit_code = 1
    finally:
        if driver:
            try:
                driver.quit()
                logger.info("Browser closed")
            except:
                pass

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
