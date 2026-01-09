#!/usr/bin/env python3
"""
CEPS Simple Headless Browser Downloader
Simplified approach - let the page handle date selection via URL parameters.
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
from selenium.common.exceptions import TimeoutException

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import setup_logging


def take_screenshot(driver, name, data_tag, logger):
    """Take screenshot for debugging."""
    try:
        screenshot_dir = Path(f"/app/scripts/ceps/{data_tag}")
        screenshot_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%H%M%S")
        filename = screenshot_dir / f"{timestamp}_{name}.png"
        driver.save_screenshot(str(filename))
        logger.info(f"📸 Screenshot: {filename}")
    except Exception as e:
        logger.debug(f"Screenshot failed: {e}")


def init_browser():
    """Initialize Chrome browser for headless execution."""
    chrome_options = Options()
    chrome_options.binary_location = "/usr/bin/chromium"
    chrome_options.add_argument("--user-data-dir=/app/browser-profile-ceps")

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

    service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.implicitly_wait(10)

    return driver


def download_ceps_simple(driver, data_tag, start_date, end_date, logger):
    """
    Simplified download with date selection:
    1. Navigate to page with anchor
    2. Accept cookies
    3. Click filter settings
    4. Select date
    5. Click CSV download
    6. Wait for file
    """
    wait = WebDriverWait(driver, 30)

    try:
        # Import Czech months for date selection
        from ceps.constants import CZECH_MONTHS

        # Navigate to English page with anchor
        url = f"https://www.ceps.cz/en/all-data#{data_tag}"
        logger.info(f"Step 1: Navigating to {url}")
        logger.info(f"  Target date: {start_date.date()}")
        driver.get(url)
        time.sleep(3)
        take_screenshot(driver, "01_page_loaded", data_tag, logger)

        # Accept cookies if they appear
        logger.info("Step 2: Handling cookie consent...")
        try:
            cookie_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "c-p-bn"))
            )
            cookie_button.click()
            logger.info("✓ Accepted cookies")
            time.sleep(2)
        except TimeoutException:
            logger.info("✓ No cookie dialog")

        take_screenshot(driver, "02_cookies_handled", data_tag, logger)

        # Wait for the graph/data section to load
        logger.info("Step 3: Waiting for page to fully load...")
        try:
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "basic-graph")))
            logger.info("✓ Page loaded")
        except TimeoutException:
            logger.error("✗ Page did not load properly")
            take_screenshot(driver, "03_page_load_failed", data_tag, logger)
            return None

        take_screenshot(driver, "03_page_ready", data_tag, logger)

        # Map data tags to their graph IDs (needed for AJAX call)
        graph_id_map = {
            'AktualniSystemovaOdchylkaCR': 1026,
        }
        graph_id = graph_id_map.get(data_tag, 1026)

        # Only select dates if not default (today)
        from datetime import date
        if start_date.date() != date.today():
            # Click filter settings button
            logger.info("Step 4: Clicking 'Filter settings' button...")
            try:
                filter_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Filter settings')]"))
                )
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", filter_button)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", filter_button)
                logger.info("✓ Clicked filter settings")
                time.sleep(3)
            except TimeoutException:
                logger.error("✗ Filter settings button not found")
                take_screenshot(driver, "04_filter_not_found", data_tag, logger)
                return None

            take_screenshot(driver, "04_filter_opened", data_tag, logger)

            # Click 'day' radio button
            logger.info("Step 5: Selecting 'day' option...")
            try:
                day_radio = wait.until(
                    EC.presence_of_element_located((By.ID, "day"))
                )
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", day_radio)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", day_radio)
                logger.info("✓ Selected 'day' option")
                time.sleep(3)
            except TimeoutException:
                logger.error("✗ Day radio button not found")
                take_screenshot(driver, "05_day_radio_not_found", data_tag, logger)
                return None

            take_screenshot(driver, "05_day_selected", data_tag, logger)

            # Select date (year, month, day)
            if start_date.date() == end_date.date():
                # Single date selection
                year = start_date.year
                month = f"{start_date.month:02d}"
                day_num = start_date.day
                month_name = CZECH_MONTHS[start_date.month]

                # Select year
                logger.info(f"Step 6: Selecting year {year}...")
                try:
                    year_dropdown = wait.until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'year_from')]//span[@class='select-pick']"))
                    )
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", year_dropdown)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", year_dropdown)
                    time.sleep(2)

                    year_item = wait.until(
                        EC.presence_of_element_located((By.XPATH, f"//div[contains(@class, 'year_from')]//li[@data-filter-value='{year}']"))
                    )
                    driver.execute_script("arguments[0].click();", year_item)
                    logger.info(f"✓ Selected year {year}")

                    # Trigger change event on the underlying select element
                    try:
                        year_select = driver.find_element(By.XPATH, "//div[contains(@class, 'year_from')]//select")
                        driver.execute_script("""
                            arguments[0].value = arguments[1];
                            arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
                        """, year_select, str(year))
                        logger.info("✓ Triggered change event for year")
                    except:
                        pass

                    time.sleep(2)
                except Exception as e:
                    logger.error(f"✗ Failed to select year: {e}")
                    take_screenshot(driver, "06_year_failed", data_tag, logger)
                    return None

                take_screenshot(driver, "06_year_selected", data_tag, logger)

                # Select month
                logger.info(f"Step 7: Selecting month {month} ({month_name})...")
                try:
                    month_dropdown = wait.until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'month_from')]//span[@class='select-pick']"))
                    )
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", month_dropdown)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", month_dropdown)
                    time.sleep(2)

                    month_item = wait.until(
                        EC.presence_of_element_located((By.XPATH, f"//div[contains(@class, 'month_from')]//li[@data-filter-value='{month}']"))
                    )
                    driver.execute_script("arguments[0].click();", month_item)
                    logger.info(f"✓ Selected month {month}")

                    # Trigger change event on the underlying select element
                    try:
                        month_select = driver.find_element(By.XPATH, "//div[contains(@class, 'month_from')]//select")
                        driver.execute_script("""
                            arguments[0].value = arguments[1];
                            arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
                        """, month_select, month)
                        logger.info("✓ Triggered change event for month")
                    except:
                        pass

                    time.sleep(2)
                except Exception as e:
                    logger.error(f"✗ Failed to select month: {e}")
                    take_screenshot(driver, "07_month_failed", data_tag, logger)
                    return None

                take_screenshot(driver, "07_month_selected", data_tag, logger)

                # Select day (try with zero-padding)
                day_str = f"{day_num:02d}"  # Zero-padded like "04"
                logger.info(f"Step 8: Selecting day {day_str}...")
                try:
                    day_dropdown = wait.until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'day_from')]//span[@class='select-pick']"))
                    )
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", day_dropdown)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", day_dropdown)
                    logger.info("✓ Opened day dropdown")
                    time.sleep(2)

                    # Try both formats - first with zero padding, then without
                    day_item = None
                    for day_val in [day_str, str(day_num)]:
                        try:
                            day_item = driver.find_element(By.XPATH, f"//div[contains(@class, 'day_from')]//li[@data-filter-value='{day_val}']")
                            if day_item:
                                logger.info(f"Found day with value='{day_val}'")
                                break
                        except:
                            continue

                    if not day_item:
                        logger.error(f"✗ Day item not found for day {day_num}")
                        take_screenshot(driver, "08_day_not_in_dropdown", data_tag, logger)
                        return None

                    driver.execute_script("arguments[0].click();", day_item)
                    logger.info(f"✓ Selected day {day_num}")

                    # Trigger change event on the underlying select element
                    try:
                        day_select = driver.find_element(By.XPATH, "//div[contains(@class, 'day_from')]//select")
                        driver.execute_script("""
                            arguments[0].value = arguments[1];
                            arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
                        """, day_select, day_str)
                        logger.info("✓ Triggered change event for day")
                    except:
                        pass

                    time.sleep(3)  # Wait for any reactions to the change event
                except Exception as e:
                    logger.error(f"✗ Failed to select day: {e}")
                    take_screenshot(driver, "08_day_failed", data_tag, logger)
                    import traceback
                    logger.error(traceback.format_exc())
                    return None

                take_screenshot(driver, "08_day_selected", data_tag, logger)

                # Click "USE FILTER" button to apply the date selection
                logger.info("Step 9: Clicking 'USE FILTER' button...")
                try:
                    # Try multiple strategies to find the USE FILTER button
                    use_filter_button = None

                    # Strategy 1: Look for button with specific text variations
                    for button_text in ['USE FILTER', 'Zobrazit', 'Use filter', 'use filter']:
                        try:
                            use_filter_button = driver.find_element(By.XPATH, f"//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{button_text.lower()}')]")
                            if use_filter_button:
                                logger.info(f"Found button with text matching '{button_text}'")
                                break
                        except:
                            continue

                    # Strategy 2: Look for button in filter dialog area
                    if not use_filter_button:
                        try:
                            use_filter_button = driver.find_element(By.XPATH, "//button[contains(@class, 'btn') and ancestor::*[contains(@class, 'filter')]]")
                            if use_filter_button:
                                logger.info("Found button in filter area by class")
                        except:
                            pass

                    # Strategy 3: Look for any submit-like button in the visible area
                    if not use_filter_button:
                        try:
                            buttons = driver.find_elements(By.XPATH, "//button[@type='submit' or contains(@class, 'submit') or contains(@class, 'primary')]")
                            for btn in buttons:
                                if btn.is_displayed():
                                    use_filter_button = btn
                                    logger.info(f"Found visible submit button: {btn.text}")
                                    break
                        except:
                            pass

                    if not use_filter_button:
                        logger.error("✗ USE FILTER button not found with any strategy")
                        take_screenshot(driver, "09_use_filter_not_found", data_tag, logger)
                        return None

                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", use_filter_button)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", use_filter_button)
                    logger.info("✓ Clicked USE FILTER button")

                    # Wait for page's AJAX call to complete and graph to reload
                    # The USE FILTER button triggers loadGraphData which sets session state
                    logger.info("Waiting for page AJAX call to complete and graph to reload...")
                    time.sleep(15)  # Wait longer for AJAX, session update, and graph redraw

                    take_screenshot(driver, "09_filter_applied", data_tag, logger)

                    # Verify the correct date is now displayed on page
                    expected_date_str = f"{start_date.day:02d}. {start_date.month:02d}. {start_date.year}"  # Format: "DD. MM. YYYY"
                    logger.info(f"Verifying page shows date: {expected_date_str}")
                    try:
                        # Look for text showing the current data range
                        date_display = driver.find_element(By.XPATH, f"//*[contains(text(), '{expected_date_str}')]")
                        if date_display:
                            logger.info(f"✓ Verified date on page: {date_display.text}")
                        else:
                            logger.warning(f"⚠ Could not find '{expected_date_str}' on page")
                            take_screenshot(driver, "09_date_not_found", data_tag, logger)
                    except:
                        logger.warning(f"⚠ Could not verify date '{expected_date_str}' on page - download may have wrong date")
                        take_screenshot(driver, "09_date_verification_failed", data_tag, logger)

                        # Try to find what date IS displayed
                        try:
                            current_data_element = driver.find_element(By.XPATH, "//*[contains(text(), 'Current data:')]")
                            logger.warning(f"Page shows: {current_data_element.text}")
                        except:
                            pass

                except Exception as e:
                    logger.error(f"✗ Failed to click USE FILTER button: {e}")
                    take_screenshot(driver, "09_use_filter_failed", data_tag, logger)
                    import traceback
                    logger.error(traceback.format_exc())
                    return None

            else:
                logger.error("✗ Date range downloads not yet supported")
                return None

        # Click the CSV download button
        logger.info("Step 10: Clicking CSV download...")
        try:
            # Find the CSV download button
            csv_button = wait.until(
                EC.presence_of_element_located((By.XPATH, "//li[@data-download-type='csv']"))
            )

            # Scroll into view
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", csv_button)
            time.sleep(1)

            # Click with JavaScript
            driver.execute_script("arguments[0].click();", csv_button)
            logger.info("✓ Clicked CSV download")

        except TimeoutException:
            logger.error("✗ CSV download button not found")
            take_screenshot(driver, "04_download_button_not_found", data_tag, logger)
            return None

        take_screenshot(driver, "04_download_clicked", data_tag, logger)

        # Wait for download to complete
        logger.info("Step 5: Waiting for download...")
        time.sleep(10)

        # Find downloaded file
        download_dir = Path("/app/downloads/ceps")
        csv_files = list(download_dir.glob("*.csv"))

        if not csv_files:
            logger.error("✗ No CSV file downloaded")
            take_screenshot(driver, "05_no_file", data_tag, logger)
            return None

        # Get the most recent file
        latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Downloaded: {latest_file.name}")

        # Move to final location
        dest_dir = Path(f"/app/scripts/ceps/{start_date.year}/{start_date.month:02d}")
        dest_dir.mkdir(parents=True, exist_ok=True)

        if start_date.date() == end_date.date():
            date_str = start_date.strftime("%Y%m%d")
        else:
            date_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

        timestamp = datetime.now().strftime("%H%M%S")
        dest_file = dest_dir / f"data_{data_tag}_{date_str}_{timestamp}.csv"

        shutil.move(str(latest_file), str(dest_file))
        logger.info(f"✓ Saved: {dest_file}")

        return dest_file

    except Exception as e:
        logger.error(f"Error: {e}")
        take_screenshot(driver, "99_error", data_tag, logger)
        import traceback
        logger.error(traceback.format_exc())
        return None


def main():
    """Main entry point."""
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description='CEPS Simple Downloader')
    parser.add_argument('--tag', type=str, default='AktualniSystemovaOdchylkaCR',
                       help='CEPS data tag')
    parser.add_argument('--start-date', type=str, default=None,
                       help='Start date YYYY-MM-DD')
    parser.add_argument('--end-date', type=str, default=None,
                       help='End date YYYY-MM-DD')
    parser.add_argument('--debug', action='store_true',
                       help='Debug logging')
    args = parser.parse_args()

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

        # Set time to cover full day
        start_date = start_date.replace(hour=0, minute=0, second=0)
        end_date = end_date.replace(hour=23, minute=59, second=59)

    except ValueError as e:
        logger.error(f"Invalid date: {e}")
        logger.error("Date format must be YYYY-MM-DD (e.g., 2026-01-04)")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("CEPS Simple Headless Browser Downloader")
    logger.info(f"Tag: {args.tag}")
    logger.info(f"Date: {start_date.date()} to {end_date.date()}")
    logger.info("=" * 60)

    driver = None
    exit_code = 0

    try:
        logger.info("Starting browser...")
        driver = init_browser()

        downloaded_file = download_ceps_simple(driver, args.tag, start_date, end_date, logger)

        if downloaded_file:
            logger.info("=" * 60)
            logger.info("✓ SUCCESS")
            logger.info(f"File: {downloaded_file}")
            logger.info("=" * 60)
        else:
            logger.error("✗ FAILED")
            exit_code = 1

    except KeyboardInterrupt:
        logger.info("\nInterrupted")
        exit_code = 130
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
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
