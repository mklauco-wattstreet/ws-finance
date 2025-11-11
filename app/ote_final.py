#!/usr/bin/env python3
"""
FINAL WORKING OTE Portal Downloader - All fixes included.
- Fixed language detection (button with CZ/EN)
- Fixed date handling (Ctrl+A method)
- Handles both English and Czech login buttons
"""

import sys
import time
import shutil
from pathlib import Path
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from common import setup_logging
from config import OTE_CERT_PATH, OTE_CERT_PASSWORD, OTE_LOCAL_STORAGE_PASSWORD


def take_screenshot(driver, name):
    """Take screenshot with timestamp."""
    try:
        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"/var/log/screenshot_{timestamp}_{name}.png"
        driver.save_screenshot(filename)
        print(f"📸 Screenshot: {filename}")
    except:
        pass


def init_browser():
    """Initialize Chrome browser with correct settings."""
    chrome_options = Options()
    chrome_options.binary_location = "/usr/bin/chromium"
    chrome_options.add_argument("--user-data-dir=/app/browser-profile")

    # Download settings
    download_dir = "/app/downloads"
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
    chrome_options.add_argument("--ignore-certificate-errors")

    service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.implicitly_wait(10)

    return driver


def setup_certificate(driver, cert_path, cert_password, local_storage_password, logger):
    """
    Import certificate into portal's local storage (first time setup).
    """
    wait = WebDriverWait(driver, 15)

    logger.info("Setting up certificate...")

    # Click Certificate settings
    try:
        cert_btn = driver.find_element(By.XPATH, "//*[contains(text(), 'Certificate')]")
        cert_btn.click()
        time.sleep(0.5)
        logger.info("Opened Certificate settings")
    except Exception as e:
        logger.error(f"Failed to open Certificate settings: {e}")
        return False

    # Setup password
    try:
        password_field = driver.find_element(By.NAME, "password")
        password_field.clear()
        password_field.send_keys(local_storage_password)

        confirm_field = driver.find_element(By.NAME, "confirmedPassword")
        confirm_field.clear()
        confirm_field.send_keys(local_storage_password)

        save_btn = driver.find_element(By.XPATH, "//button[contains(text(), 'Save')]")
        save_btn.click()
        time.sleep(0.5)
        logger.info("Password saved")
    except:
        logger.info("Password already set")

    # Go to Certificates in local storage tab
    try:
        tab = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Certificates in local storage"))
        )
        tab.click()
        time.sleep(0.5)
    except:
        pass

    # Add certificate
    try:
        add_btn = driver.find_element(By.XPATH, "//button[contains(., 'Add certificate')]")
        add_btn.click()
        time.sleep(0.5)

        # Upload file
        file_input = wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@type='file']"))
        )
        file_input.send_keys(str(cert_path.absolute()))
        time.sleep(0.5)

        # Enter certificate password
        pwd_input = driver.find_element(By.XPATH, "//input[@type='password']")
        pwd_input.send_keys(cert_password)
        time.sleep(0.5)

        # Import
        import_btn = driver.find_element(By.XPATH, "//button[contains(., 'Import')]")
        import_btn.click()
        time.sleep(0.5)

        logger.info("✓ Certificate imported successfully")

        # Mark as imported
        cert_file = Path("/app/browser-profile/.cert_imported")
        cert_file.touch()

        return True

    except Exception as e:
        logger.error(f"Failed to import certificate: {e}")
        return False


def switch_to_english(driver, logger):
    """
    Ensure English language is selected.
    The language button shows:
    - "EN" = currently in Czech, need to click to switch to English
    - "CZ" = currently in English, no action needed
    """
    try:
        time.sleep(1)

        # Find the language button
        lang_button = None

        # Method 1: Find button containing span with EN or CZ
        try:
            spans = driver.find_elements(By.XPATH, "//button//span[text()='EN' or text()='CZ']")
            if spans:
                lang_text = spans[0].text.strip()
                lang_button = spans[0].find_element(By.XPATH, "..")  # Get parent button
            else:
                # Method 2: Find button with class containing header-icon
                buttons = driver.find_elements(By.XPATH, "//button[contains(@class, 'ote-header-icon')]")
                for btn in buttons:
                    if 'EN' in btn.text or 'CZ' in btn.text:
                        lang_text = btn.text.strip()
                        lang_button = btn
                        break
        except:
            pass

        if lang_button:
            if lang_text == 'EN':
                logger.info("Currently in Czech, switching to English...")
                lang_button.click()
                time.sleep(1)
                logger.info("Switched to English")
            elif lang_text == 'CZ':
                logger.info("Already in English (CZ button visible)")
            else:
                logger.warning(f"Unexpected language button text: '{lang_text}'")
        else:
            logger.warning("Language selector not found, continuing...")
    except Exception as e:
        logger.warning(f"Could not handle language: {e}")


def login_to_portal(driver, logger):
    """Login to OTE portal - handles both languages."""
    wait = WebDriverWait(driver, 15)

    try:
        # Find login button - try both languages
        login_btn = None

        # Try English first
        try:
            login_btn = driver.find_element(By.XPATH, "//button[contains(., 'Log in')]")
            logger.info("Found 'Log in' button (English)")
        except:
            # Try Czech
            try:
                login_btn = driver.find_element(By.XPATH, "//button[contains(., 'Přihlásit')]")
                logger.info("Found 'Přihlásit' button (Czech)")
            except:
                logger.error("Login button not found in English or Czech")
                return False

        login_btn.click()
        time.sleep(2)
        logger.info("Clicked login button")
        take_screenshot(driver, "after_login_button")

        # Enter password if needed
        try:
            password_field = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//input[@type='password']"))
            )
            password_field.clear()
            password_field.send_keys(OTE_LOCAL_STORAGE_PASSWORD)
            logger.info("Entered local storage password")

            # Click Confirm
            confirm_btn = driver.find_element(By.XPATH, "//button[contains(., 'Confirm')]")
            confirm_btn.click()
            time.sleep(1)
        except TimeoutException:
            logger.info("No password field (certificate ready)")

        # Click Sign if present
        try:
            sign_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Sign')]"))
            )
            sign_btn.click()
            logger.info("Clicked 'Sign' button")
        except TimeoutException:
            logger.info("No Sign button")

        # Verify login
        time.sleep(3)
        current_url = driver.current_url
        if "dashboard" in current_url or "login" not in current_url:
            logger.info(f"✓ Login successful! URL: {current_url}")
            return True

        logger.error("Login failed")
        return False

    except Exception as e:
        logger.error(f"Login error: {e}")
        return False


def set_date_field(driver, field, date_value, logger):
    """
    Set date field using Ctrl+A method (clear() doesn't work).
    """
    try:
        # Remove readonly attribute if present
        driver.execute_script("arguments[0].removeAttribute('readonly')", field)
        time.sleep(0.2)

        # Click to focus
        field.click()
        time.sleep(0.2)

        # Select all with Ctrl+A and type new value
        field.send_keys(Keys.CONTROL + "a")
        field.send_keys(date_value)

        # Verify
        actual = field.get_attribute("value")
        if actual == date_value:
            logger.info(f"✓ Date set to: {date_value}")
            return True
        else:
            logger.error(f"Failed to set date. Expected: {date_value}, Got: {actual}")
            return False

    except Exception as e:
        logger.error(f"Error setting date: {e}")
        return False


def download_daily_payments(driver, logger):
    """Download Daily Payments report."""
    wait = WebDriverWait(driver, 15)

    try:
        logger.info("Navigating to Daily Payments...")

        # Navigate: Settlement > Report > Daily payments
        settlement = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Settlement')]"))
        )
        settlement.click()
        time.sleep(1)

        report = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Report')]"))
        )
        report.click()
        time.sleep(1)

        daily_payments = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Daily payments')]"))
        )
        daily_payments.click()
        time.sleep(2)

        # Verify page loaded
        try:
            wait.until(EC.presence_of_element_located(
                (By.XPATH, "//span[contains(text(), 'Table with results')]")
            ))
            logger.info("Daily Payments page loaded")
        except:
            logger.warning("Could not verify page load")

        take_screenshot(driver, "daily_payments_page")

        # Set dates (use 2025 - current year)
        from_date = datetime(2025, 11, 8)
        to_date = datetime(2025, 11, 10)

        from_date_str = from_date.strftime("%d/%m/%Y")
        to_date_str = to_date.strftime("%d/%m/%Y")

        logger.info(f"Setting dates: {from_date_str} to {to_date_str}")

        # Get fields
        from_field = driver.find_element(By.NAME, "fromDate")
        to_field = driver.find_element(By.NAME, "toDate")

        # Set dates using our working method
        if not set_date_field(driver, from_field, from_date_str, logger):
            return False

        if not set_date_field(driver, to_field, to_date_str, logger):
            return False

        take_screenshot(driver, "dates_set")

        # Click Retrieve
        retrieve_btn = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Retrieve')]"))
        )
        retrieve_btn.click()
        logger.info("Clicked Retrieve, waiting for data...")

        time.sleep(10)
        take_screenshot(driver, "after_retrieve")

        # Check for data
        if "No data matches" in driver.page_source:
            logger.error("No data found for selected dates")

            # Try alternative dates
            from_date = datetime(2025, 11, 1)
            to_date = datetime(2025, 11, 7)
            from_date_str = from_date.strftime("%d/%m/%Y")
            to_date_str = to_date.strftime("%d/%m/%Y")

            logger.info(f"Trying alternative dates: {from_date_str} to {to_date_str}")

            set_date_field(driver, from_field, from_date_str, logger)
            set_date_field(driver, to_field, to_date_str, logger)

            retrieve_btn.click()
            time.sleep(10)

            if "No data matches" in driver.page_source:
                logger.error("Still no data")
                return False

        logger.info("✓ Data loaded!")

        # Download
        try:
            # Click download button
            download_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(@class, 'ote-btn-secondary') and contains(@class, 'ote-icon')]")
            ))
            download_btn.click()
            logger.info("Clicked download button")
            time.sleep(2)
            take_screenshot(driver, "download_dialog_opened")

            # Select XML - try multiple methods
            xml_selected = False

            # Method 1: Click the label containing XML
            try:
                xml_label = driver.find_element(By.XPATH,
                    "//label[contains(@class, 'ote-radio-wrapper')]//span[text()='XML']/..")
                xml_label.click()
                xml_selected = True
                logger.info("Selected XML via label")
            except:
                pass

            # Method 2: Click the radio input directly
            if not xml_selected:
                try:
                    xml_radio = driver.find_element(By.XPATH,
                        "//input[@type='radio' and @value='XML']")
                    xml_radio.click()
                    xml_selected = True
                    logger.info("Selected XML via radio input")
                except:
                    pass

            # Method 3: Use JavaScript
            if not xml_selected:
                try:
                    driver.execute_script("""
                        var radio = document.querySelector('input[type="radio"][value="XML"]');
                        if (radio) {
                            radio.checked = true;
                            radio.dispatchEvent(new Event('change', {bubbles: true}));
                        }
                    """)
                    xml_selected = True
                    logger.info("Selected XML via JavaScript")
                except:
                    logger.error("Could not select XML format")
                    return False

            time.sleep(1)
            take_screenshot(driver, "xml_selected")

            # Export
            export_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(., 'Export')]")
            ))
            export_btn.click()
            logger.info("✓ Export clicked - downloading...")
            time.sleep(2)
            take_screenshot(driver, "after_export_click")

            time.sleep(3)

            # Check for file
            download_dir = Path("/app/downloads")
            xml_files = list(download_dir.glob("*.xml"))

            if xml_files:
                latest_file = max(xml_files, key=lambda p: p.stat().st_mtime)

                # Move to final location
                dest_dir = Path(f"/app/ote_files/{from_date.year}/{from_date.month:02d}")
                dest_dir.mkdir(parents=True, exist_ok=True)

                dest_file = dest_dir / f"daily_payments_{from_date_str.replace('/', '-')}_to_{to_date_str.replace('/', '-')}.xml"
                shutil.move(str(latest_file), str(dest_file))

                logger.info(f"✓ File saved: {dest_file}")
                return True
            else:
                logger.error("No XML file found")
                return False

        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False

    except Exception as e:
        logger.error(f"Error: {e}")
        return False


def logout(driver, logger):
    """Logout from portal."""
    try:
        avatar_btn = driver.find_element(By.XPATH,
            "//button[contains(@class, 'ote-header-icon') and contains(@class, 'header-icon-avatar')]")
        avatar_btn.click()
        time.sleep(0.5)

        logout_item = driver.find_element(By.XPATH,
            "//div[@role='listitem' and @data-menu-item-value='logout']")
        logout_item.click()
        logger.info("Logged out")
    except:
        pass


def cleanup_old_screenshots(logger):
    """Delete all screenshots from previous runs."""
    import glob
    screenshot_files = glob.glob("/var/log/screenshot_*.png")
    if screenshot_files:
        for f in screenshot_files:
            try:
                Path(f).unlink()
            except:
                pass
        logger.info(f"Cleaned up {len(screenshot_files)} old screenshot(s)")


def main():
    # Setup logging
    logger = setup_logging(debug='--debug' in sys.argv)

    logger.info("=" * 60)
    logger.info("OTE Portal Daily Downloader")
    logger.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Clean all old screenshots
    cleanup_old_screenshots(logger)

    if not OTE_LOCAL_STORAGE_PASSWORD:
        logger.error("OTE_LOCAL_STORAGE_PASSWORD not configured")
        sys.exit(1)

    driver = None
    try:
        # Start browser
        logger.info("Starting browser...")
        driver = init_browser()

        # Navigate to portal
        logger.info("Navigating to portal...")
        driver.get("https://portal.ote-cr.cz/common/app/login")
        time.sleep(2)

        # Switch to English if needed
        switch_to_english(driver, logger)

        # Login
        logger.info("Logging in...")
        if not login_to_portal(driver, logger):
            logger.error("Login failed!")
            sys.exit(1)

        # Download
        logger.info("Starting download...")
        if download_daily_payments(driver, logger):
            logger.info("=" * 60)
            logger.info("✓ SUCCESS! Daily Payments downloaded!")
            logger.info("=" * 60)
        else:
            logger.info("=" * 60)
            logger.info("✗ FAILED - Check logs for details")
            logger.info("=" * 60)

        time.sleep(3)

    except KeyboardInterrupt:
        logger.info("\nInterrupted")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        if driver:
            logout(driver, logger)
            driver.quit()
            logger.info("Browser closed")


if __name__ == "__main__":
    main()