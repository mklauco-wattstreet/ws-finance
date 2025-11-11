#!/usr/bin/env python3
"""
OTE Portal Production Downloader
Optimized for automated daily execution in production environment.
"""

import sys
import time
import shutil
import subprocess
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


def init_browser():
    """Initialize Chrome browser for headless execution."""
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

    # Headless mode for production
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


def setup_certificate(driver, logger):
    """Setup certificate for first-time initialization."""
    wait = WebDriverWait(driver, 15)

    logger.info("Setting up certificate...")

    # Navigate to portal
    driver.get("https://portal.ote-cr.cz/common/app/login")
    time.sleep(2)  # Give page time to load

    take_screenshot(driver, "setup_page_loaded")

    # Try to switch to English first
    try:
        switch_to_english(driver, logger)
    except:
        logger.debug("Could not switch language")

    # Click Certificate settings - try both English and Czech
    cert_btn = None
    try:
        # Try English first
        try:
            cert_btn = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Certificate')]"))
            )
            logger.info("Found 'Certificate' button (English)")
        except:
            # Try Czech
            cert_btn = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Certifikát')]"))
            )
            logger.info("Found 'Certifikát' button (Czech)")

        cert_btn.click()
        time.sleep(0.5)
        logger.info("Opened Certificate settings")
        take_screenshot(driver, "certificate_settings_opened")
    except Exception as e:
        logger.error(f"Failed to open Certificate settings: {e}")
        take_screenshot(driver, "setup_error")
        logger.error("Make sure you're on the login page with Certificate options visible")
        return False

    # Setup password
    try:
        password_field = driver.find_element(By.NAME, "password")
        password_field.clear()
        password_field.send_keys(OTE_LOCAL_STORAGE_PASSWORD)

        confirm_field = driver.find_element(By.NAME, "confirmedPassword")
        confirm_field.clear()
        confirm_field.send_keys(OTE_LOCAL_STORAGE_PASSWORD)

        # Try both English and Czech for Save button
        try:
            save_btn = driver.find_element(By.XPATH, "//button[contains(text(), 'Save')]")
        except:
            save_btn = driver.find_element(By.XPATH, "//button[contains(text(), 'Uložit')]")

        save_btn.click()
        time.sleep(0.5)
        logger.info("Password saved")
    except:
        logger.info("Password already set")

    # Navigate to Certificates in local storage tab
    try:
        tab = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Certificates in local storage"))
        )
        tab.click()
        time.sleep(0.5)
    except:
        pass

    # Add certificate - try both English and Czech
    try:
        try:
            add_btn = driver.find_element(By.XPATH, "//button[contains(., 'Add certificate')]")
        except:
            add_btn = driver.find_element(By.XPATH, "//button[contains(., 'Přidat certifikát')]")

        add_btn.click()
        time.sleep(0.5)

        # Upload file
        file_input = wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@type='file']"))
        )
        file_input.send_keys(str(Path(OTE_CERT_PATH).absolute()))
        time.sleep(0.5)

        # Enter certificate password
        pwd_input = driver.find_element(By.XPATH, "//input[@type='password']")
        pwd_input.send_keys(OTE_CERT_PASSWORD)
        time.sleep(0.5)

        # Import - try both English and Czech
        try:
            import_btn = driver.find_element(By.XPATH, "//button[contains(., 'Import')]")
        except:
            import_btn = driver.find_element(By.XPATH, "//button[contains(., 'Importovat')]")

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
    """Ensure English language is selected."""
    try:
        time.sleep(0.5)

        # Find language button
        spans = driver.find_elements(By.XPATH, "//button//span[text()='EN' or text()='CZ']")
        if spans:
            lang_text = spans[0].text.strip()
            lang_button = spans[0].find_element(By.XPATH, "..")

            if lang_text == 'EN':
                logger.info("Switching to English...")
                lang_button.click()
                time.sleep(0.5)
            elif lang_text == 'CZ':
                logger.info("Already in English")
    except Exception as e:
        logger.debug(f"Language switch: {e}")


def login_to_portal(driver, logger):
    """Login to OTE portal."""
    wait = WebDriverWait(driver, 15)

    try:
        # Check if already logged in
        if "Watt Street, s.r.o." in driver.page_source:
            logger.info("✓ Already logged in!")
            return True

        # Find login button - try both languages
        login_btn = None
        try:
            login_btn = driver.find_element(By.XPATH, "//button[contains(., 'Log in')]")
        except:
            try:
                login_btn = driver.find_element(By.XPATH, "//button[contains(., 'Přihlásit')]")
            except:
                logger.error("Login button not found")
                return False

        login_btn.click()
        time.sleep(0.5)
        take_screenshot(driver, "after_login_button")

        # Enter password if needed
        try:
            password_field = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//input[@type='password']"))
            )
            password_field.clear()
            password_field.send_keys(OTE_LOCAL_STORAGE_PASSWORD)

            confirm_btn = driver.find_element(By.XPATH, "//button[contains(., 'Confirm')]")
            confirm_btn.click()
            time.sleep(0.5)
        except TimeoutException:
            pass

        # Click Sign if present
        try:
            sign_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Sign')]"))
            )
            sign_btn.click()
            time.sleep(3)  # Keep 3 seconds here as requested
            take_screenshot(driver, "after_sign")
        except TimeoutException:
            pass

        # Verify login - check for company name or dashboard
        time.sleep(0.5)
        take_screenshot(driver, "login_verification")
        current_url = driver.current_url

        if "Watt Street, s.r.o." in driver.page_source or "dashboard" in current_url or "login" not in current_url:
            logger.info("✓ Login successful")
            return True

        logger.error("Login failed")
        take_screenshot(driver, "login_failed")
        return False

    except Exception as e:
        logger.error(f"Login error: {e}")
        return False


def set_date_field(driver, field, date_value):
    """Set date field using Ctrl+A method."""
    try:
        driver.execute_script("arguments[0].removeAttribute('readonly')", field)
        time.sleep(0.2)

        field.click()
        time.sleep(0.2)

        field.send_keys(Keys.CONTROL + "a")
        field.send_keys(date_value)

        return field.get_attribute("value") == date_value

    except:
        return False


def download_daily_payments(driver, logger):
    """Download Daily Payments report for previous days."""
    wait = WebDriverWait(driver, 15)

    try:
        logger.info("Navigating to Daily Payments...")

        # Navigate: Settlement > Report > Daily payments
        settlement = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Settlement')]"))
        )
        settlement.click()
        time.sleep(0.5)

        report = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Report')]"))
        )
        report.click()
        time.sleep(0.5)

        daily_payments = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Daily payments')]"))
        )
        daily_payments.click()
        time.sleep(0.5)
        take_screenshot(driver, "daily_payments_page")

        # Set dates - last 7 days
        to_date = datetime.now() - timedelta(days=1)  # Yesterday
        from_date = to_date - timedelta(days=6)  # 7 days ago

        from_date_str = from_date.strftime("%d/%m/%Y")
        to_date_str = to_date.strftime("%d/%m/%Y")

        logger.info(f"Downloading data: {from_date_str} to {to_date_str}")

        # Set date fields
        from_field = driver.find_element(By.NAME, "fromDate")
        to_field = driver.find_element(By.NAME, "toDate")

        if not set_date_field(driver, from_field, from_date_str):
            logger.error("Failed to set from date")
            return False

        if not set_date_field(driver, to_field, to_date_str):
            logger.error("Failed to set to date")
            return False

        # Click Retrieve
        retrieve_btn = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Retrieve')]"))
        )
        retrieve_btn.click()
        logger.info("Retrieving data...")

        # Wait for data
        time.sleep(10)
        take_screenshot(driver, "after_retrieve")

        # Check if data exists
        if "No data matches" in driver.page_source:
            logger.warning("No data found for selected dates")
            take_screenshot(driver, "no_data_found")
            return False

        logger.info("Data loaded, downloading...")

        # Click download button
        download_btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(@class, 'ote-btn-secondary') and contains(@class, 'ote-icon')]")
        ))
        download_btn.click()
        time.sleep(0.5)
        take_screenshot(driver, "download_dialog")

        # Select XML format
        try:
            xml_label = driver.find_element(By.XPATH,
                "//label[contains(@class, 'ote-radio-wrapper')]//span[text()='XML']/..")
            xml_label.click()
        except:
            try:
                xml_radio = driver.find_element(By.XPATH,
                    "//input[@type='radio' and @value='XML']")
                xml_radio.click()
            except:
                driver.execute_script("""
                    var radio = document.querySelector('input[type="radio"][value="XML"]');
                    if (radio) {
                        radio.checked = true;
                        radio.dispatchEvent(new Event('change', {bubbles: true}));
                    }
                """)

        time.sleep(0.5)

        # Export
        export_btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(., 'Export')]")
        ))
        export_btn.click()
        logger.info("Exporting...")

        time.sleep(5)

        # Check for downloaded file
        download_dir = Path("/app/downloads")
        xml_files = list(download_dir.glob("*.xml"))

        if xml_files:
            latest_file = max(xml_files, key=lambda p: p.stat().st_mtime)

            # Move to final location
            dest_dir = Path(f"/app/ote_files/{from_date.year}/{from_date.month:02d}")
            dest_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest_file = dest_dir / f"daily_payments_{timestamp}.xml"
            shutil.move(str(latest_file), str(dest_file))

            logger.info(f"✓ File saved: {dest_file}")
            return str(dest_file)  # Return file path for upload
        else:
            logger.error("No XML file found in downloads")
            return None

    except Exception as e:
        logger.error(f"Download error: {e}")
        return None


def upload_to_database(xml_file_path, logger):
    """
    Upload downloaded XML file to database.

    Args:
        xml_file_path: Path to the downloaded XML file
        logger: Logger instance

    Returns:
        bool: True if upload successful, False otherwise
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("Starting database upload...")
    logger.info("=" * 60)
    logger.info("")

    try:
        # Call the upload script - let output flow through naturally
        result = subprocess.run(
            ['/usr/local/bin/python3', '/app/scripts/ote_upload_daily_payments.py', xml_file_path],
            timeout=300  # 5 minute timeout
        )

        logger.info("")
        if result.returncode == 0:
            logger.info("=" * 60)
            logger.info("✓ Database upload completed successfully")
            logger.info("=" * 60)
            return True
        else:
            logger.error("=" * 60)
            logger.error("✗ Database upload failed (see details above)")
            logger.error("=" * 60)
            return False

    except subprocess.TimeoutExpired:
        logger.error("✗ Database upload timeout (exceeded 5 minutes)")
        return False
    except Exception as e:
        logger.error(f"✗ Error running upload script: {e}")
        return False


def logout(driver):
    """Logout from portal."""
    try:
        avatar_btn = driver.find_element(By.XPATH,
            "//button[contains(@class, 'ote-header-icon') and contains(@class, 'header-icon-avatar')]")
        avatar_btn.click()
        time.sleep(0.5)

        logout_item = driver.find_element(By.XPATH,
            "//div[@role='listitem' and @data-menu-item-value='logout']")
        logout_item.click()
    except:
        pass


def main():
    # Setup logging
    logger = setup_logging(debug='--debug' in sys.argv)

    logger.info("=" * 60)
    logger.info("OTE Portal Production Downloader")
    logger.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Clean up old screenshots
    cleanup_old_screenshots(logger)

    # Check configuration
    if not OTE_LOCAL_STORAGE_PASSWORD:
        logger.error("OTE_LOCAL_STORAGE_PASSWORD not configured")
        sys.exit(1)

    driver = None
    exit_code = 0

    try:
        # Initialize browser
        logger.info("Starting browser...")
        driver = init_browser()

        # Handle --setup flag
        if '--setup' in sys.argv:
            logger.info("Certificate setup mode")
            if not OTE_CERT_PATH or not OTE_CERT_PASSWORD:
                logger.error("OTE_CERT_PATH and OTE_CERT_PASSWORD required for setup")
                sys.exit(1)

            if setup_certificate(driver, logger):
                logger.info("✓ Certificate setup completed successfully")
            else:
                logger.error("Certificate setup failed")
                exit_code = 1
        else:
            # Normal download mode
            logger.info("Navigating to portal...")
            driver.get("https://portal.ote-cr.cz/common/app/login")
            time.sleep(0.5)

            # Switch to English
            switch_to_english(driver, logger)

            # Login
            logger.info("Logging in...")
            if not login_to_portal(driver, logger):
                logger.error("Login failed")
                exit_code = 1
            else:
                # Download
                logger.info("Starting download...")
                downloaded_file = download_daily_payments(driver, logger)

                if downloaded_file:
                    logger.info("✓ SUCCESS - Daily Payments downloaded")

                    # Upload to database
                    if upload_to_database(downloaded_file, logger):
                        logger.info("=" * 60)
                        logger.info("✓ COMPLETE - Download and upload successful")
                        logger.info("=" * 60)
                    else:
                        logger.error("=" * 60)
                        logger.error("⚠ PARTIAL SUCCESS - Download OK, Upload FAILED")
                        logger.error("=" * 60)
                        exit_code = 1
                else:
                    logger.error("✗ FAILED - Download unsuccessful")
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
                logout(driver)
                driver.quit()
                logger.info("Browser closed")
            except:
                pass

    sys.exit(exit_code)


if __name__ == "__main__":
    main()