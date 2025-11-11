#!/usr/bin/env python3
"""
OTE Portal Login Test
Verifies certificate setup and authentication is working correctly.
"""

import sys
import time
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from common import setup_logging
from config import OTE_LOCAL_STORAGE_PASSWORD


def take_screenshot(driver, name):
    """Take screenshot with timestamp."""
    try:
        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"/var/log/screenshot_{timestamp}_{name}.png"
        driver.save_screenshot(filename)
        print(f"📸 Screenshot: {filename}")
        return filename
    except Exception as e:
        print(f"Screenshot failed: {e}")
        return None


def init_browser():
    """Initialize Chrome browser."""
    chrome_options = Options()
    chrome_options.binary_location = "/usr/bin/chromium"
    chrome_options.add_argument("--user-data-dir=/app/browser-profile")

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


def test_login(driver, logger):
    """Test login to OTE portal."""
    wait = WebDriverWait(driver, 15)

    try:
        # Navigate to portal
        logger.info("Navigating to OTE portal...")
        driver.get("https://portal.ote-cr.cz/common/app/login")
        time.sleep(0.5)
        take_screenshot(driver, "portal_loaded")

        # Check for language button
        logger.info("Checking language settings...")
        try:
            lang_spans = driver.find_elements(By.XPATH, "//button//span[text()='EN' or text()='CZ']")
            if lang_spans:
                lang_text = lang_spans[0].text.strip()
                logger.info(f"✓ Language button found: {lang_text}")

                if lang_text == 'EN':
                    logger.info("  Currently in Czech, switching to English...")
                    lang_spans[0].find_element(By.XPATH, "..").click()
                    time.sleep(0.5)
                else:
                    logger.info("  Already in English")
        except:
            logger.warning("Language button not found")

        # Check if already logged in
        if "Watt Street, s.r.o." in driver.page_source:
            logger.info("✓ Already logged in!")
            logger.info("=" * 60)
            logger.info("✓ LOGIN TEST SUCCESSFUL!")
            logger.info(f"  Current URL: {driver.current_url}")
            logger.info("=" * 60)
            return True

        # Check certificate status
        cert_file = Path("/app/browser-profile/.cert_imported")
        if cert_file.exists():
            logger.info("✓ Certificate already imported")
        else:
            logger.warning("⚠ Certificate not imported - run with --setup first")
            return False

        # Try to login
        logger.info("Attempting login...")

        # Find login button
        login_btn = None
        try:
            login_btn = driver.find_element(By.XPATH, "//button[contains(., 'Log in')]")
            logger.info("✓ Found 'Log in' button (English)")
        except:
            try:
                login_btn = driver.find_element(By.XPATH, "//button[contains(., 'Přihlásit')]")
                logger.info("✓ Found 'Přihlásit' button (Czech)")
            except:
                logger.error("✗ Login button not found")
                take_screenshot(driver, "no_login_button")
                return False

        take_screenshot(driver, "before_login_click")
        login_btn.click()
        time.sleep(0.5)
        take_screenshot(driver, "after_login_click")

        # Handle password if needed
        try:
            password_field = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.XPATH, "//input[@type='password']"))
            )
            logger.info("Password field detected, entering credentials...")
            password_field.clear()
            password_field.send_keys(OTE_LOCAL_STORAGE_PASSWORD)

            confirm_btn = driver.find_element(By.XPATH, "//button[contains(., 'Confirm')]")
            confirm_btn.click()
            time.sleep(0.5)
            logger.info("✓ Password entered")
            take_screenshot(driver, "after_password_confirm")
        except TimeoutException:
            logger.info("No password field (certificate authentication ready)")
            take_screenshot(driver, "no_password_field")

        # Handle Sign button if present
        try:
            sign_btn = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Sign')]"))
            )
            take_screenshot(driver, "before_sign")
            sign_btn.click()
            time.sleep(3)
            logger.info("✓ Signed with certificate")
            take_screenshot(driver, "after_sign")
        except TimeoutException:
            logger.info("No sign button found")
            take_screenshot(driver, "no_sign_button")

        # Verify login success
        time.sleep(0.5)
        take_screenshot(driver, "login_verification")
        current_url = driver.current_url

        # Check for successful login by looking for company name
        if "Watt Street, s.r.o." in driver.page_source or "dashboard" in current_url or "login" not in current_url:
            logger.info("=" * 60)
            logger.info("✓ LOGIN TEST SUCCESSFUL!")
            logger.info(f"  Current URL: {current_url}")

            # Try to find user info
            try:
                if "Watt Street, s.r.o." in driver.page_source:
                    logger.info("✓ Company name 'Watt Street, s.r.o.' found")
            except:
                pass

            # Try to find user avatar
            try:
                avatar = driver.find_element(By.XPATH,
                    "//button[contains(@class, 'header-icon-avatar')]")
                if avatar:
                    logger.info("✓ User avatar found - fully authenticated")
            except:
                pass

            # Check for menu items
            try:
                settlement = driver.find_element(By.XPATH, "//*[contains(text(), 'Settlement')]")
                if settlement:
                    logger.info("✓ Settlement menu available")
            except:
                pass

            logger.info("=" * 60)
            return True
        else:
            logger.error("✗ LOGIN TEST FAILED")
            logger.error(f"  Still on login page: {current_url}")
            take_screenshot(driver, "login_failed")

            # Try to get any error messages on the page
            try:
                errors = driver.find_elements(By.XPATH, "//*[contains(@class, 'error') or contains(@class, 'alert')]")
                if errors:
                    for error in errors:
                        logger.error(f"  Error message: {error.text}")
            except:
                pass

            return False

    except Exception as e:
        logger.error(f"✗ Test failed with error: {e}")
        take_screenshot(driver, "test_exception")
        return False


def logout(driver):
    """Logout from portal."""
    try:
        avatar_btn = driver.find_element(By.XPATH,
            "//button[contains(@class, 'header-icon-avatar')]")
        avatar_btn.click()
        time.sleep(0.5)

        logout_item = driver.find_element(By.XPATH,
            "//div[@role='listitem' and @data-menu-item-value='logout']")
        logout_item.click()
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
    logger = setup_logging(debug='--debug' in sys.argv)

    logger.info("=" * 60)
    logger.info("OTE Portal Login Test")
    logger.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Clean up old screenshots
    cleanup_old_screenshots(logger)

    # Check configuration
    if not OTE_LOCAL_STORAGE_PASSWORD:
        logger.error("✗ OTE_LOCAL_STORAGE_PASSWORD not configured")
        logger.error("  Please set in .env file")
        sys.exit(1)

    logger.info("✓ Configuration loaded")

    driver = None
    success = False

    try:
        # Start browser
        logger.info("Starting browser...")
        driver = init_browser()
        logger.info("✓ Browser started")

        # Run login test
        success = test_login(driver, logger)

    except KeyboardInterrupt:
        logger.info("\nTest interrupted")
    except Exception as e:
        logger.error(f"Test error: {e}", exc_info=True)
    finally:
        if driver:
            try:
                if success:
                    logout(driver)
                    logger.info("✓ Logged out successfully")
                driver.quit()
                logger.info("✓ Browser closed")
            except:
                pass

    # Summary
    logger.info("")
    logger.info("=" * 60)
    if success:
        logger.info("TEST RESULT: PASSED ✓")
        logger.info("Certificate setup is working correctly!")
        logger.info("Ready for production deployment.")
        sys.exit(0)
    else:
        logger.info("TEST RESULT: FAILED ✗")
        logger.info("Please check:")
        logger.info("1. Run 'python3 ote_production.py --setup' first")
        logger.info("2. Verify OTE_LOCAL_STORAGE_PASSWORD is correct")
        logger.info("3. Check certificate file is valid")
        sys.exit(1)


if __name__ == "__main__":
    main()