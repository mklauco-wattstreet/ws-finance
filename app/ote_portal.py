#!/usr/bin/env python3
"""
Shared Selenium helpers for the OTE portal (portal.ote-cr.cz).

Lifted from ote_trade_balance_downloader.py so new portal downloaders do not
carry their own copy of the browser / certificate / login boilerplate. The
older downloaders (ote_production.py, ote_trade_balance_downloader.py) still
own their copies on purpose - they are proven in production and were not
touched when this module was introduced.

Every helper takes the Selenium ``driver`` and a ``logger``; screenshots go to
/var/log/screenshot_<HHMMSS>_<name>.png (./logs on the host) so a failed run
can be replayed visually.
"""

import glob
import time
from datetime import datetime
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from config import OTE_CERT_PATH, OTE_CERT_PASSWORD, OTE_LOCAL_STORAGE_PASSWORD

PORTAL_LOGIN_URL = "https://portal.ote-cr.cz/common/app/login"
DOWNLOAD_DIR = Path("/app/downloads")
BROWSER_PROFILE_DIR = "/app/browser-profile"
SCREENSHOT_GLOB = "/var/log/screenshot_*.png"

# Set by the calling script so screenshots from different downloaders don't
# clobber each other's cleanup (each script only deletes its own prefix).
_screenshot_prefix = "screenshot"


def set_screenshot_prefix(prefix):
    global _screenshot_prefix
    _screenshot_prefix = prefix


def take_screenshot(driver, name):
    """Save a timestamped screenshot; never raises."""
    try:
        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"/var/log/{_screenshot_prefix}_{timestamp}_{name}.png"
        driver.save_screenshot(filename)
        print(f"📸 Screenshot: {filename}")
    except Exception:
        pass


def cleanup_old_screenshots(logger):
    """Delete this script's screenshots from previous runs."""
    files = glob.glob(f"/var/log/{_screenshot_prefix}_*.png")
    for f in files:
        try:
            Path(f).unlink()
        except Exception:
            pass
    if files:
        logger.debug(f"Cleaned up {len(files)} old screenshot(s)")


def init_browser():
    """Headless Chromium with the persistent portal profile and download dir."""
    chrome_options = Options()
    chrome_options.binary_location = "/usr/bin/chromium"
    chrome_options.add_argument(f"--user-data-dir={BROWSER_PROFILE_DIR}")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
    }
    chrome_options.add_experimental_option("prefs", prefs)

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


def is_maintenance_page(driver):
    src = driver.page_source
    return "portal is not available" in src or "je nedostupný" in src


def switch_to_english(driver, logger):
    """Ensure English UI. The language button shows the *other* language's code."""
    try:
        time.sleep(0.5)
        spans = driver.find_elements(By.XPATH, "//button//span[text()='EN' or text()='CZ']")
        if spans:
            lang_text = spans[0].text.strip()
            lang_button = spans[0].find_element(By.XPATH, "..")
            if lang_text == 'EN':
                logger.debug("Switching to English...")
                lang_button.click()
                time.sleep(0.5)
            elif lang_text == 'CZ':
                logger.debug("Already in English")
    except Exception as e:
        logger.debug(f"Language switch: {e}")


def login_to_portal(driver, logger):
    """Log in with the certificate already imported into the browser profile."""
    try:
        if "Watt Street, s.r.o." in driver.page_source:
            logger.debug("Already logged in")
            return True

        login_btn = None
        try:
            login_btn = driver.find_element(By.XPATH, "//button[contains(., 'Log in')]")
        except Exception:
            try:
                login_btn = driver.find_element(By.XPATH, "//button[contains(., 'Přihlásit')]")
            except Exception:
                logger.warning("Login button not found")
                return False

        login_btn.click()
        time.sleep(0.5)
        take_screenshot(driver, "after_login_button")

        # Local-storage password prompt (protects the imported certificate)
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

        # Sign the login challenge
        try:
            sign_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Sign')]"))
            )
            sign_btn.click()
            time.sleep(3)
            take_screenshot(driver, "after_sign")
        except TimeoutException:
            pass

        time.sleep(0.5)
        take_screenshot(driver, "login_verification")
        current_url = driver.current_url
        if ("Watt Street, s.r.o." in driver.page_source
                or "dashboard" in current_url
                or "login" not in current_url):
            logger.debug("Login successful")
            return True

        logger.error("Login failed")
        take_screenshot(driver, "login_failed")
        return False

    except Exception as e:
        logger.error(f"Login error: {e}")
        return False


def set_date_field(driver, field, date_value):
    """Set an ote-picker input using the proven remove-readonly + Ctrl+A method."""
    try:
        driver.execute_script("arguments[0].removeAttribute('readonly')", field)
        time.sleep(0.2)
        field.click()
        time.sleep(0.2)
        field.send_keys(Keys.CONTROL + "a")
        field.send_keys(date_value)
        return field.get_attribute("value") == date_value
    except Exception:
        return False


def logout(driver):
    """Best-effort logout via the avatar menu."""
    try:
        avatar_btn = driver.find_element(
            By.XPATH,
            "//button[contains(@class, 'ote-header-icon') and contains(@class, 'header-icon-avatar')]",
        )
        avatar_btn.click()
        time.sleep(0.5)
        logout_item = driver.find_element(
            By.XPATH, "//div[@role='listitem' and @data-menu-item-value='logout']"
        )
        logout_item.click()
    except Exception:
        pass


def wait_for_new_download(existing, suffix, timeout, logger, driver=None, screenshot_every=15):
    """Block until a new, fully written *<suffix> file appears in DOWNLOAD_DIR.

    ``existing`` is the set of paths present before the download was triggered.
    A file is considered complete once no ``*.crdownload`` sibling remains and
    its size is stable across two polls. Returns the Path or None on timeout.
    """
    deadline = time.monotonic() + timeout
    last_shot = time.monotonic()
    last_size = None
    candidate = None
    while time.monotonic() < deadline:
        partial = list(DOWNLOAD_DIR.glob("*.crdownload"))
        new_files = [p for p in DOWNLOAD_DIR.glob(f"*{suffix}") if p not in existing]
        if new_files and not partial:
            candidate = max(new_files, key=lambda p: p.stat().st_mtime)
            size = candidate.stat().st_size
            if size > 0 and size == last_size:
                return candidate
            last_size = size
        if driver is not None and time.monotonic() - last_shot >= screenshot_every:
            take_screenshot(driver, "waiting_for_download")
            last_shot = time.monotonic()
        time.sleep(2)
    logger.error(f"Timed out after {timeout}s waiting for a new {suffix} download")
    return None
