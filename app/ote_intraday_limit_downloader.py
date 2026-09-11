#!/usr/bin/env python3
"""
OTE Portal - "Fin. security trend for limit IM" downloader.

Logs in to portal.ote-cr.cz with the certificate already imported into the
shared browser profile (run ote_trade_balance_downloader.py --setup once per
host), walks Risk Manag. > Financial security, picks the report
"Fin. security trend for limit IM" (reportCode financial_security_trend_offline_main),
sets the date range, exports XLSX, stores it under
/app/ote_files/YYYY/MM/Intraday_limit_<from>_<to>_<stamp>.xlsx and hands it to
upload_intraday_limit.py.

Usage:
    python3 ote_intraday_limit_downloader.py [--today | --yesterday | --start YYYY-MM-DD --end YYYY-MM-DD]
                                            [--dry-run] [--debug] [--timeout SECONDS]

    default      : today's events  (cron 12:30)
    --yesterday  : yesterday's events (cron 00:30, closes the day)
    --start/--end: explicit range for manual backfill. The portal generates
                   the report server-side and one day already takes ~30 s+;
                   keep ranges short.
    --dry-run    : download only, skip the database upload.

The report period filters on the order event timestamp, not the delivery
day. Every run is a full login + one report generation - this is a live
system, do not loop it.
"""

import sentry_init  # noqa: F401 - must be first to capture errors
sentry_init.set_module("ote")
import argparse
import shutil
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from common import setup_logging
from config import OTE_LOCAL_STORAGE_PASSWORD
import ote_portal
from ote_portal import take_screenshot

REPORT_CODE = "financial_security_trend_offline_main"
FINANCIAL_SECURITY_PATH = "/sfvot/app/financial_security"
FINANCIAL_SECURITY_URL = f"https://portal.ote-cr.cz{FINANCIAL_SECURITY_PATH}"
OTE_FILES_DIR = Path("/app/ote_files")
UPLOAD_SCRIPT = "/app/scripts/upload_intraday_limit.py"


def navigate_to_financial_security(driver, logger):
    """Sidebar: Risk Manag. > Financial security. Falls back to the direct URL."""
    wait = WebDriverWait(driver, 15)
    link_xpath = f"//a[@href='{FINANCIAL_SECURITY_PATH}']"

    driver.implicitly_wait(2)
    try:
        links = [l for l in driver.find_elements(By.XPATH, link_xpath) if l.is_displayed()]
        if not links:
            logger.debug("Expanding 'Risk Manag.' sidebar group")
            risk = wait.until(EC.element_to_be_clickable((
                By.XPATH,
                "//li[contains(@class, 'ote-sidebar-item')][div[normalize-space()='Risk Manag.']]",
            )))
            risk.click()
            time.sleep(0.5)
            take_screenshot(driver, "risk_manag_expanded")
        link = wait.until(EC.element_to_be_clickable((By.XPATH, link_xpath)))
        link.click()
        logger.debug("Clicked 'Financial security'")
    except TimeoutException:
        logger.warning("Sidebar navigation failed, loading the URL directly")
        take_screenshot(driver, "sidebar_nav_failed")
        driver.get(FINANCIAL_SECURITY_URL)
    finally:
        driver.implicitly_wait(10)

    wait.until(EC.presence_of_element_located((By.NAME, "reportCode")))
    time.sleep(0.5)
    take_screenshot(driver, "financial_security_page")


def select_report(driver, logger):
    wait = WebDriverWait(driver, 15)
    current = driver.find_element(By.NAME, "reportCode").get_attribute("value")
    if current == REPORT_CODE:
        logger.debug("Report already selected")
        return True

    selector = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//input[@name='reportCode']/ancestor::div[contains(@class, 'ote-select-selector')]",
    )))
    selector.click()
    time.sleep(0.5)
    take_screenshot(driver, "report_dropdown_open")

    option = wait.until(EC.element_to_be_clickable((
        By.XPATH, f"//div[@role='listitem' and @data-menu-item-value='{REPORT_CODE}']",
    )))
    option.click()
    time.sleep(0.5)

    current = driver.find_element(By.NAME, "reportCode").get_attribute("value")
    take_screenshot(driver, "report_selected")
    if current != REPORT_CODE:
        logger.error(f"Report select failed: reportCode={current!r}")
        return False
    logger.debug("Selected 'Fin. security trend for limit IM'")
    return True


def set_date_range(driver, logger, date_from, date_to):
    wait = WebDriverWait(driver, 15)
    from_str = date_from.strftime("%d/%m/%Y")
    to_str = date_to.strftime("%d/%m/%Y")

    from_field = wait.until(EC.presence_of_element_located((By.NAME, "dateFrom")))
    to_field = driver.find_element(By.NAME, "dateTo")

    if not ote_portal.set_date_field(driver, from_field, from_str):
        logger.error("Failed to set dateFrom")
        take_screenshot(driver, "date_from_failed")
        return False
    if not ote_portal.set_date_field(driver, to_field, to_str):
        logger.error("Failed to set dateTo")
        take_screenshot(driver, "date_to_failed")
        return False

    # Close the picker popup by clicking the page heading, then re-read the
    # inputs: a range picker may revert an unconfirmed value on blur, and a
    # wrong range would still cost a server-side report generation.
    try:
        driver.find_element(By.XPATH, "//*[normalize-space()='Choose report']").click()
    except Exception:
        pass
    time.sleep(0.5)

    got_from = driver.find_element(By.NAME, "dateFrom").get_attribute("value")
    got_to = driver.find_element(By.NAME, "dateTo").get_attribute("value")
    take_screenshot(driver, "dates_set")
    if (got_from, got_to) != (from_str, to_str):
        logger.error(f"Date range did not stick: got {got_from} - {got_to}, wanted {from_str} - {to_str}")
        return False
    logger.debug(f"Date range set: {from_str} - {to_str}")
    return True


def ensure_excel_export(driver, logger):
    """Excel is the default; verify and click it if something else is selected."""
    try:
        label = driver.find_element(
            By.XPATH, "//label[contains(@class, 'ote-radio-wrapper')][.//*[normalize-space()='Excel']]"
        )
        radio = label.find_element(By.XPATH, ".//input[@type='radio']")
        if not radio.is_selected():
            label.click()
            time.sleep(0.3)
            logger.debug("Selected Excel export type")
        return radio.is_selected()
    except Exception as e:
        logger.warning(f"Could not verify export type radio, assuming default Excel: {e}")
        return True


def generate_and_download(driver, logger, timeout):
    wait = WebDriverWait(driver, 15)
    existing = set(ote_portal.DOWNLOAD_DIR.glob("*.xlsx"))

    generate_btn = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//button[contains(@class, 'ote-btn-primary')][.//span[normalize-space()='Generate']]",
    )))
    generate_btn.click()
    logger.info("Generate clicked, waiting for the portal to build the report...")
    time.sleep(3)
    take_screenshot(driver, "after_generate")

    return ote_portal.wait_for_new_download(existing, ".xlsx", timeout, logger, driver=driver)


def store_file(downloaded, date_from, date_to, logger):
    dest_dir = OTE_FILES_DIR / f"{date_from.year}" / f"{date_from.month:02d}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = dest_dir / f"Intraday_limit_{date_from:%Y%m%d}_{date_to:%Y%m%d}_{stamp}.xlsx"
    shutil.move(str(downloaded), str(dest))
    logger.info(f"File saved: {dest} ({dest.stat().st_size} bytes)")
    return dest


def upload_to_database(xlsx_path, logger, debug):
    cmd = ["/usr/local/bin/python3", UPLOAD_SCRIPT, str(xlsx_path)]
    if debug:
        cmd.append("--debug")
    try:
        result = subprocess.run(cmd, timeout=300)
        if result.returncode == 0:
            return True
        logger.error(f"Upload script exited with {result.returncode}")
        return False
    except subprocess.TimeoutExpired:
        logger.error("Database upload timeout (exceeded 5 minutes)")
        return False


def parse_args():
    p = argparse.ArgumentParser(description="Download OTE 'Fin. security trend for limit IM' report")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--today", action="store_true", help="today's events (default)")
    g.add_argument("--yesterday", action="store_true", help="yesterday's events")
    g.add_argument("--start", type=date.fromisoformat, help="range start YYYY-MM-DD (needs --end)")
    p.add_argument("--end", type=date.fromisoformat, help="range end YYYY-MM-DD")
    p.add_argument("--timeout", type=int, default=300, help="seconds to wait for the generated file")
    p.add_argument("--dry-run", action="store_true", help="download only, skip DB upload")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()

    today = datetime.now().date()
    if args.start:
        if not args.end:
            p.error("--start requires --end")
        if args.end < args.start:
            p.error("--end must not be before --start")
        args.date_from, args.date_to = args.start, args.end
    elif args.yesterday:
        args.date_from = args.date_to = today - timedelta(days=1)
    else:
        args.date_from = args.date_to = today
    return args


def main():
    args = parse_args()
    logger = setup_logging(debug=args.debug)
    ote_portal.set_screenshot_prefix("screenshot_idlimit")

    logger.info(f"OTE IntradayLimit: started ({args.date_from} .. {args.date_to})")
    ote_portal.cleanup_old_screenshots(logger)

    if not OTE_LOCAL_STORAGE_PASSWORD:
        logger.error("OTE_LOCAL_STORAGE_PASSWORD not configured")
        sys.exit(1)

    driver = None
    exit_code = 0
    try:
        driver = ote_portal.init_browser()
        driver.get(ote_portal.PORTAL_LOGIN_URL)
        time.sleep(0.5)

        if ote_portal.is_maintenance_page(driver):
            logger.warning("OTE portal is in maintenance mode, skipping")
            sys.exit(0)

        ote_portal.switch_to_english(driver, logger)
        if not ote_portal.login_to_portal(driver, logger):
            raise RuntimeError("OTE IntradayLimit: login failed")

        navigate_to_financial_security(driver, logger)
        if not select_report(driver, logger):
            raise RuntimeError("OTE IntradayLimit: report selection failed")
        if not set_date_range(driver, logger, args.date_from, args.date_to):
            raise RuntimeError("OTE IntradayLimit: date range not set")
        if not ensure_excel_export(driver, logger):
            raise RuntimeError("OTE IntradayLimit: Excel export type not selected")

        downloaded = generate_and_download(driver, logger, args.timeout)
        if not downloaded:
            take_screenshot(driver, "download_timeout")
            raise RuntimeError("OTE IntradayLimit: no XLSX arrived")

        stored = store_file(downloaded, args.date_from, args.date_to, logger)

        if args.dry_run:
            logger.info(f"OTE IntradayLimit: dry run, downloaded {stored}")
        elif upload_to_database(stored, logger, args.debug):
            logger.info(f"OTE IntradayLimit: downloaded and uploaded {stored}")
        else:
            raise RuntimeError("OTE IntradayLimit: download OK, upload failed")

    except SystemExit:
        raise
    except KeyboardInterrupt:
        exit_code = 130
    except Exception as e:
        logger.error(f"OTE IntradayLimit: {e}", exc_info=True)
        if driver:
            take_screenshot(driver, "error")
        exit_code = 1
    finally:
        if driver:
            try:
                ote_portal.logout(driver)
                driver.quit()
            except Exception:
                pass

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
