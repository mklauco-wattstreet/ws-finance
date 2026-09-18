#!/usr/bin/env python3
"""
OTE Portal - one login, several reports.

Every portal report used to be its own script with its own Chromium start,
certificate login and logout. This runner logs in once and walks the
requested reports in sequence, so the login count per day is set by the
cron cadence, not by the number of reports.

Reports (run in this order):
    trade_balance   ote_trade_balance_downloader.download_trade_balance
                    D-1..D+1 -> ote_trade_balance
    intraday_limit  ote_intraday_limit_downloader.run_report
                    today's events -> ote_intraday_limit_utilization;
                    with --limit-yesterday also yesterday's (closes the day)
    fs_trend        same page, report "Fin. security trend"
                    -> ote_financial_security_trend; same day logic

Daily payments (ote_production.py) stay separate: the settlement is
aggregated once, in the morning for the previous day, so one 09:00 login
is the right cadence for it.

Usage:
    python3 ote_portal_session.py [--reports trade_balance,intraday_limit]
                                  [--limit-yesterday] [--dry-run] [--debug]

Each report is isolated: a failure is logged (with screenshot) and the
next report still runs; the exit code is non-zero if any report failed.
"""

import sentry_init  # noqa: F401 - must be first to capture errors
sentry_init.set_module("ote")
import argparse
import sys
import time
from datetime import datetime, timedelta

from common import setup_logging
from config import OTE_LOCAL_STORAGE_PASSWORD
import ote_portal
from ote_portal import take_screenshot
import ote_trade_balance_downloader as trade_balance
import ote_intraday_limit_downloader as intraday_limit

ALL_REPORTS = ("trade_balance", "intraday_limit", "fs_trend")

FS_TREND = intraday_limit.ReportSpec(
    code="financial_security_trend_main",
    file_prefix="FS_trend",
    upload_script="/app/scripts/upload_financial_security_trend.py",
    label="OTE FSTrend",
)


def run_trade_balance(driver, logger, args):
    path = trade_balance.download_trade_balance(driver, logger)
    if not path:
        raise RuntimeError("trade balance download failed")
    if args.dry_run:
        logger.info(f"OTE TradeBalance: dry run, downloaded {path}")
        return
    if not trade_balance.upload_to_database(path, logger):
        raise RuntimeError("trade balance download OK, upload failed")
    logger.info(f"OTE TradeBalance: downloaded and uploaded {path}")


def _fs_days(args):
    today = datetime.now().date()
    days = [today]
    if args.limit_yesterday:
        days.insert(0, today - timedelta(days=1))
    return days


def run_intraday_limit(driver, logger, args):
    for day in _fs_days(args):
        intraday_limit.run_report(driver, logger, day, day,
                                  timeout=args.timeout, dry_run=args.dry_run, debug=args.debug)


def run_fs_trend(driver, logger, args):
    for day in _fs_days(args):
        intraday_limit.run_report(driver, logger, day, day,
                                  timeout=args.timeout, dry_run=args.dry_run, debug=args.debug,
                                  spec=FS_TREND)


REPORT_RUNNERS = {
    "trade_balance": run_trade_balance,
    "intraday_limit": run_intraday_limit,
    "fs_trend": run_fs_trend,
}


def parse_args():
    p = argparse.ArgumentParser(description="OTE portal: one login, several reports")
    p.add_argument("--reports", default=",".join(ALL_REPORTS),
                   help=f"comma-separated subset of {','.join(ALL_REPORTS)} (default: all)")
    p.add_argument("--limit-yesterday", action="store_true",
                   help="intraday_limit and fs_trend: also fetch yesterday's events (closing the day)")
    p.add_argument("--timeout", type=int, default=300,
                   help="seconds to wait for a generated report file")
    p.add_argument("--dry-run", action="store_true", help="download only, skip DB uploads")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()
    args.reports = [r.strip() for r in args.reports.split(",") if r.strip()]
    unknown = [r for r in args.reports if r not in REPORT_RUNNERS]
    if unknown:
        p.error(f"unknown report(s): {', '.join(unknown)}")
    return args


def main():
    args = parse_args()
    logger = setup_logging(debug=args.debug)
    ote_portal.set_screenshot_prefix("screenshot_session")

    logger.info(f"OTE PortalSession: started ({', '.join(args.reports)})")
    trade_balance.cleanup_old_screenshots(logger)  # clears every screenshot_*.png

    if not OTE_LOCAL_STORAGE_PASSWORD:
        logger.error("OTE_LOCAL_STORAGE_PASSWORD not configured")
        sys.exit(1)

    driver = None
    failed = []
    try:
        driver = ote_portal.init_browser()
        driver.get(ote_portal.PORTAL_LOGIN_URL)
        time.sleep(0.5)

        if ote_portal.is_maintenance_page(driver):
            logger.warning("OTE portal is in maintenance mode, skipping")
            sys.exit(0)

        ote_portal.switch_to_english(driver, logger)
        if not ote_portal.login_to_portal(driver, logger):
            raise RuntimeError("OTE PortalSession: login failed")

        for name in args.reports:
            started = time.monotonic()
            try:
                REPORT_RUNNERS[name](driver, logger, args)
                logger.info(f"OTE PortalSession: {name} OK in {time.monotonic() - started:.0f}s")
            except Exception as e:
                failed.append(name)
                logger.error(f"OTE PortalSession: {name} FAILED: {e}", exc_info=True)
                take_screenshot(driver, f"{name}_failed")

    except SystemExit:
        raise
    except KeyboardInterrupt:
        failed.append("interrupted")
    except Exception as e:
        logger.error(f"OTE PortalSession: {e}", exc_info=True)
        if driver:
            take_screenshot(driver, "error")
        failed.append("session")
    finally:
        if driver:
            try:
                ote_portal.logout(driver)
                driver.quit()
            except Exception:
                pass

    if failed:
        logger.error(f"OTE PortalSession: finished with failures: {', '.join(failed)}")
        sys.exit(1)
    logger.info("OTE PortalSession: finished, all reports OK")


if __name__ == "__main__":
    main()
