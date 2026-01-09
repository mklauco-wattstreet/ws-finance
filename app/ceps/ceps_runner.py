#!/usr/bin/env python3
"""
CEPS Runner - Download and Upload Pipeline

Downloads CEPS data from CEPS website and immediately uploads to PostgreSQL database:
1. Actual RE Prices (AktualniCenaRE)
2. SVR Activation (AktivaceSVRvCR)
3. Actual System Imbalance (AktualniSystemovaOdchylkaCR)

Download order optimized for CEPS caching (RE → SVR → Imbalance with 65s delays).

IMPORTANT: Session is reset between downloads to avoid breaking OTE CSV downloads.

Usage:
    # Download and upload today's data
    python3 ceps_runner.py

    # Download and upload specific date range
    python3 ceps_runner.py --start-date 2026-01-01 --end-date 2026-01-05

    # Download only specific dataset
    python3 ceps_runner.py --dataset re_price
    python3 ceps_runner.py --dataset svr_activation
    python3 ceps_runner.py --dataset imbalance

    # Debug mode
    python3 ceps_runner.py --debug
"""

import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, date

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import setup_logging
from ceps.ceps_hybrid_downloader import download_ceps_hybrid, init_browser
from ceps.ceps_uploader import process_csv_file, get_db_connection
from ceps.ceps_re_price_downloader import download_ceps_re_price
from ceps.ceps_re_price_uploader import process_csv_file as process_re_price_csv_file
from ceps.ceps_svr_activation_downloader import download_ceps_svr_activation
from ceps.ceps_svr_activation_uploader import process_csv_file as process_svr_activation_csv_file


def run_download_and_upload(start_date: date, end_date: date, dataset: str, logger):
    """
    Download CEPS data for date range and immediately upload to database.

    NOTE: For date ranges > 1 day, downloads day-by-day because the CEPS website
    is unreliable for multi-day downloads (returns wrong datasets).

    Args:
        start_date: Start date
        end_date: End date
        dataset: Which dataset to download ('re_price', 'svr_activation', 'imbalance', or 'all')
        logger: Logger instance

    Returns:
        True if successful, False otherwise
    """
    logger.info("=" * 80)
    logger.info("CEPS RUNNER - Download and Upload Pipeline")
    logger.info("=" * 80)
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Dataset: {dataset}")

    # Calculate number of days
    num_days = (end_date - start_date).days + 1
    if num_days > 1:
        logger.info(f"Multi-day range detected: {num_days} days")
        logger.info("Will download day-by-day for reliability")
    logger.info("")

    # For multi-day ranges, process day-by-day
    if num_days > 1:
        overall_success = True
        from datetime import timedelta

        current_date = start_date
        day_count = 0
        while current_date <= end_date:
            day_count += 1
            logger.info(f"Processing day {current_date} ({day_count}/{num_days})...")
            success = run_download_and_upload_single_day(current_date, dataset, logger)
            if not success:
                logger.error(f"Failed to process {current_date}")
                overall_success = False

            current_date += timedelta(days=1)

            # Add 125-second delay between days (not after last day)
            if current_date <= end_date:
                logger.info("")
                logger.info("=" * 80)
                logger.info("Waiting 125 seconds before next day (cache expiry)...")
                logger.info("=" * 80)

                # Simple countdown progress
                for remaining in range(125, 0, -1):
                    print(f"\rTime remaining: {remaining:3d} seconds... ", end='', flush=True)
                    time.sleep(1)
                print()  # New line after countdown

                logger.info("")
            else:
                logger.info("")

        return overall_success

    # Single day - use original logic
    return run_download_and_upload_single_day(start_date, dataset, logger)


def run_download_and_upload_single_day(target_date: date, dataset: str, logger):
    """
    Download CEPS data for a single day and upload to database.

    Downloads in order: RE Price → SVR Activation → Imbalance (65s delay between each)

    Args:
        target_date: Date to download
        dataset: Which dataset to download ('re_price', 'svr_activation', 'imbalance', or 'all')
        logger: Logger instance

    Returns:
        True if successful, False otherwise
    """
    # Convert date to datetime objects (naive, no timezone)
    # Data is already in Europe/Prague local time
    start_dt = datetime.combine(target_date, datetime.min.time())
    end_dt = datetime.combine(target_date, datetime.max.time())

    driver = None
    conn = None
    overall_success = True

    # Track results
    results = {
        're_price': {'success': False, 'records_1min': 0, 'intervals_15min': 0, 'csv_path': None},
        'svr_activation': {'success': False, 'records_1min': 0, 'intervals_15min': 0, 'csv_path': None},
        'imbalance': {'success': False, 'records_1min': 0, 'intervals_15min': 0, 'csv_path': None}
    }

    try:
        # Connect to database once for all uploads
        conn = get_db_connection()
        logger.info("✓ Connected to database")
        logger.info("")

        # ====================================================================
        # DATASET 1: RE Prices (AktualniCenaRE) - FIRST
        # ====================================================================
        if dataset in ['re_price', 'all']:
            logger.info("=" * 80)
            logger.info("DATASET 1: RE PRICES (AktualniCenaRE)")
            logger.info("=" * 80)

            try:
                # Step 1.1: Download RE price CSV
                logger.info("STEP 1.1: DOWNLOADING RE PRICE DATA")
                logger.info("-" * 80)

                driver = init_browser()
                re_price_csv = download_ceps_re_price(
                    driver=driver,
                    start_date=start_dt,
                    end_date=end_dt,
                    logger=logger
                )

                if re_price_csv and re_price_csv.exists():
                    logger.info(f"✓ RE price download successful: {re_price_csv}")
                    results['re_price']['csv_path'] = re_price_csv

                    # Step 1.2: Upload RE price to database
                    logger.info("")
                    logger.info("STEP 1.2: UPLOADING RE PRICE TO DATABASE")
                    logger.info("-" * 80)

                    records_1min, intervals_15min = process_re_price_csv_file(re_price_csv, conn, logger)
                    results['re_price']['records_1min'] = records_1min
                    results['re_price']['intervals_15min'] = intervals_15min

                    if records_1min > 0:
                        results['re_price']['success'] = True
                        logger.info("✓ RE price data uploaded successfully")
                    else:
                        logger.warning("⚠ No RE price records uploaded")
                        overall_success = False
                else:
                    logger.error("✗ RE price download failed")
                    overall_success = False

            except Exception as e:
                logger.error(f"✗ RE price pipeline failed: {e}")
                overall_success = False
                if logger.level == 10:  # DEBUG level
                    import traceback
                    logger.error(traceback.format_exc())

            finally:
                # Clean up browser
                if driver:
                    try:
                        driver.quit()
                        driver = None
                        logger.info("✓ Browser closed")
                    except Exception as e:
                        logger.warning(f"Warning: Failed to close browser: {e}")

                # 65 second delay before next download
                if dataset == 'all':
                    logger.info("")
                    logger.info("Waiting 65 seconds before next download...")
                    time.sleep(65)

        # ====================================================================
        # DATASET 2: SVR Activation (AktivaceSVRvCR) - SECOND
        # ====================================================================
        if dataset in ['svr_activation', 'all']:
            logger.info("")
            logger.info("=" * 80)
            logger.info("DATASET 2: SVR ACTIVATION (AktivaceSVRvCR)")
            logger.info("=" * 80)

            try:
                # Step 2.1: Download SVR activation CSV
                logger.info("STEP 2.1: DOWNLOADING SVR ACTIVATION DATA")
                logger.info("-" * 80)

                driver = init_browser()
                svr_csv = download_ceps_svr_activation(
                    driver=driver,
                    start_date=start_dt,
                    end_date=end_dt,
                    logger=logger
                )

                if svr_csv and svr_csv.exists():
                    logger.info(f"✓ SVR activation download successful: {svr_csv}")
                    results['svr_activation']['csv_path'] = svr_csv

                    # Step 2.2: Upload SVR activation to database
                    logger.info("")
                    logger.info("STEP 2.2: UPLOADING SVR ACTIVATION TO DATABASE")
                    logger.info("-" * 80)

                    records_1min, intervals_15min = process_svr_activation_csv_file(svr_csv, conn, logger)
                    results['svr_activation']['records_1min'] = records_1min
                    results['svr_activation']['intervals_15min'] = intervals_15min

                    if records_1min > 0:
                        results['svr_activation']['success'] = True
                        logger.info("✓ SVR activation data uploaded successfully")
                    else:
                        logger.warning("⚠ No SVR activation records uploaded")
                        overall_success = False
                else:
                    logger.error("✗ SVR activation download failed")
                    overall_success = False

            except Exception as e:
                logger.error(f"✗ SVR activation pipeline failed: {e}")
                overall_success = False
                if logger.level == 10:  # DEBUG level
                    import traceback
                    logger.error(traceback.format_exc())

            finally:
                # Clean up browser
                if driver:
                    try:
                        driver.quit()
                        driver = None
                        logger.info("✓ Browser closed")
                    except Exception as e:
                        logger.warning(f"Warning: Failed to close browser: {e}")

                # 65 second delay before next download
                if dataset == 'all':
                    logger.info("")
                    logger.info("Waiting 65 seconds before next download...")
                    time.sleep(65)

        # ====================================================================
        # DATASET 3: System Imbalance (AktualniSystemovaOdchylkaCR) - THIRD
        # ====================================================================
        if dataset in ['imbalance', 'all']:
            logger.info("")
            logger.info("=" * 80)
            logger.info("DATASET 3: SYSTEM IMBALANCE (AktualniSystemovaOdchylkaCR)")
            logger.info("=" * 80)

            try:
                # Step 3.1: Download imbalance CSV
                logger.info("STEP 3.1: DOWNLOADING IMBALANCE DATA")
                logger.info("-" * 80)

                driver = init_browser()
                imbalance_csv = download_ceps_hybrid(
                    driver=driver,
                    data_tag='AktualniSystemovaOdchylkaCR',
                    start_date=start_dt,
                    end_date=end_dt,
                    logger=logger
                )

                if imbalance_csv and imbalance_csv.exists():
                    logger.info(f"✓ Imbalance download successful: {imbalance_csv}")
                    results['imbalance']['csv_path'] = imbalance_csv

                    # Step 3.2: Upload imbalance to database
                    logger.info("")
                    logger.info("STEP 3.2: UPLOADING IMBALANCE TO DATABASE")
                    logger.info("-" * 80)

                    records_1min, intervals_15min = process_csv_file(imbalance_csv, conn, logger)
                    results['imbalance']['records_1min'] = records_1min
                    results['imbalance']['intervals_15min'] = intervals_15min

                    if records_1min > 0:
                        results['imbalance']['success'] = True
                        logger.info("✓ Imbalance data uploaded successfully")
                    else:
                        logger.warning("⚠ No imbalance records uploaded")
                        overall_success = False
                else:
                    logger.error("✗ Imbalance download failed")
                    overall_success = False

            except Exception as e:
                logger.error(f"✗ Imbalance pipeline failed: {e}")
                overall_success = False
                if logger.level == 10:  # DEBUG level
                    import traceback
                    logger.error(traceback.format_exc())

            finally:
                # Clean up browser
                if driver:
                    try:
                        driver.quit()
                        driver = None
                        logger.info("✓ Browser closed")
                    except Exception as e:
                        logger.warning(f"Warning: Failed to close browser: {e}")

        # ====================================================================
        # SUMMARY
        # ====================================================================
        logger.info("")
        logger.info("=" * 80)
        if overall_success:
            logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY")
        else:
            logger.info("⚠ PIPELINE COMPLETED WITH ERRORS")
        logger.info("=" * 80)
        logger.info(f"Date: {target_date}")
        logger.info("")

        if dataset in ['re_price', 'all'] and results['re_price']['success']:
            logger.info("RE Prices:")
            logger.info(f"  CSV: {results['re_price']['csv_path'].name}")
            logger.info(f"  1min records: {results['re_price']['records_1min']:,}")
            logger.info(f"  15min intervals: {results['re_price']['intervals_15min']:,}")

        if dataset in ['svr_activation', 'all'] and results['svr_activation']['success']:
            logger.info("SVR Activation:")
            logger.info(f"  CSV: {results['svr_activation']['csv_path'].name}")
            logger.info(f"  1min records: {results['svr_activation']['records_1min']:,}")
            logger.info(f"  15min intervals: {results['svr_activation']['intervals_15min']:,}")

        if dataset in ['imbalance', 'all'] and results['imbalance']['success']:
            logger.info("System Imbalance:")
            logger.info(f"  CSV: {results['imbalance']['csv_path'].name}")
            logger.info(f"  1min records: {results['imbalance']['records_1min']:,}")
            logger.info(f"  15min intervals: {results['imbalance']['intervals_15min']:,}")

        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"✗ Pipeline failed: {e}")
        overall_success = False
        if logger.level == 10:  # DEBUG level
            import traceback
            logger.error(traceback.format_exc())

    finally:
        # Final cleanup
        if driver:
            try:
                driver.quit()
                logger.info("✓ Browser closed")
            except Exception as e:
                logger.warning(f"Warning: Failed to close browser: {e}")

        if conn:
            try:
                conn.close()
                logger.info("✓ Database connection closed")
            except Exception as e:
                logger.warning(f"Warning: Failed to close database connection: {e}")

    return overall_success


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Download CEPS data (RE prices, SVR activation, imbalance) and upload to PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download and upload today's data for all datasets (for cron jobs)
  python3 ceps_runner.py

  # Download and upload specific date for all datasets
  python3 ceps_runner.py --start-date 2026-01-01 --end-date 2026-01-01

  # Download and upload date range for all datasets
  python3 ceps_runner.py --start-date 2026-01-01 --end-date 2026-01-05

  # Download only RE price data
  python3 ceps_runner.py --dataset re_price

  # Download only SVR activation data
  python3 ceps_runner.py --dataset svr_activation

  # Download only imbalance data
  python3 ceps_runner.py --dataset imbalance

  # With debug logging
  python3 ceps_runner.py --debug
        """
    )

    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date (YYYY-MM-DD). Defaults to today.'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date (YYYY-MM-DD). Defaults to start-date.'
    )
    parser.add_argument(
        '--dataset',
        type=str,
        choices=['re_price', 'svr_activation', 'imbalance', 'all'],
        default='all',
        help='Which dataset to download (re_price, svr_activation, imbalance, or all). Defaults to all.'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(debug=args.debug)

    # Parse dates
    today = date.today()

    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid start-date format: {args.start_date}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        start_date = today

    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid end-date format: {args.end_date}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        end_date = start_date

    # Validate date range
    if end_date < start_date:
        logger.error("end-date cannot be before start-date")
        sys.exit(1)

    # Run pipeline
    success = run_download_and_upload(start_date, end_date, args.dataset, logger)

    if success:
        logger.info("✓ Pipeline completed successfully")
        sys.exit(0)
    else:
        logger.error("✗ Pipeline failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
