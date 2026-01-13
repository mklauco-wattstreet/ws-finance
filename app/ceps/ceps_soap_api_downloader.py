#!/usr/bin/env python3
"""
CEPS SOAP API Downloader with Multi-Day Support

Downloads CEPS data via SOAP API with automatic chunking for large date ranges.
Splits requests > 30 days into 30-day chunks to avoid memory issues.

Usage:
    python3 ceps_soap_api_downloader.py --dataset imbalance --start-date 2025-11-01 --end-date 2025-12-31
    python3 ceps_soap_api_downloader.py --dataset all --start-date 2025-12-01 --end-date 2025-12-15
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
import requests
import xml.etree.ElementTree as ET

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import setup_logging


# SOAP Endpoint
SOAP_URL = "https://www.ceps.cz/_layouts/CepsData.asmx"

# Dataset configurations
DATASETS = {
    'imbalance': {
        'operation': 'AktualniSystemovaOdchylkaCR',
        'soap_action': 'https://www.ceps.cz/CepsData/AktualniSystemovaOdchylkaCR',
        'params': ['dateFrom', 'dateTo', 'agregation', 'function']
    },
    're_price': {
        'operation': 'AktualniCenaRE',
        'soap_action': 'https://www.ceps.cz/CepsData/AktualniCenaRE',
        'params': ['dateFrom', 'dateTo', 'param1']
    },
    'svr_activation': {
        'operation': 'AktivaceSVRvCR',
        'soap_action': 'https://www.ceps.cz/CepsData/AktivaceSVRvCR',
        'params': ['dateFrom', 'dateTo', 'agregation', 'function', 'param1']
    },
    'export_import_svr': {
        'operation': 'ExportImportSVR',
        'soap_action': 'https://www.ceps.cz/CepsData/ExportImportSVR',
        'params': ['dateFrom', 'dateTo', 'agregation', 'function', 'param1']
    }
}


def build_soap_envelope(operation: str, date_from: str, date_to: str, agregation: str = "MI",
                       function: str = "AVG", param1: str = "all") -> str:
    """
    Build SOAP 1.1 XML envelope for CEPS API request.

    Args:
        operation: SOAP operation name
        date_from: Start datetime (ISO format)
        date_to: End datetime (ISO format)
        agregation: Aggregation type (MI=minute, QH=quarter-hour, etc.)
        function: Aggregation function (AVG, SUM, etc.)
        param1: Additional parameter

    Returns:
        SOAP XML string
    """
    # Different operations have different parameters
    if operation == 'AktualniCenaRE':
        # RE Price only needs dateFrom, dateTo, param1
        body_content = f"""
        <{operation} xmlns="https://www.ceps.cz/CepsData/">
            <dateFrom>{date_from}</dateFrom>
            <dateTo>{date_to}</dateTo>
            <param1>{param1}</param1>
        </{operation}>
        """
    elif operation == 'AktualniSystemovaOdchylkaCR':
        # Imbalance needs dateFrom, dateTo, agregation, function
        body_content = f"""
        <{operation} xmlns="https://www.ceps.cz/CepsData/">
            <dateFrom>{date_from}</dateFrom>
            <dateTo>{date_to}</dateTo>
            <agregation>{agregation}</agregation>
            <function>{function}</function>
        </{operation}>
        """
    else:
        # SVR datasets need all parameters
        body_content = f"""
        <{operation} xmlns="https://www.ceps.cz/CepsData/">
            <dateFrom>{date_from}</dateFrom>
            <dateTo>{date_to}</dateTo>
            <agregation>{agregation}</agregation>
            <function>{function}</function>
            <param1>{param1}</param1>
        </{operation}>
        """

    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Body>
        {body_content}
    </soap:Body>
</soap:Envelope>"""

    return envelope


def download_soap_chunk(dataset: str, start_date: datetime, end_date: datetime, logger):
    """
    Download CEPS data via SOAP API for a single chunk.

    Args:
        dataset: Dataset key ('imbalance', 're_price', 'svr_activation', 'export_import_svr')
        start_date: Start datetime
        end_date: End datetime
        logger: Logger instance

    Returns:
        Tuple of (success: bool, xml_root: ET.Element, response_time: float)
    """
    if dataset not in DATASETS:
        logger.error(f"Unknown dataset: {dataset}")
        return False, None, 0

    config = DATASETS[dataset]
    operation = config['operation']
    soap_action = config['soap_action']

    # Format dates for SOAP (ISO 8601 format)
    date_from = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    date_to = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    logger.info(f"  Requesting: {start_date.date()} to {end_date.date()}")

    # Build SOAP envelope
    soap_envelope = build_soap_envelope(operation, date_from, date_to)

    # Prepare headers
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': soap_action,
        'User-Agent': 'CEPS-SOAP-Client/1.0'
    }

    try:
        import time
        start_time = time.time()

        # Make SOAP request
        response = requests.post(
            SOAP_URL,
            data=soap_envelope.encode('utf-8'),
            headers=headers,
            timeout=60
        )

        response_time = time.time() - start_time

        if response.status_code == 200:
            # Parse XML response
            root = ET.fromstring(response.content)

            # Find the result element
            result_element = root.find(f".//{{{config['soap_action'].rsplit('/', 1)[0]}/}}{operation}Result")

            if result_element is not None:
                logger.info(f"  ✓ Downloaded in {response_time:.2f}s ({len(response.content):,} bytes)")
                return True, result_element, response_time
            else:
                logger.error(f"  ✗ Result element not found in response")
                return False, None, response_time
        else:
            logger.error(f"  ✗ HTTP {response.status_code}: {response.text[:200]}")
            return False, None, response_time

    except requests.exceptions.Timeout:
        logger.error(f"  ✗ Request timeout after 60 seconds")
        return False, None, 0
    except requests.exceptions.ConnectionError as e:
        logger.error(f"  ✗ Connection error: {e}")
        return False, None, 0
    except Exception as e:
        logger.error(f"  ✗ Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False, None, 0


def download_soap_data(dataset: str, start_date: datetime, end_date: datetime, logger, max_chunk_days: int = None):
    """
    Download CEPS data via SOAP API with automatic chunking.

    Args:
        dataset: Dataset key
        start_date: Start datetime
        end_date: End datetime
        logger: Logger instance
        max_chunk_days: Maximum days per chunk (default varies by dataset)

    Returns:
        List of (success, xml_root, chunk_start, chunk_end) tuples
    """
    config = DATASETS[dataset]

    # Set default chunk size to 7 days for all datasets
    if max_chunk_days is None:
        max_chunk_days = 7

    logger.info("=" * 70)
    logger.info(f"CEPS SOAP API Downloader - {dataset.upper()}")
    logger.info("=" * 70)
    logger.info(f"Operation: {config['operation']}")
    logger.info(f"Date Range: {start_date.date()} to {end_date.date()}")

    # Calculate total days
    total_days = (end_date.date() - start_date.date()).days + 1
    logger.info(f"Total Days: {total_days}")

    # Determine if chunking is needed
    if total_days <= max_chunk_days:
        logger.info(f"Single request (≤ {max_chunk_days} days)")
        logger.info("")
        success, xml_root, response_time = download_soap_chunk(dataset, start_date, end_date, logger)
        return [(success, xml_root, start_date, end_date, response_time)]
    else:
        # Split into chunks
        num_chunks = (total_days + max_chunk_days - 1) // max_chunk_days
        logger.info(f"Splitting into {num_chunks} chunks ({max_chunk_days} days each)")
        logger.info("")

        results = []
        current_start = start_date
        chunk_num = 0

        while current_start <= end_date:
            chunk_num += 1
            # Calculate chunk end (max 30 days or remaining days)
            chunk_end = min(
                current_start + timedelta(days=max_chunk_days - 1),
                end_date
            )
            # Set to end of day
            chunk_end = chunk_end.replace(hour=23, minute=59, second=59)

            logger.info(f"Chunk {chunk_num}/{num_chunks}:")
            success, xml_root, response_time = download_soap_chunk(dataset, current_start, chunk_end, logger)
            results.append((success, xml_root, current_start, chunk_end, response_time))

            # Move to next chunk
            current_start = chunk_end + timedelta(seconds=1)
            current_start = current_start.replace(hour=0, minute=0, second=0)

            # Add small delay between chunks to avoid overwhelming the server
            if current_start <= end_date and chunk_num < num_chunks:
                import time
                time.sleep(2)  # 2 second delay between chunks

        return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='CEPS SOAP API Downloader with multi-day support',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download imbalance data for date range
  python3 ceps_soap_api_downloader.py --dataset imbalance --start-date 2025-11-01 --end-date 2025-11-30

  # Download all datasets for single day
  python3 ceps_soap_api_downloader.py --dataset all --start-date 2026-01-09

  # Download with debug logging
  python3 ceps_soap_api_downloader.py --dataset re_price --start-date 2025-12-01 --end-date 2025-12-31 --debug
        """
    )

    parser.add_argument(
        '--dataset',
        type=str,
        required=True,
        choices=['imbalance', 're_price', 'svr_activation', 'export_import_svr', 'all'],
        help='Dataset to download (or "all" for all datasets)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default=None,
        help='Start date (YYYY-MM-DD). Defaults to today.'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='End date (YYYY-MM-DD). Defaults to start-date.'
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
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            logger.error(f"Invalid start-date format: {args.start_date}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        start_date = datetime.combine(today, datetime.min.time())

    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        except ValueError:
            logger.error(f"Invalid end-date format: {args.end_date}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        end_date = start_date

    # Set time to cover full day
    start_date = start_date.replace(hour=0, minute=0, second=0)
    end_date = end_date.replace(hour=23, minute=59, second=59)

    # Determine which datasets to download
    if args.dataset == 'all':
        datasets_to_download = ['imbalance', 're_price', 'svr_activation', 'export_import_svr']
    else:
        datasets_to_download = [args.dataset]

    # Download each dataset
    all_results = {}
    for dataset in datasets_to_download:
        results = download_soap_data(dataset, start_date, end_date, logger)
        all_results[dataset] = results
        logger.info("")

    # Summary
    logger.info("=" * 70)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 70)

    total_success = 0
    total_failed = 0

    for dataset, results in all_results.items():
        success_count = sum(1 for r in results if r[0])
        failed_count = sum(1 for r in results if not r[0])
        total_success += success_count
        total_failed += failed_count

        status = "✓" if failed_count == 0 else "⚠"
        logger.info(f"{status} {dataset}: {success_count}/{len(results)} chunks successful")

    logger.info("")
    logger.info(f"Total: {total_success} successful, {total_failed} failed")
    logger.info("=" * 70)

    if total_failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
