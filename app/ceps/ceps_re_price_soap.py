#!/usr/bin/env python3
"""
CEPS RE Price Downloader - SOAP API
Downloads CEPS Actual Reserve Energy (RE) pricing data using official SOAP API.

API Endpoint: https://www.ceps.cz/_layouts/CepsData.asmx
Method: AktualniCenaRE

This is the official API - much more reliable than web scraping!
"""

import sys
import csv
from pathlib import Path
from datetime import datetime
from io import StringIO
import requests
from xml.etree import ElementTree as ET

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import setup_logging


def call_soap_api(method_name, date_from, date_to, logger, **kwargs):
    """
    Call CEPS SOAP API.

    Args:
        method_name: API method name (e.g., "AktualniCenaRE")
        date_from: Start datetime
        date_to: End datetime
        logger: Logger instance
        **kwargs: Additional parameters (agregation, function, param1, etc.)

    Returns:
        XML response as ElementTree, or None if failed
    """
    url = "https://www.ceps.cz/_layouts/CepsData.asmx"

    # Format dates - try standard datetime format without T separator
    # SQL Server might not like ISO 8601
    date_from_str = date_from.strftime("%Y-%m-%d %H:%M:%S")
    date_to_str = date_to.strftime("%Y-%m-%d %H:%M:%S")

    # Build SOAP envelope
    soap_body_params = f"""
        <dateFrom>{date_from_str}</dateFrom>
        <dateTo>{date_to_str}</dateTo>
    """

    # Add optional parameters
    for key, value in kwargs.items():
        if value is not None:
            soap_body_params += f"\n        <{key}>{value}</{key}>"

    soap_envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <{method_name} xmlns="http://www.ceps.cz/CepsData">{soap_body_params}
    </{method_name}>
  </soap:Body>
</soap:Envelope>"""

    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': f'"https://www.ceps.cz/CepsData/{method_name}"',
    }

    logger.info(f"Calling SOAP API: {method_name}")
    logger.info(f"  Date from: {date_from_str}")
    logger.info(f"  Date to: {date_to_str}")
    if kwargs:
        logger.info(f"  Additional params: {kwargs}")

    try:
        response = requests.post(url, data=soap_envelope, headers=headers, timeout=30)

        logger.info(f"  Response status: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"✗ HTTP error: {response.status_code}")
            logger.error(f"  Response: {response.text[:500]}")
            return None

        # Parse XML response
        root = ET.fromstring(response.content)

        # Extract the result element
        # Namespace handling
        ns = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ceps': 'http://www.ceps.cz/CepsData'
        }

        result_element = root.find(f'.//ceps:{method_name}Result', ns)
        if result_element is None:
            logger.error("✗ Could not find result element in SOAP response")
            logger.error(f"  Response: {response.text[:500]}")
            return None

        logger.info("✓ SOAP API call successful")
        return result_element

    except requests.exceptions.Timeout:
        logger.error("✗ Request timeout")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Request failed: {e}")
        return None
    except ET.ParseError as e:
        logger.error(f"✗ XML parse error: {e}")
        return None


def download_ceps_re_price_soap(start_date, end_date, logger, param1="All"):
    """
    Download CEPS RE price data using official SOAP API.

    Args:
        start_date: Start datetime
        end_date: End datetime
        logger: Logger instance
        param1: Type of DT (All | aFRR | mFFR+ | mFRR- | mFRR5), default: All

    Returns:
        Path to downloaded file, or None if failed
    """
    data_tag = "AktualniCenaRE"

    logger.info("=" * 60)
    logger.info("CEPS RE Price Downloader (SOAP API)")
    logger.info(f"Tag: {data_tag}")
    logger.info(f"Date: {start_date.date()} to {end_date.date()}")
    logger.info(f"Param1 (Type DT): {param1}")
    logger.info("=" * 60)

    # Call SOAP API with param1
    result = call_soap_api(data_tag, start_date, end_date, logger, param1=param1)

    if result is None:
        return None

    # The result contains CSV data as text
    csv_data = result.text

    if not csv_data or len(csv_data) < 100:
        logger.error("✗ No data in API response")
        return None

    logger.info(f"✓ Received {len(csv_data)} bytes of data")

    # Save to file
    dest_dir = Path(f"/app/scripts/ceps/{start_date.year}/{start_date.month:02d}")
    dest_dir.mkdir(parents=True, exist_ok=True)

    if start_date.date() == end_date.date():
        date_str = start_date.strftime("%Y%m%d")
    else:
        date_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

    timestamp = datetime.now().strftime("%H%M%S")
    dest_file = dest_dir / f"data_{data_tag}_{date_str}_{timestamp}.csv"

    with open(dest_file, 'w', encoding='utf-8') as f:
        f.write(csv_data)

    logger.info(f"✓ Saved to: {dest_file}")

    # Verify file content
    try:
        with open(dest_file, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            second_line = f.readline().strip()
            logger.info(f"  Header: {first_line[:100]}")
            logger.info(f"  Metadata: {second_line[:100]}")

            # Check if it contains expected date
            date_check = start_date.strftime("%d.%m.%Y")
            if date_check in second_line:
                logger.info(f"✓ Verified: File contains expected date {date_check}")
                logger.info("=" * 60)
                logger.info("✓ SUCCESS")
                logger.info("=" * 60)
                return dest_file
            else:
                logger.error("=" * 60)
                logger.error(f"✗ VALIDATION FAILED: Expected date {date_check} not found in CSV")
                logger.error(f"  Requested: {start_date.date()} to {end_date.date()}")
                logger.error(f"  Metadata: {second_line[:200]}")
                logger.error("  The API returned wrong data!")
                logger.error("=" * 60)
                # Delete the wrong file
                dest_file.unlink()
                logger.info(f"  Deleted wrong file: {dest_file}")
                return None
    except Exception as e:
        logger.error(f"✗ Error reading file: {e}")
        return None


def main():
    """Main entry point."""
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description='CEPS RE Price SOAP API Downloader')
    parser.add_argument('--start-date', type=str, default=None,
                       help='Start date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--end-date', type=str, default=None,
                       help='End date in YYYY-MM-DD format (default: same as start-date)')
    parser.add_argument('--param1', type=str, default='All',
                       choices=['All', 'aFRR', 'mFFR+', 'mFRR-', 'mFRR5'],
                       help='Type of DT (default: All)')
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
            end_date = start_date

        # Set time to cover full day
        start_date = start_date.replace(hour=0, minute=0, second=0)
        end_date = end_date.replace(hour=23, minute=59, second=59)

    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        logger.error("Date format must be YYYY-MM-DD (e.g., 2026-01-04)")
        sys.exit(1)

    exit_code = 0

    try:
        # Download data
        downloaded_file = download_ceps_re_price_soap(start_date, end_date, logger, param1=args.param1)

        if not downloaded_file:
            exit_code = 1

    except KeyboardInterrupt:
        logger.info("\nInterrupted")
        exit_code = 130
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
