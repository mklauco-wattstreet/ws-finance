#!/usr/bin/env python3
"""
CEPS SOAP API Downloader - Lightweight Test Script

Tests direct SOAP API access to CEPS data without Selenium.
Downloads raw XML response for one dataset to test cache behavior.

Usage:
    python3 ceps_soap_downloader.py --dataset imbalance --start-date 2026-01-09
    python3 ceps_soap_downloader.py --dataset re_price --start-date 2026-01-09
    python3 ceps_soap_downloader.py --dataset svr_activation --start-date 2026-01-09
    python3 ceps_soap_downloader.py --dataset export_import_svr --start-date 2026-01-09
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, date
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


def download_soap_data(dataset: str, start_date: datetime, end_date: datetime, logger):
    """
    Download CEPS data via SOAP API.

    Args:
        dataset: Dataset key ('imbalance', 're_price', 'svr_activation', 'export_import_svr')
        start_date: Start datetime
        end_date: End datetime
        logger: Logger instance

    Returns:
        Tuple of (success: bool, xml_content: str, response_time: float)
    """
    if dataset not in DATASETS:
        logger.error(f"Unknown dataset: {dataset}")
        return False, None, 0

    config = DATASETS[dataset]
    operation = config['operation']
    soap_action = config['soap_action']

    logger.info("=" * 70)
    logger.info("CEPS SOAP API Downloader")
    logger.info("=" * 70)
    logger.info(f"Dataset: {dataset}")
    logger.info(f"Operation: {operation}")
    logger.info(f"Date Range: {start_date.date()} to {end_date.date()}")
    logger.info("")

    # Format dates for SOAP (ISO 8601 format)
    date_from = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    date_to = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    logger.info(f"Building SOAP envelope...")
    logger.info(f"  dateFrom: {date_from}")
    logger.info(f"  dateTo: {date_to}")

    # Build SOAP envelope
    soap_envelope = build_soap_envelope(operation, date_from, date_to)

    # Prepare headers
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': soap_action,
        'User-Agent': 'CEPS-SOAP-Client/1.0'
    }

    logger.info("")
    logger.info(f"Sending SOAP request to {SOAP_URL}...")
    logger.info(f"SOAPAction: {soap_action}")

    try:
        import time
        start_time = time.time()

        # Make SOAP request
        response = requests.post(
            SOAP_URL,
            data=soap_envelope.encode('utf-8'),
            headers=headers,
            timeout=30
        )

        response_time = time.time() - start_time

        logger.info("")
        logger.info(f"Response received in {response_time:.2f}s")
        logger.info(f"Status Code: {response.status_code}")
        logger.info(f"Content-Type: {response.headers.get('Content-Type', 'N/A')}")
        logger.info(f"Content-Length: {len(response.content)} bytes")

        if response.status_code == 200:
            logger.info("✓ SOAP request successful")

            # Try to parse response
            try:
                root = ET.fromstring(response.content)
                logger.info("")
                logger.info("Response structure:")

                # Navigate SOAP response
                # Find the result element (varies by operation)
                result_element = root.find(f".//{{{config['soap_action'].rsplit('/', 1)[0]}/}}{operation}Result")

                if result_element is not None:
                    logger.info(f"  Found {operation}Result element")

                    # Convert XML subtree to string
                    xml_content = ET.tostring(result_element, encoding='unicode')
                    logger.info(f"  Data length: {len(xml_content)} characters")

                    # Show first 500 chars of data
                    preview = xml_content[:500]
                    logger.info("")
                    logger.info("Data preview (first 500 chars):")
                    logger.info("-" * 70)
                    logger.info(preview)
                    logger.info("-" * 70)

                    return True, xml_content, response_time
                else:
                    logger.warning("⚠ Result element not found or empty")
                    logger.info("")
                    logger.info("Full response (first 1000 chars):")
                    logger.info(response.text[:1000])
                    return False, response.text, response_time

            except ET.ParseError as e:
                logger.error(f"✗ Failed to parse XML response: {e}")
                logger.info("")
                logger.info("Raw response (first 1000 chars):")
                logger.info(response.text[:1000])
                return False, response.text, response_time
        else:
            logger.error(f"✗ SOAP request failed with status {response.status_code}")
            logger.error(f"Response: {response.text[:1000]}")
            return False, None, response_time

    except requests.exceptions.Timeout:
        logger.error("✗ Request timeout after 30 seconds")
        return False, None, 0
    except requests.exceptions.ConnectionError as e:
        logger.error(f"✗ Connection error: {e}")
        return False, None, 0
    except Exception as e:
        logger.error(f"✗ Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False, None, 0


def save_response(dataset: str, start_date: datetime, xml_content: str, logger):
    """
    Save XML response to file.

    Args:
        dataset: Dataset key
        start_date: Start date for filename
        xml_content: XML content to save
        logger: Logger instance

    Returns:
        Path to saved file
    """
    # Create output directory
    output_dir = Path(f"/app/downloads/ceps/soap/{start_date.year}/{start_date.month:02d}")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{dataset}_{start_date.strftime('%Y%m%d')}_{timestamp}.xml"
    output_path = output_dir / filename

    # Save file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(xml_content)

    logger.info("")
    logger.info(f"✓ Saved response to: {output_path}")
    logger.info(f"  File size: {output_path.stat().st_size} bytes")

    return output_path


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='CEPS SOAP API Downloader - Lightweight test script',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download imbalance data for today
  python3 ceps_soap_downloader.py --dataset imbalance

  # Download RE prices for specific date
  python3 ceps_soap_downloader.py --dataset re_price --start-date 2026-01-09

  # Download SVR activation with debug logging
  python3 ceps_soap_downloader.py --dataset svr_activation --start-date 2026-01-09 --debug

  # Test cache behavior (run twice quickly)
  python3 ceps_soap_downloader.py --dataset imbalance --start-date 2025-11-13
  python3 ceps_soap_downloader.py --dataset imbalance --start-date 2025-11-13
        """
    )

    parser.add_argument(
        '--dataset',
        type=str,
        required=True,
        choices=['imbalance', 're_price', 'svr_activation', 'export_import_svr'],
        help='Dataset to download'
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
        '--save',
        action='store_true',
        help='Save XML response to file'
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

    # Download data
    success, xml_content, response_time = download_soap_data(
        args.dataset,
        start_date,
        end_date,
        logger
    )

    if success and args.save and xml_content:
        save_response(args.dataset, start_date, xml_content, logger)

    logger.info("")
    logger.info("=" * 70)
    if success:
        logger.info("✓ SOAP API test completed successfully")
        logger.info(f"  Response time: {response_time:.2f}s")
        logger.info("=" * 70)
        sys.exit(0)
    else:
        logger.info("✗ SOAP API test failed")
        logger.info("=" * 70)
        sys.exit(1)


if __name__ == "__main__":
    main()
