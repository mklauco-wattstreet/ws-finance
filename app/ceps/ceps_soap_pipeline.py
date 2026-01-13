#!/usr/bin/env python3
"""
CEPS SOAP API Pipeline

Complete pipeline: Download via SOAP API -> Parse XML -> Upload to DB
Downloads XML to disk, then processes immediately (chunk by chunk).
Uses UPSERT logic for safe backfills.

Usage:
    # Single dataset, single day
    python3 ceps_soap_pipeline.py --dataset imbalance --start-date 2026-01-09

    # All datasets, date range
    python3 ceps_soap_pipeline.py --dataset all --start-date 2025-12-01 --end-date 2025-12-31

    # Large backfill (auto-chunks to 7-day requests)
    python3 ceps_soap_pipeline.py --dataset all --start-date 2024-12-01 --end-date 2025-12-31
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
import psycopg2
import requests
import xml.etree.ElementTree as ET
import time

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import setup_logging
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT
from ceps.ceps_soap_xml_parser import parse_soap_xml
from ceps.ceps_soap_uploader import upsert_data

# SOAP Endpoint
SOAP_URL = "https://www.ceps.cz/_layouts/CepsData.asmx"

# Dataset configurations
DATASETS = {
    'imbalance': {
        'operation': 'AktualniSystemovaOdchylkaCR',
        'soap_action': 'https://www.ceps.cz/CepsData/AktualniSystemovaOdchylkaCR',
    },
    're_price': {
        'operation': 'AktualniCenaRE',
        'soap_action': 'https://www.ceps.cz/CepsData/AktualniCenaRE',
    },
    'svr_activation': {
        'operation': 'AktivaceSVRvCR',
        'soap_action': 'https://www.ceps.cz/CepsData/AktivaceSVRvCR',
    },
    'export_import_svr': {
        'operation': 'ExportImportSVR',
        'soap_action': 'https://www.ceps.cz/CepsData/ExportImportSVR',
    }
}


def build_soap_envelope(operation: str, date_from: str, date_to: str) -> str:
    """Build SOAP envelope for CEPS API request."""
    if operation == 'AktualniCenaRE':
        body_content = f"""
        <{operation} xmlns="https://www.ceps.cz/CepsData/">
            <dateFrom>{date_from}</dateFrom>
            <dateTo>{date_to}</dateTo>
            <param1>all</param1>
        </{operation}>
        """
    elif operation == 'AktualniSystemovaOdchylkaCR':
        body_content = f"""
        <{operation} xmlns="https://www.ceps.cz/CepsData/">
            <dateFrom>{date_from}</dateFrom>
            <dateTo>{date_to}</dateTo>
            <agregation>MI</agregation>
            <function>AVG</function>
        </{operation}>
        """
    else:
        body_content = f"""
        <{operation} xmlns="https://www.ceps.cz/CepsData/">
            <dateFrom>{date_from}</dateFrom>
            <dateTo>{date_to}</dateTo>
            <agregation>MI</agregation>
            <function>AVG</function>
            <param1>all</param1>
        </{operation}>
        """

    return f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Body>
        {body_content}
    </soap:Body>
</soap:Envelope>"""


def download_chunk_to_disk(dataset: str, start_date: datetime, end_date: datetime, logger):
    """
    Download one chunk via SOAP API and save to disk.

    Returns:
        Path to saved XML file or None if failed
    """
    config = DATASETS[dataset]
    operation = config['operation']
    soap_action = config['soap_action']

    # Format dates
    date_from = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    date_to = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    # Build SOAP envelope
    soap_envelope = build_soap_envelope(operation, date_from, date_to)

    # Prepare headers
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': soap_action,
        'User-Agent': 'CEPS-SOAP-Client/1.0'
    }

    try:
        start_time = time.time()

        # Make SOAP request
        response = requests.post(
            SOAP_URL,
            data=soap_envelope.encode('utf-8'),
            headers=headers,
            timeout=60
        )

        response_time = time.time() - start_time

        if response.status_code != 200:
            logger.error(f"  ✗ HTTP {response.status_code}")
            return None

        # Parse to verify structure
        root = ET.fromstring(response.content)
        result_element = root.find(f".//{{{config['soap_action'].rsplit('/', 1)[0]}/}}{operation}Result")

        if result_element is None:
            logger.error(f"  ✗ Result element not found in response")
            return None

        # Save to disk
        output_dir = Path(f"/app/downloads/ceps/soap/{start_date.year}/{start_date.month:02d}")
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{dataset}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{timestamp}.xml"
        output_path = output_dir / filename

        # Save entire response
        with open(output_path, 'wb') as f:
            f.write(response.content)

        logger.info(f"  ✓ Downloaded in {response_time:.2f}s → {output_path.name}")

        return output_path

    except Exception as e:
        logger.error(f"  ✗ Download failed: {e}")
        return None


def process_dataset(dataset: str, start_date: datetime, end_date: datetime, conn, logger):
    """
    Process one dataset: download chunks, parse, upload immediately.
    """
    logger.info("=" * 70)
    logger.info(f"PROCESSING DATASET: {dataset.upper()}")
    logger.info("=" * 70)
    logger.info(f"Date Range: {start_date.date()} to {end_date.date()}")

    # Calculate chunks (7 days each)
    chunk_days = 7
    total_days = (end_date.date() - start_date.date()).days + 1
    num_chunks = (total_days + chunk_days - 1) // chunk_days

    logger.info(f"Chunks: {num_chunks} ({chunk_days} days each)")
    logger.info("")

    total_uploaded = 0
    failed_chunks = 0

    current_start = start_date
    chunk_num = 0

    while current_start <= end_date:
        chunk_num += 1

        # Calculate chunk end
        chunk_end = min(
            current_start + timedelta(days=chunk_days - 1),
            end_date
        )
        chunk_end = chunk_end.replace(hour=23, minute=59, second=59)

        logger.info(f"Chunk {chunk_num}/{num_chunks}: {current_start.date()} to {chunk_end.date()}")

        # Step 1: Download to disk
        xml_path = download_chunk_to_disk(dataset, current_start, chunk_end, logger)

        if xml_path is None:
            failed_chunks += 1
            logger.error(f"  ✗ Chunk failed")
        else:
            # Step 2: Parse XML from disk
            try:
                root = ET.parse(xml_path).getroot()
                config = DATASETS[dataset]
                operation = config['operation']

                # Find result element
                result_element = root.find(f".//{{{config['soap_action'].rsplit('/', 1)[0]}/}}{operation}Result")

                if result_element is None:
                    logger.error(f"  ✗ Could not parse XML")
                    failed_chunks += 1
                else:
                    records = parse_soap_xml(dataset, result_element)
                    logger.info(f"  Parsed {len(records):,} records")

                    # Step 3: Upload immediately
                    if records:
                        try:
                            # Deduplicate records (keep last occurrence for each timestamp)
                            seen = {}
                            for record in records:
                                seen[record['delivery_timestamp']] = record
                            deduped_records = list(seen.values())

                            if len(deduped_records) < len(records):
                                logger.info(f"  Deduplicated: {len(records):,} → {len(deduped_records):,} records")

                            uploaded = upsert_data(dataset, deduped_records, conn, logger)
                            total_uploaded += uploaded
                        except Exception as e:
                            logger.error(f"  ✗ Upload failed: {e}")
                            failed_chunks += 1
                            conn.rollback()  # Rollback transaction

            except Exception as e:
                logger.error(f"  ✗ Parse failed: {e}")
                failed_chunks += 1

        logger.info("")

        # Move to next chunk
        current_start = chunk_end + timedelta(seconds=1)
        current_start = current_start.replace(hour=0, minute=0, second=0)

        # Delay between chunks (but not after last chunk)
        if current_start <= end_date:
            time.sleep(2)

    logger.info(f"✓ {dataset.upper()} completed")
    logger.info(f"  Total uploaded: {total_uploaded:,} records")
    logger.info(f"  Failed chunks: {failed_chunks}")
    logger.info("")

    return {
        'success': failed_chunks == 0,
        'uploaded': total_uploaded,
        'failed_chunks': failed_chunks
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='CEPS SOAP API Pipeline - Download, Parse, Upload',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '--dataset',
        type=str,
        required=True,
        choices=['imbalance', 're_price', 'svr_activation', 'export_import_svr', 'all'],
        help='Dataset to process (or "all" for all datasets)'
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

    # Connect to database
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            port=DB_PORT
        )
        logger.info("✓ Connected to database")
        logger.info("")
    except Exception as e:
        logger.error(f"✗ Failed to connect to database: {e}")
        sys.exit(1)

    try:
        # Determine which datasets to process
        if args.dataset == 'all':
            datasets_to_process = ['imbalance', 're_price', 'svr_activation', 'export_import_svr']
        else:
            datasets_to_process = [args.dataset]

        # Process each dataset
        results = {}
        for dataset in datasets_to_process:
            result = process_dataset(dataset, start_date, end_date, conn, logger)
            results[dataset] = result

        # Final summary
        logger.info("=" * 70)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Date Range: {start_date.date()} to {end_date.date()}")
        logger.info("")

        total_success = 0
        total_failed = 0
        total_records = 0

        for dataset, result in results.items():
            if result['success']:
                total_success += 1
                total_records += result['uploaded']
                logger.info(f"✓ {dataset}: {result['uploaded']:,} records uploaded")
            else:
                total_failed += 1
                logger.error(f"✗ {dataset}: FAILED ({result['failed_chunks']} chunks failed)")

        logger.info("")
        logger.info(f"Datasets: {total_success} successful, {total_failed} failed")
        logger.info(f"Total Records: {total_records:,}")
        logger.info("=" * 70)

        if total_failed > 0:
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("")
        logger.info("✗ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"✗ Pipeline failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
