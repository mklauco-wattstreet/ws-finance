#!/usr/bin/env python3
"""
CEPS SOAP API Pipeline

Complete pipeline: Download via SOAP API -> Parse XML -> Upload to DB
Downloads XML to disk, then processes immediately (chunk by chunk).
Uses UPSERT logic for safe backfills.

Usage:
    # Normal mode - fetch today's data
    python3 -m ceps.ceps_soap_pipeline --dataset all

    # Single dataset, single day
    python3 -m ceps.ceps_soap_pipeline --dataset imbalance --start 2026-01-09

    # All datasets, date range
    python3 -m ceps.ceps_soap_pipeline --dataset all --start 2025-12-01 --end 2025-12-31

    # Large backfill (auto-chunks to 7-day requests)
    python3 -m ceps.ceps_soap_pipeline --dataset all --start 2024-12-01 --end 2025-12-31

    # Dry run (fetch and parse but skip database upload)
    python3 -m ceps.ceps_soap_pipeline --dataset all --dry-run --debug
"""

import sentry_init  # noqa: F401 - must be first to capture errors
sentry_init.set_module("ceps")
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, List
import psycopg2
import requests
import xml.etree.ElementTree as ET
import time

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

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
    },
    'generation_res': {
        'operation': 'GenerationRES',
        'soap_action': 'https://www.ceps.cz/CepsData/GenerationRES',
    },
    'generation': {
        'operation': 'Generation',
        'soap_action': 'https://www.ceps.cz/CepsData/Generation',
    },
    'generation_plan': {
        'operation': 'GenerationPlan',
        'soap_action': 'https://www.ceps.cz/CepsData/GenerationPlan',
    },
    'estimated_imbalance_price': {
        'operation': 'OdhadovanaCenaOdchylky',
        'soap_action': 'https://www.ceps.cz/CepsData/OdhadovanaCenaOdchylky',
    }
}

# All datasets for --dataset all
ALL_DATASETS = [
    'imbalance', 're_price', 'svr_activation', 'export_import_svr',
    'generation_res', 'generation', 'generation_plan', 'estimated_imbalance_price'
]

# Maximum chunk size (CEPS API works best with 7-day chunks)
MAX_CHUNK_DAYS = 7

# Data directory (container path, mounted from ./downloads)
DATA_DIR = Path("/app/downloads/ceps/soap")


def setup_logging(debug: bool = False) -> logging.Logger:
    """Configure logging."""
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Suppress verbose request logging
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    return logging.getLogger("CEPS-Pipeline")


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
    elif operation == 'GenerationRES':
        body_content = f"""
        <{operation} xmlns="https://www.ceps.cz/CepsData/">
            <dateFrom>{date_from}</dateFrom>
            <dateTo>{date_to}</dateTo>
            <agregation>MI</agregation>
            <function>AVG</function>
            <version>RT</version>
            <para1>all</para1>
        </{operation}>
        """
    elif operation == 'Generation':
        body_content = f"""
        <{operation} xmlns="https://www.ceps.cz/CepsData/">
            <dateFrom>{date_from}</dateFrom>
            <dateTo>{date_to}</dateTo>
            <agregation>QH</agregation>
            <function>AVG</function>
            <version>RT</version>
            <para1>all</para1>
        </{operation}>
        """
    elif operation == 'GenerationPlan':
        body_content = f"""
        <{operation} xmlns="https://www.ceps.cz/CepsData/">
            <dateFrom>{date_from}</dateFrom>
            <dateTo>{date_to}</dateTo>
            <agregation>QH</agregation>
            <function>AVG</function>
            <version>RT</version>
        </{operation}>
        """
    elif operation == 'OdhadovanaCenaOdchylky':
        body_content = f"""
        <{operation} xmlns="https://www.ceps.cz/CepsData/">
            <dateFrom>{date_from}</dateFrom>
            <dateTo>{date_to}</dateTo>
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


def download_chunk_to_disk(
    dataset: str,
    start_date: datetime,
    end_date: datetime,
    logger: logging.Logger
) -> Optional[Path]:
    """
    Download one chunk via SOAP API and save to disk.

    Returns:
        Path to saved XML file or None if failed
    """
    config = DATASETS[dataset]
    operation = config['operation']
    soap_action = config['soap_action']

    date_from = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    date_to = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    soap_envelope = build_soap_envelope(operation, date_from, date_to)

    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': soap_action,
        'User-Agent': 'CEPS-SOAP-Client/1.0'
    }

    try:
        start_time = time.time()

        response = requests.post(
            SOAP_URL,
            data=soap_envelope.encode('utf-8'),
            headers=headers,
            timeout=60
        )

        response_time = time.time() - start_time

        if response.status_code != 200:
            logger.error(f"  HTTP {response.status_code}")
            return None

        root = ET.fromstring(response.content)
        ns_prefix = config['soap_action'].rsplit('/', 1)[0]
        result_element = root.find(f".//{{{ns_prefix}/}}{operation}Result")

        if result_element is None:
            logger.error(f"  Result element not found in response")
            return None

        # Save to disk
        output_dir = DATA_DIR / str(start_date.year) / f"{start_date.month:02d}"
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{dataset}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{timestamp}.xml"
        output_path = output_dir / filename

        with open(output_path, 'wb') as f:
            f.write(response.content)

        logger.debug(f"  {dataset}: downloaded in {response_time:.2f}s -> {output_path.name}")
        return output_path

    except requests.exceptions.Timeout:
        logger.error(f"  Request timeout")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"  Request failed: {e}")
        return None
    except ET.ParseError as e:
        logger.error(f"  XML parse error: {e}")
        return None


def process_dataset(
    dataset: str,
    start_date: datetime,
    end_date: datetime,
    conn,
    logger: logging.Logger,
    dry_run: bool = False
) -> Dict[str, Any]:
    """Process one dataset: download chunks, parse, upload."""
    total_days = (end_date.date() - start_date.date()).days + 1
    num_chunks = (total_days + MAX_CHUNK_DAYS - 1) // MAX_CHUNK_DAYS

    total_uploaded = 0
    total_parsed = 0
    failed_chunks = 0

    current_start = start_date
    chunk_num = 0

    while current_start <= end_date:
        chunk_num += 1

        chunk_end = min(
            current_start + timedelta(days=MAX_CHUNK_DAYS - 1),
            end_date
        )
        chunk_end = chunk_end.replace(hour=23, minute=59, second=59)

        xml_path = download_chunk_to_disk(dataset, current_start, chunk_end, logger)

        if xml_path is None:
            failed_chunks += 1
            logger.error(f"  {dataset}: chunk {chunk_num}/{num_chunks} failed")
        else:
            try:
                root = ET.parse(xml_path).getroot()
                config = DATASETS[dataset]
                operation = config['operation']
                ns_prefix = config['soap_action'].rsplit('/', 1)[0]

                result_element = root.find(f".//{{{ns_prefix}/}}{operation}Result")

                if result_element is None:
                    logger.error(f"  {dataset}: XML parse failed")
                    failed_chunks += 1
                else:
                    records = parse_soap_xml(dataset, result_element)
                    total_parsed += len(records)

                    if records and not dry_run:
                        try:
                            # Deduplicate records
                            seen = {}
                            for record in records:
                                if dataset == 'estimated_imbalance_price':
                                    key = (record['trade_date'], record['time_interval'])
                                else:
                                    key = record['delivery_timestamp']
                                seen[key] = record
                            deduped_records = list(seen.values())

                            uploaded = upsert_data(dataset, deduped_records, conn, logger)
                            total_uploaded += uploaded
                        except Exception as e:
                            logger.error(f"  {dataset}: upload failed: {e}")
                            failed_chunks += 1
                            conn.rollback()

            except Exception as e:
                logger.error(f"  {dataset}: parse failed: {e}")
                failed_chunks += 1

        current_start = chunk_end + timedelta(seconds=1)
        current_start = current_start.replace(hour=0, minute=0, second=0)

        if current_start <= end_date:
            time.sleep(2)

    # One-line summary per dataset
    if dry_run:
        logger.info(f"  {dataset}: {total_parsed:,} parsed (dry run)")
    elif failed_chunks > 0:
        logger.warning(f"  {dataset}: {total_uploaded:,} uploaded, {failed_chunks} chunks failed")
    else:
        logger.info(f"  {dataset}: {total_uploaded:,} uploaded")

    return {
        'success': failed_chunks == 0,
        'parsed': total_parsed,
        'uploaded': total_uploaded,
        'failed_chunks': failed_chunks
    }


def print_header(logger: logging.Logger) -> None:
    """Print pipeline header (debug only)."""
    pass


def print_footer(logger: logging.Logger, success: bool) -> None:
    """Print pipeline footer (debug only)."""
    pass


def parse_date(date_str: Optional[str]) -> Optional[date]:
    """Parse date string to date object."""
    if date_str is None:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD.")


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
        choices=list(DATASETS.keys()) + ['all'],
        help='Dataset to process (or "all" for all datasets)'
    )
    parser.add_argument(
        '--start',
        type=str,
        metavar='YYYY-MM-DD',
        help='Start date (defaults to today)'
    )
    parser.add_argument(
        '--end',
        type=str,
        metavar='YYYY-MM-DD',
        help='End date (defaults to start date)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Fetch and parse but skip database upload'
    )

    args = parser.parse_args()

    logger = setup_logging(debug=args.debug)

    # Parse dates
    # Default to yesterday to catch late-arriving data from midnight boundary
    # (e.g., 23:45-00:00 interval not available until after midnight)
    try:
        start = parse_date(args.start) or (date.today() - timedelta(days=1))
        end = parse_date(args.end) or date.today()
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    if end < start:
        logger.error(f"End date ({end}) must be >= start date ({start})")
        sys.exit(1)

    start_date = datetime.combine(start, datetime.min.time())
    end_date = datetime.combine(end, datetime.min.time()).replace(hour=23, minute=59, second=59)

    datasets_to_process = ALL_DATASETS if args.dataset == 'all' else [args.dataset]
    ds_label = args.dataset if args.dataset != 'all' else f"all ({len(datasets_to_process)})"
    logger.info(f"CEPS {ds_label} {start}..{end}{' (dry run)' if args.dry_run else ''}")

    # Connect to database (skip if dry run)
    conn = None
    if not args.dry_run:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                dbname=DB_NAME,
                port=DB_PORT,
                connect_timeout=15
            )
        except Exception as e:
            logger.error(f"DB connection failed: {e}")
            sys.exit(1)

    try:
        results = {}
        for dataset in datasets_to_process:
            result = process_dataset(
                dataset, start_date, end_date, conn, logger,
                dry_run=args.dry_run
            )
            results[dataset] = result

        # Summary
        total_failed = sum(1 for r in results.values() if not r['success'])
        total_uploaded = sum(r['uploaded'] for r in results.values())

        if total_failed > 0:
            logger.warning(f"CEPS done: {total_uploaded:,} records, {total_failed} datasets failed")
            sys.exit(1)
        else:
            logger.info(f"CEPS done: {total_uploaded:,} records uploaded")

    except KeyboardInterrupt:
        logger.info("Interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
