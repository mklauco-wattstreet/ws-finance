#!/usr/bin/env python3
"""
ENTSO-E Data Pipeline - Fetch, Parse, and Upload to Database.

This script is designed to run every 15 minutes via cron.
It fetches imbalance prices and volumes, parses the XML data,
and uploads it to the PostgreSQL database.

Usage:
    python3 entsoe_pipeline.py [--debug] [--dry-run]

Options:
    --debug     Enable debug logging
    --dry-run   Fetch and parse data but don't upload to database
"""

import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2 import extras

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from entsoe.entsoe_client import EntsoeClient
from entsoe.entsoe_parser import EntsoeParser
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT


def setup_logging(debug=False):
    """
    Setup logging configuration.

    Args:
        debug: If True, set log level to DEBUG, otherwise INFO
    """
    log_level = logging.DEBUG if debug else logging.INFO

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    return logging.getLogger(__name__)


def connect_database(logger):
    """
    Connect to PostgreSQL database.

    Args:
        logger: Logger instance

    Returns:
        psycopg2 connection object or None if connection fails
    """
    logger.info("Connecting to PostgreSQL database...")

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            connect_timeout=10
        )
        logger.info(f"✓ Connected to {DB_NAME}@{DB_HOST}:{DB_PORT}")
        return conn
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return None


def create_tables_if_not_exist(conn, logger):
    """
    Create database tables if they don't exist.

    Args:
        conn: Database connection
        logger: Logger instance

    Returns:
        bool: True if successful
    """
    cursor = conn.cursor()

    try:
        # Create imbalance prices table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS entsoe_imbalance_prices (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                position INTEGER NOT NULL,
                price DECIMAL(12, 2),
                business_type VARCHAR(10),
                resolution_minutes INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, business_type)
            )
        """)

        # Create imbalance volumes table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS entsoe_imbalance_volumes (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                position INTEGER NOT NULL,
                volume DECIMAL(12, 2),
                business_type VARCHAR(10),
                resolution_minutes INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, business_type)
            )
        """)

        # Create indexes for better query performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_entsoe_prices_timestamp
            ON entsoe_imbalance_prices(timestamp)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_entsoe_volumes_timestamp
            ON entsoe_imbalance_volumes(timestamp)
        """)

        conn.commit()
        logger.info("✓ Database tables verified/created")
        return True

    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to create tables: {e}")
        return False
    finally:
        cursor.close()


def upload_prices_to_database(conn, records, logger):
    """
    Upload imbalance prices to database.

    Args:
        conn: Database connection
        records: List of parsed price records
        logger: Logger instance

    Returns:
        int: Number of records inserted
    """
    if not records:
        return 0

    cursor = conn.cursor()

    # Prepare bulk insert query
    insert_query = """
        INSERT INTO entsoe_imbalance_prices (
            timestamp, position, price, business_type, resolution_minutes
        ) VALUES %s
        ON CONFLICT (timestamp, business_type)
        DO UPDATE SET
            price = EXCLUDED.price,
            position = EXCLUDED.position,
            resolution_minutes = EXCLUDED.resolution_minutes
    """

    # Prepare values for bulk insert
    values = [
        (
            record['timestamp'],
            record['position'],
            record['price'],
            record['business_type'],
            record['resolution_minutes']
        )
        for record in records
    ]

    try:
        extras.execute_values(cursor, insert_query, values)
        conn.commit()
        inserted = len(values)
        logger.debug(f"Inserted/updated {inserted} price records")
        return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert prices: {e}")
        return 0
    finally:
        cursor.close()


def upload_volumes_to_database(conn, records, logger):
    """
    Upload imbalance volumes to database.

    Args:
        conn: Database connection
        records: List of parsed volume records
        logger: Logger instance

    Returns:
        int: Number of records inserted
    """
    if not records:
        return 0

    cursor = conn.cursor()

    # Prepare bulk insert query
    insert_query = """
        INSERT INTO entsoe_imbalance_volumes (
            timestamp, position, volume, business_type, resolution_minutes
        ) VALUES %s
        ON CONFLICT (timestamp, business_type)
        DO UPDATE SET
            volume = EXCLUDED.volume,
            position = EXCLUDED.position,
            resolution_minutes = EXCLUDED.resolution_minutes
    """

    # Prepare values for bulk insert
    values = [
        (
            record['timestamp'],
            record['position'],
            record['volume'],
            record['business_type'],
            record['resolution_minutes']
        )
        for record in records
    ]

    try:
        extras.execute_values(cursor, insert_query, values)
        conn.commit()
        inserted = len(values)
        logger.debug(f"Inserted/updated {inserted} volume records")
        return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert volumes: {e}")
        return 0
    finally:
        cursor.close()


def save_xml_file(xml_content, document_type, period_start, period_end, output_dir, logger):
    """
    Save XML content to file.

    Args:
        xml_content: XML content as string
        document_type: Document type (A85 or A86)
        period_start: Start datetime
        period_end: End datetime
        output_dir: Output directory path
        logger: Logger instance

    Returns:
        Path: Path to saved file
    """
    # Create output directory structure: YYYY/MM/
    year_month_dir = output_dir / period_start.strftime('%Y') / period_start.strftime('%m')
    year_month_dir.mkdir(parents=True, exist_ok=True)

    # Create filename
    doc_type_name = {
        'A85': 'imbalance_prices',
        'A86': 'imbalance_volumes'
    }.get(document_type, document_type)

    filename = f"entsoe_{doc_type_name}_{period_start.strftime('%Y%m%d_%H%M')}_{period_end.strftime('%H%M')}.xml"
    file_path = year_month_dir / filename

    # Save XML content
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(xml_content)

    logger.debug(f"Saved XML to: {file_path}")
    return file_path


def process_document_type(client, parser, conn, document_type, dry_run, output_dir, logger):
    """
    Process a single document type - fetch, parse, save, and upload.

    Args:
        client: EntsoeClient instance
        parser: EntsoeParser instance
        conn: Database connection
        document_type: Document type (A85 or A86)
        dry_run: If True, don't upload to database
        output_dir: Directory to save XML files
        logger: Logger instance

    Returns:
        bool: True if successful
    """
    data_type_name = {
        'A85': 'Imbalance Prices',
        'A86': 'Imbalance Volumes'
    }.get(document_type, document_type)

    # Get preceding hour range
    period_start, period_end = client.get_preceding_hour_range()

    logger.info(f"{'=' * 60}")
    logger.info(f"Processing {data_type_name} (Document Type: {document_type})")
    logger.info(f"{'=' * 60}")
    logger.info(f"Period: {period_start.strftime('%Y-%m-%d %H:%M')} to {period_end.strftime('%Y-%m-%d %H:%M')}")

    try:
        # Fetch XML data
        logger.info("Fetching data from ENTSO-E API...")
        xml_content = client.fetch_data(document_type, period_start, period_end)

        if not xml_content:
            logger.warning("No data received from API")
            return False

        logger.debug(f"Received {len(xml_content)} bytes of XML data")

        # Save XML file
        logger.info("Saving XML file...")
        xml_path = save_xml_file(xml_content, document_type, period_start, period_end, output_dir, logger)
        logger.info(f"✓ Saved XML to: {xml_path}")

        # Parse XML data
        logger.info("Parsing XML data...")
        if document_type == 'A85':
            records = parser.parse_imbalance_prices(xml_content)
        else:  # A86
            records = parser.parse_imbalance_volumes(xml_content)

        logger.info(f"✓ Parsed {len(records)} records")

        if not records:
            logger.warning("No records extracted from XML")
            return False

        # Upload to database
        if dry_run:
            logger.info("DRY RUN - Skipping database upload")
            logger.info(f"Would upload {len(records)} records")
        else:
            logger.info(f"Uploading {len(records)} records to database...")

            if document_type == 'A85':
                inserted = upload_prices_to_database(conn, records, logger)
            else:  # A86
                inserted = upload_volumes_to_database(conn, records, logger)

            if inserted > 0:
                logger.info(f"✓ Successfully uploaded {inserted} records")
            else:
                logger.error("✗ Failed to upload records")
                return False

        return True

    except Exception as e:
        logger.error(f"✗ Failed to process {data_type_name}: {e}")
        if logger.level <= logging.DEBUG:
            logger.exception("Full exception:")
        return False


def main():
    """Main function."""
    parser_args = argparse.ArgumentParser(
        description="ENTSO-E Data Pipeline - Fetch, Parse, and Upload"
    )
    parser_args.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    parser_args.add_argument(
        '--dry-run',
        action='store_true',
        help='Fetch and parse data but don\'t upload to database'
    )
    parser_args.add_argument(
        '--document-type',
        choices=['A85', 'A86', 'all'],
        default='all',
        help='Document type to process (A85=prices, A86=volumes, all=both)'
    )
    parser_args.add_argument(
        '--output-dir',
        type=str,
        default='/app/scripts/entsoe/data',
        help='Output directory for XML files (default: /app/scripts/entsoe/data)'
    )

    args = parser_args.parse_args()

    # Setup logging
    logger = setup_logging(debug=args.debug)

    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║  ENTSO-E Data Pipeline                                   ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.dry_run:
        logger.info("DRY RUN MODE - No data will be uploaded to database")

    logger.info("")

    # Create output directory for XML files
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"XML output directory: {output_dir}")

    # Initialize ENTSO-E client
    try:
        logger.info("Initializing ENTSO-E client...")
        client = EntsoeClient()
        logger.info(f"✓ Client initialized (control area: {client.control_area_domain})")
        logger.info("")
    except Exception as e:
        logger.error(f"✗ Client initialization failed: {e}")
        sys.exit(1)

    # Initialize parser
    parser = EntsoeParser()
    logger.debug("Parser initialized")

    # Connect to database
    conn = None
    if not args.dry_run:
        conn = connect_database(logger)
        if not conn:
            logger.error("Cannot proceed without database connection")
            sys.exit(1)

        # Create tables if they don't exist
        if not create_tables_if_not_exist(conn, logger):
            logger.error("Cannot proceed without database tables")
            conn.close()
            sys.exit(1)

        logger.info("")

    # Process document types
    success = True

    try:
        if args.document_type in ['A85', 'all']:
            if not process_document_type(
                client, parser, conn, 'A85', args.dry_run, output_dir, logger
            ):
                success = False

        if args.document_type in ['A86', 'all']:
            if not process_document_type(
                client, parser, conn, 'A86', args.dry_run, output_dir, logger
            ):
                success = False

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

    # Summary
    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info(f"║  {'Pipeline Completed Successfully' if success else 'Pipeline Completed with Errors':<56} ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()