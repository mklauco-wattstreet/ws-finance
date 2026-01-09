#!/usr/bin/env python3
"""
CEPS Data API Downloader
Downloads CSV data from CEPS using direct HTTP API calls instead of headless browser.
Much faster and more stable than browser automation.
"""

import sys
import time
from pathlib import Path
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import setup_logging
from ceps.constants import CEPS_BASE_URL


# Czech month names mapping
CZECH_MONTHS = {
    1: "leden", 2: "únor", 3: "březen", 4: "duben",
    5: "květen", 6: "červen", 7: "červenec", 8: "srpen",
    9: "září", 10: "říjen", 11: "listopad", 12: "prosinec"
}


def create_session():
    """Create a requests session with retry logic."""
    session = requests.Session()

    # Retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Set realistic browser headers
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'cs,en-US;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.ceps.cz/cs/data',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
    })

    return session


def accept_cookies(session, logger):
    """Visit the main page and accept cookies if needed."""
    try:
        logger.info("Visiting CEPS homepage (English) to establish session...")
        response = session.get("https://www.ceps.cz/en/all-data", timeout=10)

        if response.status_code == 200:
            logger.info(f"✓ Session established (status: {response.status_code})")
            logger.debug(f"Response headers: {dict(response.headers)}")
            logger.debug(f"Cookies from response: {response.cookies}")
            logger.debug(f"Session cookies: {dict(session.cookies)}")

            # Check if we have any cookies
            if not session.cookies:
                logger.warning("⚠ No session cookies set - this may cause issues!")

            return True
        else:
            logger.error(f"✗ Failed to establish session (status: {response.status_code})")
            return False

    except Exception as e:
        logger.error(f"✗ Error establishing session: {e}")
        return False


def load_graph_data(session, data_tag, start_date, end_date, logger):
    """
    Load graph data by calling the AJAX endpoint.
    This sets up the session state for the download.

    Args:
        session: requests.Session object
        data_tag: CEPS data tag
        start_date: Start datetime
        end_date: End datetime
        logger: Logger instance

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Loading graph data via AJAX endpoint...")

    # Format dates in ISO format with T separator
    date_from = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    date_to = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    # Determine date_type based on date range
    if start_date.date() == end_date.date():
        date_type = "day"
        move_graph = "day"
    else:
        # TODO: implement date range logic
        date_type = "day"
        move_graph = "day"

    # Map data tags to their graph IDs (English version)
    # TODO: We'll need to discover the graph_id for other tags
    graph_id_map = {
        'AktualniSystemovaOdchylkaCR': 1026,  # English version uses 1026, not 1040
        # Add more mappings as we discover them
    }

    graph_id = graph_id_map.get(data_tag, 1026)  # Default to 1026

    # Build parameters - EXACTLY as browser sends them when clicking download
    params = {
        'do': 'loadGraphData',
        'method': data_tag,  # The data tag itself is the method!
        'graph_id': graph_id,
        'move_graph': move_graph,
        'download': 'csv',  # When downloading, browser sends 'csv' not 'false'!
        'date_to': date_to,
        'date_from': date_from,
        'agregation': 'MI',  # Minute aggregation (as browser sends)
        'date_type': date_type,
        'interval': 'false',
        'version': 'RT',  # Real-time version
        'function': 'AVG'  # Average function
    }

    logger.info(f"  date_from: {params['date_from']}")
    logger.info(f"  date_to: {params['date_to']}")
    logger.info(f"  method: {params['method']}")
    logger.info(f"  graph_id: {params['graph_id']}")
    logger.info(f"  agregation: {params['agregation']}")

    try:
        # Update referer to the specific anchor (English version)
        session.headers.update({
            'Referer': f'https://www.ceps.cz/en/all-data#{data_tag}',
            'X-Requested-With': 'XMLHttpRequest'
        })

        response = session.get(
            "https://www.ceps.cz/en/all-data",
            params=params,
            timeout=15
        )

        # Log the actual URL that was requested
        logger.info(f"  Actual loadGraphData URL: {response.url}")

        if response.status_code == 200:
            logger.info(f"✓ Graph data loaded successfully")
            logger.debug(f"Response length: {len(response.text)} bytes")
            logger.debug(f"Response Content-Type: {response.headers.get('Content-Type')}")

            # Check if this is a direct CSV download (when download=csv)
            content_type = response.headers.get('Content-Type', '')
            if 'text/csv' in content_type or 'application/octet-stream' in content_type:
                logger.info("✓ Received direct CSV file from loadGraphData")
                return response  # Return response with CSV data

            # Try to parse JSON response
            try:
                json_data = response.json()
                logger.debug(f"Response keys: {list(json_data.keys())}")

                # Check if response contains a download URL or redirect
                if 'redirect' in json_data:
                    logger.info(f"Backend redirect: {json_data.get('redirect')}")

                return json_data  # Return JSON data for further processing
            except:
                logger.warning("Response is not JSON, might be direct file download")
                logger.debug(f"Response content preview: {response.content[:200]}")
                return response  # Return response object if not JSON
        else:
            logger.error(f"✗ Failed to load graph data (status: {response.status_code})")
            logger.error(f"Response: {response.text[:500]}")
            return False

    except Exception as e:
        logger.error(f"✗ Error loading graph data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def download_csv(session, data_tag, start_date, end_date, logger):
    """
    Download CSV file from session (filters already set by loadGraphData).

    Args:
        session: requests.Session object
        data_tag: CEPS data tag
        start_date: Start datetime
        end_date: End datetime
        logger: Logger instance

    Returns:
        Path to downloaded file, or None if failed
    """
    logger.info("Downloading CSV file from session...")

    try:
        # Simple download - filters are in the session from loadGraphData
        download_params = {
            'format': 'csv'
        }

        logger.info(f"  Download params: {download_params}")

        # Don't use stream=True to allow automatic decompression
        response = session.get(
            "https://www.ceps.cz/download-data/",
            params=download_params,
            timeout=30
        )

        # Log the actual URL that was requested
        logger.info(f"  Actual download URL: {response.url}")

        if response.status_code == 200:
            # Check if we got a CSV file
            content_type = response.headers.get('Content-Type', '')
            content_disposition = response.headers.get('Content-Disposition', '')
            content_encoding = response.headers.get('Content-Encoding', '')

            logger.info(f"  Content-Type: {content_type}")
            logger.info(f"  Content-Disposition: {content_disposition}")
            logger.info(f"  Content-Encoding: {content_encoding}")

            # Get the decompressed content
            content = response.content

            # Check if it's HTML (error page)
            if content.startswith(b'<!DOCTYPE') or content.startswith(b'<html'):
                logger.error("✗ Received HTML instead of CSV - likely an error page")
                try:
                    logger.error(f"Response: {content.decode('utf-8')[:500]}")
                except:
                    logger.error(f"Response (binary): {content[:100]}")
                return None

            # Create destination directory
            dest_dir = Path(f"/app/scripts/ceps/{start_date.year}/{start_date.month:02d}")
            dest_dir.mkdir(parents=True, exist_ok=True)

            # Rename file with date range
            if start_date.date() == end_date.date():
                date_str = start_date.strftime("%Y%m%d")
            else:
                date_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

            timestamp = datetime.now().strftime("%H%M%S")
            dest_file = dest_dir / f"data_{data_tag}_{date_str}_{timestamp}.csv"

            # Save the decompressed content
            with open(dest_file, 'wb') as f:
                f.write(content)

            file_size = dest_file.stat().st_size
            logger.info(f"✓ CSV file downloaded: {dest_file}")
            logger.info(f"  File size: {file_size:,} bytes")

            # Verify it's a valid CSV (check first line)
            try:
                with open(dest_file, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    logger.info(f"  First line: {first_line[:100]}")

                    # Check if it looks like CSV
                    if not first_line or (not ',' in first_line and not ';' in first_line):
                        logger.error("✗ File doesn't look like a valid CSV")
                        dest_file.unlink()
                        return None
            except UnicodeDecodeError:
                logger.error("✗ File is not valid text - still compressed or corrupted")
                dest_file.unlink()
                return None

            return dest_file

        else:
            logger.error(f"✗ Failed to download CSV (status: {response.status_code})")
            try:
                logger.error(f"Response: {response.text[:500]}")
            except:
                logger.error(f"Response (binary): {response.content[:100]}")
            return None

    except Exception as e:
        logger.error(f"✗ Error downloading CSV: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def download_ceps_data_api(data_tag, start_date, end_date, logger):
    """
    Main function to download CEPS data using API approach.

    Args:
        data_tag: CEPS data tag (e.g., "AktualniSystemovaOdchylkaCR")
        start_date: Start datetime
        end_date: End datetime
        logger: Logger instance

    Returns:
        Path to downloaded file, or None if failed
    """
    logger.info("=" * 60)
    logger.info("CEPS API Downloader")
    logger.info(f"Tag: {data_tag}")
    logger.info(f"Date Range: {start_date.date()} to {end_date.date()}")
    logger.info("=" * 60)

    # Create session
    session = create_session()

    # Step 1: Establish session and accept cookies
    if not accept_cookies(session, logger):
        logger.error("Failed to establish session")
        return None

    time.sleep(1)  # Small delay to be polite

    # Step 2: Load graph data with download=csv (might return CSV directly)
    load_result = load_graph_data(session, data_tag, start_date, end_date, logger)
    if not load_result:
        logger.error("Failed to load graph data")
        return None

    # Check if load_result is a Response object with CSV data
    if hasattr(load_result, 'content'):
        logger.info("loadGraphData returned CSV file directly, saving it...")

        # Save the CSV directly from loadGraphData response
        try:
            content = load_result.content

            # Check if it's HTML (error page)
            if content.startswith(b'<!DOCTYPE') or content.startswith(b'<html'):
                logger.error("✗ Received HTML instead of CSV from loadGraphData")
                return None

            # Create destination directory
            dest_dir = Path(f"/app/scripts/ceps/{start_date.year}/{start_date.month:02d}")
            dest_dir.mkdir(parents=True, exist_ok=True)

            # Rename file with date range
            if start_date.date() == end_date.date():
                date_str = start_date.strftime("%Y%m%d")
            else:
                date_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

            timestamp = datetime.now().strftime("%H%M%S")
            dest_file = dest_dir / f"data_{data_tag}_{date_str}_{timestamp}.csv"

            # Save the content
            with open(dest_file, 'wb') as f:
                f.write(content)

            file_size = dest_file.stat().st_size
            logger.info(f"✓ CSV file saved: {dest_file}")
            logger.info(f"  File size: {file_size:,} bytes")

            # Verify it's a valid CSV (check first line)
            try:
                with open(dest_file, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    logger.info(f"  First line: {first_line[:100]}")
            except UnicodeDecodeError:
                logger.error("✗ File is not valid text")
                dest_file.unlink()
                return None

            downloaded_file = dest_file
        except Exception as e:
            logger.error(f"✗ Error saving CSV from loadGraphData: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    else:
        # JSON response - need to call separate download endpoint
        logger.debug(f"Session cookies after loadGraphData: {dict(session.cookies)}")
        time.sleep(1)

        # Step 3: Download CSV file from separate endpoint
        downloaded_file = download_csv(session, data_tag, start_date, end_date, logger)

    if downloaded_file:
        logger.info("=" * 60)
        logger.info("✓ SUCCESS - Data downloaded via API")
        logger.info(f"File: {downloaded_file}")
        logger.info("=" * 60)
        return downloaded_file
    else:
        logger.error("=" * 60)
        logger.error("✗ FAILED - Download unsuccessful")
        logger.error("=" * 60)
        return None


def main():
    """Main entry point for CEPS API downloader."""
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description='CEPS Data API Downloader')
    parser.add_argument('--tag', type=str, default='AktualniSystemovaOdchylkaCR',
                       help='CEPS data tag to download')
    parser.add_argument('--start-date', type=str, default=None,
                       help='Start date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--end-date', type=str, default=None,
                       help='End date in YYYY-MM-DD format (default: today)')
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
            end_date = datetime.combine(date.today(), datetime.min.time())

        # Set time to cover full day
        start_date = start_date.replace(hour=0, minute=0, second=0)
        end_date = end_date.replace(hour=23, minute=59, second=59)

    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        logger.error("Date format must be YYYY-MM-DD (e.g., 2026-01-05)")
        sys.exit(1)

    exit_code = 0

    try:
        # Download data using API
        downloaded_file = download_ceps_data_api(args.tag, start_date, end_date, logger)

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
