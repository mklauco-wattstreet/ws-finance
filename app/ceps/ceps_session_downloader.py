#!/usr/bin/env python3
"""
CEPS Session-Based Downloader
Reverse-engineered from ceps_source to properly handle PHP session state.

Key insight: The CEPS website requires a two-step process:
1. Call loadGraphData with filter parameters to UPDATE the PHP session
2. Call download-data endpoint which READS from the PHP session
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


def create_session():
    """Create a requests session with retry logic and proper headers."""
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
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'cs,en-US;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'X-Requested-With': 'XMLHttpRequest',  # Critical for AJAX
        'Cache-Control': 'no-cache',  # Bypass CDN cache
        'Pragma': 'no-cache',  # HTTP/1.0 cache bypass
    })

    return session


def download_ceps_data_session(data_tag, start_date, end_date, logger):
    """
    Download CEPS data using session-based approach (reverse-engineered from ceps_source).

    This follows the exact flow from the CEPS website JavaScript:
    1. Establish session by visiting main page
    2. Call loadGraphData with filter parameters (updates PHP session)
    3. Download CSV from download-data endpoint (reads from PHP session)

    Args:
        data_tag: CEPS data tag (e.g., "AktualniSystemovaOdchylkaCR")
        start_date: Start datetime
        end_date: End datetime
        logger: Logger instance

    Returns:
        Path to downloaded file, or None if failed
    """
    logger.info("=" * 60)
    logger.info("CEPS Session-Based Downloader (Reverse-Engineered)")
    logger.info(f"Tag: {data_tag}")
    logger.info(f"Date: {start_date.date()} to {end_date.date()}")
    logger.info("=" * 60)

    # Create session
    session = create_session()

    # Map data tags to their graph IDs (Czech version uses different IDs)
    # From ceps_source line 1828: graph-id="1040" for AktualniSystemovaOdchylkaCR
    graph_id_map = {
        'AktualniSystemovaOdchylkaCR': 1040,  # Czech version
    }
    graph_id = graph_id_map.get(data_tag, 1040)

    # Step 1: Initialize session by visiting main page
    logger.info("Step 1: Establishing session...")
    try:
        # First, visit the homepage to get initial cookies
        logger.info("  Visiting homepage to establish cookies...")
        homepage_response = session.get("https://www.ceps.cz/cs", timeout=10)
        logger.debug(f"  Homepage status: {homepage_response.status_code}")
        logger.debug(f"  Homepage cookies: {dict(session.cookies)}")

        time.sleep(0.5)

        # Then visit the data page
        logger.info("  Visiting data page...")
        init_response = session.get("https://www.ceps.cz/cs/data", timeout=10)

        if init_response.status_code == 200:
            logger.info(f"✓ Session established (status: {init_response.status_code})")
            logger.debug(f"Response headers: {dict(init_response.headers)}")
            logger.debug(f"Cookies received: {dict(session.cookies)}")

            # Check if Set-Cookie header exists
            if 'Set-Cookie' in init_response.headers:
                logger.debug(f"Set-Cookie header: {init_response.headers['Set-Cookie']}")

            # Check if we got PHPSESSID
            if 'PHPSESSID' in session.cookies:
                logger.info(f"✓ PHPSESSID cookie set: {session.cookies['PHPSESSID'][:8]}...")
            else:
                logger.warning("⚠ No PHPSESSID cookie - this may cause issues")
                logger.warning("  This means the server cannot maintain session state")
                logger.warning("  The download will likely contain default/wrong data")
        else:
            logger.error(f"✗ Failed to establish session (status: {init_response.status_code})")
            return None
    except Exception as e:
        logger.error(f"✗ Error establishing session: {e}")
        return None

    time.sleep(1)  # Be polite

    # Step 2: Call loadGraphData to UPDATE PHP session state
    # This is the critical step that was missing in previous attempts
    logger.info("Step 2: Updating server session with filter parameters...")

    # Format dates as the JavaScript does (from ceps_source line 3718)
    # Format: "YYYY-MM-DD HH:MM:SS" which gets converted to "YYYY-MM-DDTHH:MM:SS"
    date_from_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    date_to_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

    # Replace space with "T" as per serializeFilters() function (line 2963-2964)
    date_from_param = date_from_str.replace(" ", "T")
    date_to_param = date_to_str.replace(" ", "T")

    # Determine date_type based on date range (from getFilterData)
    if start_date.date() == end_date.date():
        date_type = "day"
        move_graph = "day"
    else:
        date_type = "day"  # TODO: implement week/month/year logic
        move_graph = "day"

    # Build parameters EXACTLY as the JavaScript does (from ceps_source lines 3470-3475)
    params = {
        'do': 'loadGraphData',              # Nette framework signal
        'method': data_tag,                 # The data type we want
        'graph_id': graph_id,               # Graph identifier
        'move_graph': move_graph,           # Navigation type
        'download': 'csv',                  # CRITICAL: Tells server we want to download
        'date_to': date_to_param,           # End date in ISO format with T
        'date_from': date_from_param,       # Start date in ISO format with T
        'agregation': 'MI',                 # Minute aggregation
        'date_type': date_type,             # Type of date selection
        'interval': 'false',                # Not using interval mode
        'version': 'RT',                    # Real-time version
        'function': 'AVG'                   # Average function
    }

    logger.info(f"  Calling loadGraphData to set session state...")
    logger.info(f"  date_from: {params['date_from']}")
    logger.info(f"  date_to: {params['date_to']}")
    logger.info(f"  method: {params['method']}")
    logger.info(f"  download: {params['download']}")

    try:
        # Update Referer header to match browser behavior
        session.headers.update({
            'Referer': 'https://www.ceps.cz/cs/data'
        })

        # Call loadGraphData (line 3476 from ceps_source)
        load_response = session.get(
            "https://www.ceps.cz/cs/data",  # Czech version
            params=params,
            timeout=15
        )

        logger.info(f"  Actual URL: {load_response.url}")

        if load_response.status_code == 200:
            logger.info("✓ loadGraphData call successful")
            logger.debug(f"Response length: {len(load_response.text)} bytes")
            logger.debug(f"Content-Type: {load_response.headers.get('Content-Type')}")

            # Parse JSON response
            try:
                json_data = load_response.json()
                logger.debug(f"Response keys: {list(json_data.keys())}")
                logger.debug(f"Full response: {json_data}")

                # Check output_format (line 3582 from ceps_source)
                if 'output_format' in json_data:
                    output_format = json_data.get('output_format', '').strip('"')
                    logger.info(f"✓ Server returned output_format: {output_format}")

                    # According to ceps_source line 3582, output_format should be "download"
                    if output_format != "download":
                        logger.warning(f"⚠ Unexpected output_format: {output_format} (expected: download)")

                # Check the 'state' field which might contain session information
                if 'state' in json_data:
                    logger.debug(f"State field: {json_data['state']}")

                # The session should now be updated with our filter parameters
                logger.info("✓ PHP session updated with filter parameters")

            except Exception as e:
                logger.warning(f"Could not parse JSON response: {e}")
                # Continue anyway - session might still be updated
        else:
            logger.error(f"✗ loadGraphData failed (status: {load_response.status_code})")
            logger.error(f"Response: {load_response.text[:500]}")
            return None

    except Exception as e:
        logger.error(f"✗ Error calling loadGraphData: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

    time.sleep(1)  # Small delay between requests

    # Step 3: Download CSV file (session state is now set)
    logger.info("Step 3: Downloading CSV file from session...")

    # From ceps_source line 3614: window.location.href = "/download-data/?format="+download
    download_url = "https://www.ceps.cz/download-data/"
    download_params = {'format': 'csv'}

    logger.info(f"  Requesting: {download_url}?format=csv")

    try:
        download_response = session.get(
            download_url,
            params=download_params,
            timeout=30
        )

        if download_response.status_code == 200:
            content_type = download_response.headers.get('Content-Type', '')
            content_disposition = download_response.headers.get('Content-Disposition', '')

            logger.info(f"  Content-Type: {content_type}")
            logger.info(f"  Content-Disposition: {content_disposition}")

            # Get the content
            content = download_response.content

            # Check if it's HTML (error page)
            if content.startswith(b'<!DOCTYPE') or content.startswith(b'<html'):
                logger.error("✗ Received HTML instead of CSV")
                try:
                    logger.error(f"Response preview: {content.decode('utf-8')[:500]}")
                except:
                    pass
                return None

            # Create destination directory
            dest_dir = Path(f"/app/scripts/ceps/{start_date.year}/{start_date.month:02d}")
            dest_dir.mkdir(parents=True, exist_ok=True)

            # Create filename with date range
            if start_date.date() == end_date.date():
                date_str = start_date.strftime("%Y%m%d")
            else:
                date_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

            timestamp = datetime.now().strftime("%H%M%S")
            dest_file = dest_dir / f"data_{data_tag}_{date_str}_{timestamp}.csv"

            # Save the file
            with open(dest_file, 'wb') as f:
                f.write(content)

            file_size = dest_file.stat().st_size
            logger.info(f"✓ CSV file downloaded and saved")
            logger.info(f"  File: {dest_file}")
            logger.info(f"  Size: {file_size:,} bytes")

            # Verify it's a valid CSV
            try:
                with open(dest_file, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    second_line = f.readline().strip()
                    logger.info(f"  Header: {first_line[:100]}")
                    logger.info(f"  Metadata: {second_line[:100]}")

                    # Check if it contains the expected date
                    date_check = start_date.strftime("%d.%m.%Y")
                    if date_check in second_line:
                        logger.info(f"✓ Verified: File contains expected date {date_check}")
                    else:
                        logger.warning(f"⚠ Warning: Expected date {date_check} not found in metadata line")
                        logger.warning(f"  This might still be correct data - check the file manually")

            except UnicodeDecodeError:
                logger.error("✗ File is not valid text")
                dest_file.unlink()
                return None

            logger.info("=" * 60)
            logger.info("✓ SUCCESS")
            logger.info("=" * 60)

            return dest_file

        else:
            logger.error(f"✗ Download failed (status: {download_response.status_code})")
            try:
                logger.error(f"Response: {download_response.text[:500]}")
            except:
                logger.error(f"Response (binary): {download_response.content[:100]}")
            return None

    except Exception as e:
        logger.error(f"✗ Error downloading CSV: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def main():
    """Main entry point."""
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description='CEPS Session-Based Downloader')
    parser.add_argument('--tag', type=str, default='AktualniSystemovaOdchylkaCR',
                       help='CEPS data tag to download')
    parser.add_argument('--start-date', type=str, default=None,
                       help='Start date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--end-date', type=str, default=None,
                       help='End date in YYYY-MM-DD format (default: same as start-date)')
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
            end_date = start_date  # Default to same day

        # Set time to cover full day
        start_date = start_date.replace(hour=0, minute=0, second=0)
        end_date = end_date.replace(hour=23, minute=59, second=59)

    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        logger.error("Date format must be YYYY-MM-DD (e.g., 2026-01-04)")
        sys.exit(1)

    exit_code = 0

    try:
        # Download data using session-based approach
        downloaded_file = download_ceps_data_session(args.tag, start_date, end_date, logger)

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
