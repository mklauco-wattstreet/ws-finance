#!/usr/bin/env python3
"""
Cleanup old ENTSO-E XML files.

Retention policy: Keep only the last 1 day of XML files.
Files older than 1 day are automatically deleted.

This script runs daily at 2 AM via cron.

Usage:
    python3 cleanup_old_xml.py [--dry-run] [--debug]
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta


def setup_logging(debug=False):
    """Setup logging configuration."""
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def get_old_files(base_dir, days_old=1):
    """
    Find XML files older than specified days.

    Args:
        base_dir: Base directory to search
        days_old: Files older than this many days (default: 1)

    Returns:
        list: List of Path objects for old files
    """
    cutoff_time = datetime.now() - timedelta(days=days_old)
    old_files = []

    # Find all XML files in the data directory
    for xml_file in Path(base_dir).rglob("*.xml"):
        # Get file modification time
        file_mtime = datetime.fromtimestamp(xml_file.stat().st_mtime)

        if file_mtime < cutoff_time:
            old_files.append(xml_file)

    return sorted(old_files)


def delete_files(files, logger, dry_run=False):
    """
    Delete specified files.

    Args:
        files: List of Path objects to delete
        logger: Logger instance
        dry_run: If True, don't actually delete (default: False)

    Returns:
        tuple: (deleted_count, total_size_mb)
    """
    deleted_count = 0
    total_size = 0

    for file_path in files:
        try:
            file_size = file_path.stat().st_size
            total_size += file_size

            if dry_run:
                logger.info(f"[DRY RUN] Would delete: {file_path}")
            else:
                file_path.unlink()
                logger.debug(f"Deleted: {file_path}")

            deleted_count += 1

        except Exception as e:
            logger.error(f"Failed to delete {file_path}: {e}")

    total_size_mb = total_size / (1024 * 1024)
    return deleted_count, total_size_mb


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Cleanup old ENTSO-E XML files (keep last 1 day)"
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be deleted without actually deleting')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    parser.add_argument('--days', type=int, default=1,
                        help='Delete files older than N days (default: 1)')

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(debug=args.debug)

    logger.info("=" * 60)
    logger.info("ENTSO-E XML Cleanup Script")
    logger.info("=" * 60)
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Retention policy: Keep last {args.days} day(s)")

    if args.dry_run:
        logger.info("DRY RUN MODE - No files will be deleted")

    logger.info("")

    # Base directory for ENTSO-E data
    base_dir = Path(__file__).parent / "data"

    if not base_dir.exists():
        logger.warning(f"Data directory does not exist: {base_dir}")
        logger.info("Nothing to clean up.")
        sys.exit(0)

    logger.info(f"Scanning directory: {base_dir}")
    logger.info("")

    # Find old files
    old_files = get_old_files(base_dir, days_old=args.days)

    if not old_files:
        logger.info("✓ No old files found. Nothing to delete.")
        logger.info("")
        sys.exit(0)

    logger.info(f"Found {len(old_files)} file(s) older than {args.days} day(s)")
    logger.info("")

    # Delete old files
    deleted_count, total_size_mb = delete_files(old_files, logger, dry_run=args.dry_run)

    # Summary
    logger.info("=" * 60)
    logger.info("CLEANUP SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Files processed: {deleted_count}")
    logger.info(f"Disk space {'would be ' if args.dry_run else ''}freed: {total_size_mb:.2f} MB")
    logger.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    logger.info("")

    sys.exit(0)


if __name__ == '__main__':
    main()
