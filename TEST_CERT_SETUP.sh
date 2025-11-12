#!/bin/bash

# Certificate Setup Test with Screenshots

echo "====================================="
echo "OTE Certificate Setup Test"
echo "====================================="

# Clean up old screenshots
echo "Cleaning up old screenshots..."
rm -f logs/screenshot_*.png

# Run certificate setup
echo "Running certificate setup..."
docker exec python-cron-scheduler python3 /app/scripts/ote_production.py --setup

# Copy screenshots to host
echo ""
echo "Copying screenshots to logs/ folder..."
docker cp python-cron-scheduler:/var/log/. logs/ 2>/dev/null

# List screenshots
echo ""
echo "Screenshots generated:"
ls -la logs/screenshot_*.png 2>/dev/null | tail -20

echo ""
echo "====================================="
echo "Check screenshots to see what happened:"
echo "- screenshot_*_setup_page_loaded.png - Initial page"
echo "- screenshot_*_certificate_settings_opened.png - After clicking Certificate"
echo "- screenshot_*_setup_error.png - If there was an error"
echo "====================================="