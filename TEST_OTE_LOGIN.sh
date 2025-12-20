#!/bin/bash

# OTE Login Test with Screenshot Debugging

echo "====================================="
echo "OTE Portal Login Test"
echo "====================================="

# Clean up old screenshots on host
echo "Cleaning up old screenshots..."
rm -f logs/screenshot_*.png

# Run the test
echo "Running login test..."
docker exec entsoe-ote-data-uploader python3 /app/scripts/ote_test_login.py

# Copy screenshots to host
echo ""
echo "Copying screenshots to logs/ folder..."
docker cp entsoe-ote-data-uploader:/var/log/. logs/ 2>/dev/null

# List screenshots
echo ""
echo "Screenshots generated:"
ls -la logs/screenshot_*.png 2>/dev/null | tail -20

echo ""
echo "====================================="
echo "View screenshots in logs/ folder"
echo "====================================="