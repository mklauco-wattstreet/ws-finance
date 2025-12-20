#!/bin/bash

# OTE Portal Download Script for Production
# To be run daily at 09:00 via crontab

# Run the downloader
docker exec entsoe-ote-data-uploader bash -c "cd /app/scripts && python3 ote_final.py --debug"

# Check exit status
if [ $? -eq 0 ]; then
    echo "OTE download completed successfully"
    exit 0
else
    echo "OTE download failed"
    exit 1
fi