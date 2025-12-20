#!/bin/bash

# Dump environment variables to a file that cron jobs can source
printenv | grep -E '^(DB_|PYTHONPATH|TZ|ENTSOE_|OTE_)' > /etc/environment_for_cron

# Load the crontab from the mounted file
crontab /etc/cron.d/entsoe-ote-cron

# Start cron in foreground
echo "Starting cron service..."
cron

# Print crontab to verify it's loaded
echo "Loaded crontab:"
crontab -l

# Create log file if it doesn't exist
touch /var/log/cron.log

# Keep container running and tail the log file
echo "Monitoring cron logs..."
tail -f /var/log/cron.log
