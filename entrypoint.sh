#!/bin/bash
set -e

# Dump container env vars to a file that cron lines source via
#   export $(cat /etc/environment_for_cron | xargs)
#
# Uses a DENYLIST of shell/system internals so any new var added to .env
# (or to the compose `environment:` block) reaches cron automatically.
#
# DO NOT replace this with an allowlist of prefixes — every new service
# prefix (TRADER_*, FOO_*, ...) would then silently fail until someone
# edits this script AND rebuilds the image. We've been bitten by that
# pattern multiple times; the denylist is intentional.
EXCLUDE_RE='^(_|PWD|OLDPWD|SHLVL|HOSTNAME|TERM|PS[1-4]|HOME|USER|SHELL|LOGNAME|COLUMNS|LINES)='
printenv | grep -vE "$EXCLUDE_RE" > /etc/environment_for_cron

# Visibility: log env-var KEYS (not values) at startup so a missing var
# can be diagnosed via `docker compose logs entsoe-ote-data-uploader`
# without needing to exec into the container.
echo "Cron environment keys (sourced by every cron line):"
cut -d= -f1 /etc/environment_for_cron | sort

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
