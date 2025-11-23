#!/bin/bash
# Script to reload crontab in running container without restarting

CONTAINER_NAME="python-cron-scheduler"

echo "Reloading crontab in container ${CONTAINER_NAME}..."

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Error: Container ${CONTAINER_NAME} is not running"
    exit 1
fi

# First, reload the crontab from the mounted file (critical step!)
echo "Loading updated crontab from mounted file..."
docker exec ${CONTAINER_NAME} /bin/sh -c "crontab /etc/cron.d/python-cron"

# Then, reload cron daemon to pick up changes
echo "Signaling cron daemon to reload..."
docker exec ${CONTAINER_NAME} /bin/sh -c "kill -HUP \$(pgrep cron)"

if [ $? -eq 0 ]; then
    echo "✓ Crontab reloaded successfully!"
    echo ""
    echo "=== Mounted file content (from /etc/cron.d/python-cron) ==="
    docker exec ${CONTAINER_NAME} head -15 /etc/cron.d/python-cron
    echo ""
    echo "=== Loaded crontab (what cron is actually using) ==="
    docker exec ${CONTAINER_NAME} crontab -l | head -15
else
    echo "✗ Error: Failed to reload crontab"
    exit 1
fi
