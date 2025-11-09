#!/bin/bash
# Script to reload crontab in running container without restarting

CONTAINER_NAME="python-cron-scheduler"

echo "Reloading crontab in container ${CONTAINER_NAME}..."

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Error: Container ${CONTAINER_NAME} is not running"
    exit 1
fi

# Reload the crontab from the mounted file
docker exec ${CONTAINER_NAME} crontab /etc/cron.d/python-cron

if [ $? -eq 0 ]; then
    echo "Crontab reloaded successfully!"
    echo ""
    echo "Current crontab:"
    docker exec ${CONTAINER_NAME} crontab -l
else
    echo "Error: Failed to reload crontab"
    exit 1
fi
