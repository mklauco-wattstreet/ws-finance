FROM python:3.11-slim

# Install cron and other necessary packages
RUN apt-get update && apt-get install -y \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy crontab file
COPY crontab /etc/cron.d/python-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/python-cron

# Apply cron job
RUN crontab /etc/cron.d/python-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Run the command on container startup
ENTRYPOINT ["/entrypoint.sh"]
