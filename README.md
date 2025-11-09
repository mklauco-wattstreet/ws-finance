# OTE-CR Price Data Downloader - Docker Setup

Automated system for downloading and storing electricity market price data from OTE-CR (Czech electricity market operator).

## Features

- **Auto Mode**: Automatically downloads missing files from last download to yesterday
- **Manual Mode**: Specify exact date ranges to download
- **Smart Recovery**: Detects gaps and downloads missing data
- **Scheduled Execution**: Daily cron jobs at 10:00 AM
- **Comprehensive Logging**: Detailed logs with debug mode
- **Environment-based Config**: Secure credential management

## Quick Start

1. **Set environment variables** in `.env`:
   ```bash
   # Database credentials are already configured
   # Add any additional variables if needed
   ```

2. **Build and run**:
   ```bash
   docker-compose up -d --build
   ```

3. **Monitor logs**:
   ```bash
   docker-compose logs -f
   # or
   tail -f logs/cron.log
   ```

## Structure

```
.
├── docker-compose.yml    # Docker Compose configuration
├── Dockerfile            # Python + cron container image
├── entrypoint.sh         # Container startup script
├── crontab               # Daily cron schedule (10:00 AM)
├── requirements.txt      # Python dependencies
├── .env                  # Environment variables (database credentials)
├── .env.example          # Environment template
├── app/                  # Python scripts (mounted as volume)
│   ├── config.py         # Configuration from environment
│   ├── common.py         # Shared utilities
│   ├── download_*.py     # Download scripts (3 types)
│   └── upload_*.py       # Upload scripts (3 types)
└── logs/                 # Cron logs (mounted as volume)
```

## Download Scripts

### Available Scripts

All download scripts support both **AUTO** and **MANUAL** modes:

| Script | Data Type | Auto Mode | Manual Mode |
|--------|-----------|-----------|-------------|
| `download_day_ahead_prices.py` | Day-ahead electricity prices | `python3 download_day_ahead_prices.py` | `python3 download_day_ahead_prices.py 2025-05-01 2025-05-31` |
| `download_imbalance_prices.py` | System imbalance prices | `python3 download_imbalance_prices.py` | `python3 download_imbalance_prices.py 2025-10-15 2025-10-31` |
| `download_intraday_prices.py` | Intraday market prices | `python3 download_intraday_prices.py` | `python3 download_intraday_prices.py 2025-10-15 2025-10-31` |

### AUTO Mode (Recommended for Cron)

When run without arguments, scripts automatically:
1. Search for the last downloaded file in the directory structure
2. Extract the date from the filename
3. Download all missing files from `(last_date + 1)` to `yesterday`
4. If no files exist, download from **2025-11-01** to `yesterday`

**Example:**
```bash
# In container or locally
cd app
python3 download_day_ahead_prices.py
# Output: "Last download: 2025-11-05, downloading 2025-11-06 to 2025-11-08"
```

**Benefits:**
- No manual date management
- Automatic gap recovery after missed runs
- Safe - won't re-download existing files

### MANUAL Mode

Specify exact date range:
```bash
cd app
python3 download_day_ahead_prices.py 2025-05-01 2025-05-31
```

### Debug Mode

Add `--debug` flag for verbose logging:
```bash
python3 download_day_ahead_prices.py --debug
python3 download_day_ahead_prices.py 2025-05-01 2025-05-31 --debug
```

## Cron Schedule

The `crontab` file defines the download schedule. Edit it to customize timing:

```cron
# Download day-ahead prices daily at 13:00
0 13 * * * export $(cat /etc/environment_for_cron | xargs) && cd /app/scripts && /usr/local/bin/python3 download_day_ahead_prices.py >> /var/log/cron.log 2>&1

# Download imbalance prices daily at 13:05
5 13 * * * export $(cat /etc/environment_for_cron | xargs) && cd /app/scripts && /usr/local/bin/python3 download_imbalance_prices.py >> /var/log/cron.log 2>&1

# Download intraday prices daily at 13:10
10 13 * * * export $(cat /etc/environment_for_cron | xargs) && cd /app/scripts && /usr/local/bin/python3 download_intraday_prices.py >> /var/log/cron.log 2>&1
```

All scripts run in AUTO mode, so they automatically determine what to download.

### Updating Crontab Without Restart

The `crontab` file is mounted as a volume. After editing it, reload with:

```bash
./reload-crontab.sh
```

This reloads the crontab in the running container **without rebuilding or restarting**.

## Environment Configuration

Database credentials are loaded from `.env`:
```bash
DB_HOST=james
DB_USER=user_finance
DB_PASSWORD=your_password
DB_NAME=finance
DB_PORT=5432
```

## Usage

### Start the container
```bash
docker-compose up -d --build
```

### View logs
```bash
docker-compose logs -f
```

### View cron logs specifically
```bash
tail -f logs/cron.log
```

### Stop the container
```bash
docker-compose down
```

### Restart after changes
```bash
# If you modified crontab, Dockerfile, or requirements.txt:
docker-compose up -d --build

# If you only modified scripts (they're mounted as volumes):
docker-compose restart
```

### Execute a script manually
```bash
docker-compose exec python-cron python /app/scripts/your_script.py
```

### Check crontab inside container
```bash
docker-compose exec python-cron crontab -l
```

## Development vs Production

### Development (Local with Tailscale)
- Scripts are mounted as volumes - edit on host, changes reflect immediately
- Logs are accessible in `logs/` directory
- Database "james" accessed via Tailscale (configured in `extra_hosts`)
- Use `docker-compose logs -f` to monitor
- Deploy with: `docker-compose up -d`

### Production (Database as Docker Container)
- Use Docker networking to connect to database container
- Deploy with production overrides: `docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d`
- Update `.env` to set `DB_HOST` to your database container name (e.g., `postgres`)
- See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed production setup instructions
- Set `restart: unless-stopped` in docker-compose.yml (already configured)
- Monitor logs with your logging solution
- Consider adding health checks

## Scaling to Multiple Services

If you need separate containers for different tasks:

```yaml
services:
  imbalance-prices:
    build: .
    volumes:
      - ./scripts:/app/scripts:ro
    # ... specific crontab for this service

  another-service:
    build: .
    volumes:
      - ./scripts:/app/scripts:ro
    # ... different crontab
```

## Troubleshooting

### Cron jobs not running
1. Check crontab syntax: `docker-compose exec python-cron crontab -l`
2. Verify logs: `tail -f logs/cron.log`
3. Ensure script paths are `/app/scripts/your_script.py`

### Python dependencies missing
1. Add to `requirements.txt`
2. Rebuild: `docker-compose up -d --build`

### Scripts not found
1. Ensure scripts are in `scripts/` directory
2. Check volume mount in docker-compose.yml
3. Verify path in crontab uses `/app/scripts/`

### Timezone issues
- Timezone is set to `Europe/Prague` in docker-compose.yml
- Change `TZ` environment variable if needed
