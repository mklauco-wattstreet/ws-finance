# Production Deployment Guide

Quick reference for deploying to production server using Docker networking (Option A).

## Overview

**Local Development:** Database "james" accessed via Tailscale
**Production:** Database accessed via Docker network (database runs as Docker container)

---

## Production Deployment Steps

### 1. Find Your PostgreSQL Container's Network

```bash
# Get the network name
docker inspect <postgres-container-name> | grep -A 10 Networks

# Example output:
# "Networks": {
#     "finance-network": {
#         ...
#     }
# }
```

Note the network name (e.g., `finance-network`).

---

### 2. Update docker-compose.prod.yml

Edit `docker-compose.prod.yml` and replace `finance-network` with your actual network name:

```yaml
services:
  entsoe-ote-data-uploader:
    networks:
      - your-actual-network-name  # Line 10

networks:
  your-actual-network-name:        # Line 16
    external: true
```

---

### 3. Create Production .env File

```bash
# Copy the template
cp .env.production.example .env

# Edit the file
nano .env
```

Set `DB_HOST` to your PostgreSQL container name:

```bash
DB_HOST=postgres              # Or your actual postgres container name
DB_USER=user_finance
DB_PASSWORD=A9DtRovze2dtjdUMZcgYRjNymdEY9qSH
DB_NAME=finance
DB_PORT=5432
```

**How to find your postgres container name:**
```bash
docker ps | grep postgres
```

---

### 4. Deploy

```bash
# Build and start
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build

# Verify running
docker ps | grep entsoe-ote-data-uploader

# Check logs
docker logs entsoe-ote-data-uploader
```

---

### 5. Verify Database Connection

```bash
# Test DNS resolution
docker exec entsoe-ote-data-uploader getent hosts postgres

# Should output something like:
# 172.18.0.2    postgres
```

---

### 6. Test Upload Manually

```bash
# Run a manual test
docker exec entsoe-ote-data-uploader bash -c "export \$(cat /etc/environment_for_cron | xargs) && /usr/local/bin/python3 /app/scripts/download_day_ahead_prices.py"

# Check logs
tail -f logs/cron.log
```

---

### 7. Set Production Crontab Schedule

Edit the `crontab` file to set your desired production schedule:

```bash
nano crontab
```

Example production schedule:

```cron
# Download day-ahead prices daily at 13:00
0 13 * * * export $(cat /etc/environment_for_cron | xargs) && cd /app/scripts && /usr/local/bin/python3 download_day_ahead_prices.py >> /var/log/cron.log 2>&1

# Download imbalance prices daily at 13:05
5 13 * * * export $(cat /etc/environment_for_cron | xargs) && cd /app/scripts && /usr/local/bin/python3 download_imbalance_prices.py >> /var/log/cron.log 2>&1

# Download intraday prices daily at 13:10
10 13 * * * export $(cat /etc/environment_for_cron | xargs) && cd /app/scripts && /usr/local/bin/python3 download_intraday_prices.py >> /var/log/cron.log 2>&1
```

Then reload:

```bash
./reload-crontab.sh
```

---

## Quick Reference Commands

```bash
# View logs
docker logs -f entsoe-ote-data-uploader
tail -f logs/cron.log

# Reload crontab after changes
./reload-crontab.sh

# Restart container
docker-compose restart

# Stop
docker-compose down

# View crontab
docker exec entsoe-ote-data-uploader crontab -l

# Manual test download
docker exec entsoe-ote-data-uploader /usr/local/bin/python3 /app/scripts/download_day_ahead_prices.py

# Manual test upload
docker exec entsoe-ote-data-uploader bash -c "export \$(cat /etc/environment_for_cron | xargs) && /usr/local/bin/python3 /app/scripts/upload_day_ahead_prices.py 2025/11"
```

---

## Troubleshooting

**Container can't connect to database:**
```bash
# 1. Check both containers are on same network
docker inspect entsoe-ote-data-uploader | grep -A 10 Networks
docker inspect <postgres-container> | grep -A 10 Networks

# 2. Test DNS resolution
docker exec entsoe-ote-data-uploader getent hosts <DB_HOST>

# 3. Check environment variables
docker exec entsoe-ote-data-uploader cat /etc/environment_for_cron
```

**Crontab not running:**
```bash
# Check crontab is loaded
docker exec entsoe-ote-data-uploader crontab -l

# Reload crontab
./reload-crontab.sh

# Check cron logs
tail -f logs/cron.log
```

**Environment variables not found:**
```bash
# Verify env file exists
docker exec entsoe-ote-data-uploader cat /etc/environment_for_cron

# Should show:
# DB_HOST=postgres
# DB_USER=user_finance
# DB_PASSWORD=...
# etc.
```

---

## Key Differences from Local Development

| Aspect | Local (Dev) | Production |
|--------|-------------|------------|
| **DB_HOST** | `james` (via Tailscale) | `postgres` (container name) |
| **Network** | `extra_hosts` mapping | Docker network |
| **Deploy Command** | `docker-compose up -d` | `docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d` |
| **Connection** | Tailscale IP (100.79.143.77) | Docker internal DNS |

---

## Files to Transfer to Production

```
.
├── app/                      # All Python scripts
├── docker-compose.yml        # Base configuration
├── docker-compose.prod.yml   # Production overrides
├── Dockerfile
├── entrypoint.sh
├── requirements.txt
├── crontab                   # Edit schedule as needed
├── reload-crontab.sh
└── .env                      # Create from .env.production.example
```

**Do NOT transfer:**
- `.env` from local (has james/Tailscale config)
- `logs/` directory (will be created)
- `app/2025/` directories with downloaded files (optional)

---

## First Time Setup Checklist

- [ ] Find PostgreSQL container network name
- [ ] Update `docker-compose.prod.yml` with network name
- [ ] Create `.env` from `.env.production.example`
- [ ] Set `DB_HOST` to PostgreSQL container name in `.env`
- [ ] Deploy: `docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build`
- [ ] Test database connection: `docker exec entsoe-ote-data-uploader getent hosts postgres`
- [ ] Run manual test download
- [ ] Edit `crontab` for production schedule
- [ ] Reload crontab: `./reload-crontab.sh`
- [ ] Monitor first cron run: `tail -f logs/cron.log`

---

## Support

For detailed deployment information, see [DEPLOYMENT.md](DEPLOYMENT.md)

For general usage, see [README.md](README.md)
