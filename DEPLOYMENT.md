# Deployment Guide

## Local Development (with Tailscale)

The current setup is configured for local development where the database "james" is accessed via Tailscale.

**Start the container:**
```bash
docker-compose up -d
```

**Configuration:**
- `.env`: `DB_HOST=james`
- `docker-compose.yml`: Has `extra_hosts` mapping james to Tailscale IP (100.79.143.77)

---

## Production Deployment (with Docker Database)

When deploying to production where the database runs as a Docker container, use Docker networking.

### Step-by-Step Production Deployment

**1. Find your database container's network:**
```bash
docker inspect <your-database-container-name> | grep NetworkMode
# OR
docker inspect <your-database-container-name> | grep -A 10 Networks
```

**2. Update the network name in `docker-compose.prod.yml`:**

If your database is on a network called `my-db-network`, update line 10:
```yaml
networks:
  - my-db-network
```

And line 16-17:
```yaml
networks:
  my-db-network:
    external: true
```

**3. Create production .env file:**
```bash
# Copy the example
cp .env.production.example .env

# Edit .env and set DB_HOST to your database container name
# For example, if your PostgreSQL container is named "postgres":
DB_HOST=postgres

# Or if it's named something else like "finance-db":
DB_HOST=finance-db
```

**4. Deploy:**
```bash
# Build and start the container
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build

# Verify it's running
docker ps | grep python-cron

# Check logs
docker logs python-cron-scheduler
```

**5. Test the connection:**
```bash
# Check if container can reach database
docker exec python-cron-scheduler getent hosts <DB_HOST>

# Run a manual test
docker exec python-cron-scheduler /usr/local/bin/python3 /app/scripts/download_day_ahead_prices.py
```

**6. Update crontab schedule:**

Edit `crontab` file to set production schedule, then:
```bash
./reload-crontab.sh
```

---

## Updating Crontab Without Restart

After any crontab changes:
```bash
./reload-crontab.sh
```

This reloads the crontab in the running container without rebuilding or restarting.

---

## Notes

- The `app/` directory is mounted as a volume, so Python script changes take effect immediately
- The `.env` file is loaded by docker-compose, not mounted in the container
- Environment variables are dumped to `/etc/environment_for_cron` at container startup for cron jobs to use
