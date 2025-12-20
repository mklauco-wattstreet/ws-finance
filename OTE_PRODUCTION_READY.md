# OTE Portal Production Deployment

## Overview
Automated daily downloader for OTE Czech electricity market settlement reports using Docker container.

## Scripts

- **`app/ote_production.py`** - Streamlined production script (download + upload)
- **`app/ote_upload_daily_payments.py`** - Database upload script
- **`app/ote_test_login.py`** - Certificate verification test
- **`crontab`** - Updated with OTE entry for 09:00 daily

## Initial Setup

### 1. Environment Configuration
Ensure `.env` file contains:
```bash
OTE_CERT_PATH=/app/certs/certificate.p12
OTE_CERT_PASSWORD=your_cert_password
OTE_LOCAL_STORAGE_PASSWORD=your_storage_password
```

### 2. Deploy Container
```bash
docker-compose down
docker-compose up -d --build
```

### 3. Certificate Installation
Run once to import certificate:
```bash
docker exec entsoe-ote-data-uploader python3 /app/scripts/ote_production.py --setup
```

### 4. Verify Setup
```bash
docker exec entsoe-ote-data-uploader python3 /app/scripts/ote_test_login.py
```

## Production Deployment

The crontab is automatically installed when container starts.
Entry runs daily at 09:00 Europe/Prague time.

### Verify Crontab
```bash
docker exec entsoe-ote-data-uploader crontab -l | grep ote
```

## Daily Operation

The script runs automatically at 09:00 daily and:
1. Logs into OTE portal using certificate
2. Downloads last 7 days of payment data (XML format)
3. Saves XML files to `/app/ote_files/{year}/{month}/`
4. **Automatically uploads data to `daily_payments` database table**
5. Logs all operations to `/var/log/cron.log`

The upload process handles:
- Duplicate detection (skips already uploaded records)
- Data validation and type conversion
- Detailed logging of inserted/skipped records

## Manual Execution

```bash
# Standard run
python3 /app/ote_production.py

# Debug mode
python3 /app/ote_production.py --debug

# Certificate re-setup
rm /app/browser-profile/.cert_imported
python3 /app/ote_production.py --setup
```

## File Structure

```
/app/
├── ote_production.py          # Main production script
├── ote_test_login.py          # Login test script
├── downloads/                 # Temporary download folder
├── ote_files/                 # Archived XML files
│   └── {year}/
│       └── {month}/
│           └── ote_daily_payments_{timestamp}.xml
└── browser-profile/           # Chrome profile with certificate
    └── .cert_imported         # Certificate setup flag
```

## Monitoring

### Check Logs
```bash
tail -f /var/log/ote_download.log
```

### Verify Downloads
```bash
ls -la /app/ote_files/$(date +%Y)/$(date +%m)/
```

### Monitor Cron
```bash
grep CRON /var/log/syslog | grep ote_production
```

## Troubleshooting

### Login Issues
```bash
# Reset and re-setup certificate
rm -rf /app/browser-profile/*
python3 /app/ote_production.py --setup
python3 /app/ote_test_login.py
```

### No Data Available
- OTE publishes data next business day
- Weekend/holiday data may be delayed
- Script downloads last 7 days to handle gaps

### Browser Errors
```bash
# Check ChromeDriver compatibility
chromium --version
chromedriver --version

# Clear download folder
rm /app/downloads/*.xml
```

## Security

- Credentials stored in environment variables
- Browser profile contains auth tokens - restrict access:
```bash
chmod 600 .env
chmod 700 /app/browser-profile
```

## Exit Codes

- 0: Success
- 1: Error occurred
- 130: User interrupt

## Support

Check `/var/log/ote_download.log` for detailed error messages.
The production script provides clear status for each operation stage.