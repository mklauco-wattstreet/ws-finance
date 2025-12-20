# OTE Portal Deployment Test Procedure

## Test Commands - Execute in Order

### 1. Rebuild Docker Container with Updated Crontab
```bash
docker-compose down
docker-compose up -d --build
```

### 2. Verify Container is Running
```bash
docker ps | grep entsoe-ote-data-uploader
```

### 3. Check Environment Variables are Set
```bash
docker exec entsoe-ote-data-uploader env | grep OTE
```
Expected output should show:
- OTE_CERT_PATH
- OTE_CERT_PASSWORD
- OTE_LOCAL_STORAGE_PASSWORD

### 4. Initial Certificate Setup
```bash
# First time only - imports certificate into browser profile
docker exec entsoe-ote-data-uploader python3 /app/scripts/ote_production.py --setup
```

### 5. Test Login with Test Script
```bash
# Method 1: Quick test with screenshots
./TEST_OTE_LOGIN.sh

# Method 2: Manual test
docker exec entsoe-ote-data-uploader python3 /app/scripts/ote_test_login.py

# Copy screenshots to review
docker cp entsoe-ote-data-uploader:/var/log/. logs/
ls -la logs/screenshot_*.png
```

### 6. Manual Test Run of Production Script
```bash
# Test full download AND upload process
docker exec entsoe-ote-data-uploader python3 /app/scripts/ote_production.py
```

### 7. Check Downloaded Files
```bash
# Check if XML files were downloaded
ls -la ote_files/$(date +%Y)/$(date +%m)/
```

### 8. Verify Database Upload
```bash
# Check database for uploaded records (requires psql access)
# Replace with your database access method
docker exec entsoe-ote-data-uploader python3 -c "
import psycopg2
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT
conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=DB_PORT)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM ote_daily_payments')
print(f'Total records in ote_daily_payments: {cur.fetchone()[0]}')
cur.execute('SELECT MAX(delivery_day) FROM ote_daily_payments')
print(f'Latest delivery day: {cur.fetchone()[0]}')
conn.close()
"
```

### 9. Test Upload Script Separately (Optional)
```bash
# Test upload script with existing XML file
docker exec entsoe-ote-data-uploader python3 /app/scripts/ote_upload_daily_payments.py /app/ote_files/2025/11/daily_payments_YYYYMMDD_HHMMSS.xml
```

### 10. Verify Crontab is Installed
```bash
docker exec entsoe-ote-data-uploader crontab -l | grep ote_production
```

### 11. Check Logs
```bash
# View recent logs
tail -n 100 logs/cron.log
```

### 12. Test Cron Execution (Optional - Quick Test)
```bash
# Add temporary test cron that runs in 1 minute
current_minute=$(date +%M)
next_minute=$(( (current_minute + 2) % 60 ))
docker exec entsoe-ote-data-uploader bash -c "crontab -l > /tmp/cron_test && echo '$next_minute * * * * export \$(cat /etc/environment_for_cron | xargs) && cd /app/scripts && /usr/local/bin/python3 ote_production.py >> /var/log/cron.log 2>&1' >> /tmp/cron_test && crontab /tmp/cron_test"

# Wait 2-3 minutes then check logs
sleep 180
tail -n 50 logs/cron.log

# Remove test cron and restore original
docker exec entsoe-ote-data-uploader crontab /etc/cron.d/entsoe-ote-cron
```

## Troubleshooting Commands

### Reset Certificate (if needed)
```bash
# Clear browser profile and re-setup
rm -rf browser-profile/*
docker exec entsoe-ote-data-uploader python3 /app/scripts/ote_production.py --setup
docker exec entsoe-ote-data-uploader python3 /app/scripts/ote_test_login.py
```

### Debug Mode Run
```bash
docker exec entsoe-ote-data-uploader python3 /app/scripts/ote_production.py --debug
```

### Check Container Logs
```bash
docker logs entsoe-ote-data-uploader --tail 100
```

### Interactive Shell
```bash
docker exec -it entsoe-ote-data-uploader bash
```

## Expected Results

1. **Certificate Setup**: Should show "✓ Certificate imported successfully"
2. **Login Test**: Should show "TEST RESULT: PASSED ✓"
3. **Production Run**: Should download XML files to `ote_files/{year}/{month}/`
4. **Crontab**: Should show entry for 09:00 daily execution

## Success Criteria

- [ ] Certificate imports without errors
- [ ] Login test passes
- [ ] Manual production run downloads XML files
- [ ] Files appear in `ote_files/` directory
- [ ] Crontab entry is active
- [ ] No errors in logs/cron.log