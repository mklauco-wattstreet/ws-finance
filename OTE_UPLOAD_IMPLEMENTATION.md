# OTE Daily Payments - Upload Implementation

## Overview
Successfully implemented automatic database upload for OTE Daily Payments data. The system now downloads XML files from OTE portal and automatically uploads them to the PostgreSQL `daily_payments` table.

## New Files Created

### 1. `app/ote_upload_daily_payments.py`
Database upload script that:
- Parses OTE Daily Payments XML files
- Maps XML columns to database fields
- Checks for duplicates before inserting
- Provides detailed logging
- Handles errors gracefully

**Database Table:** `daily_payments`

**Column Mapping:**
- Column 1 → `delivery_day` (DATE)
- Column 2 → `settlement_version` (TEXT)
- Column 3 → `settlement_item` (TEXT)
- Column 4 → `type_of_payment` (TEXT)
- Column 5 → `volume_mwh` (NUMERIC)
- Column 6 → `amount_excl_vat` (NUMERIC)
- Column 7 → `currency_of_payment` (TEXT)
- Column 8 → `currency_rate` (NUMERIC)
- Column 9 → `system` (TEXT)
- Column 10 → `message` (TEXT)

**Duplicate Detection:**
Records are considered duplicates based on:
- `delivery_day`
- `settlement_version`
- `settlement_item`
- `type_of_payment`

## Modified Files

### `app/ote_production.py`
Enhanced to:
- Return file path after successful download (instead of True/False)
- Call upload script automatically after download
- Provide comprehensive logging for both download and upload phases
- Set appropriate exit codes for monitoring

**Workflow:**
1. Download XML file from OTE portal
2. Save to `/app/ote_files/{year}/{month}/daily_payments_{timestamp}.xml`
3. Automatically call `ote_upload_daily_payments.py` with file path
4. Log upload results

## Integration with Docker

### Paths (from `docker-compose.yml`):
- Scripts: `/app/scripts` (mounted from `./app`)
- OTE files: `/app/ote_files` (mounted from `./ote_files`)
- Logs: `/var/log` (mounted from `./logs`)
- Downloads: `/app/downloads` (temporary)

### Crontab Entry:
```
0 09 * * * export $(cat /etc/environment_for_cron | xargs) && cd /app/scripts && /usr/local/bin/python3 ote_production.py >> /var/log/cron.log 2>&1
```

## Testing

### 1. Test Upload Script Independently
```bash
# Find an existing XML file
ls -la ote_files/2025/11/

# Test upload with that file
docker exec python-cron-scheduler python3 /app/scripts/ote_upload_daily_payments.py /app/ote_files/2025/11/daily_payments_YYYYMMDD_HHMMSS.xml
```

### 2. Test Complete Workflow (Download + Upload)
```bash
# Run production script manually
docker exec python-cron-scheduler python3 /app/scripts/ote_production.py

# Check logs for both download and upload
tail -n 100 logs/cron.log
```

### 3. Verify Database Records
```bash
# Check if records were inserted
docker exec python-cron-scheduler python3 -c "
import psycopg2
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT
conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=DB_PORT)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM daily_payments')
print(f'Total records: {cur.fetchone()[0]}')
cur.execute('SELECT delivery_day, settlement_version, type_of_payment, amount_excl_vat FROM daily_payments ORDER BY delivery_day DESC LIMIT 5')
print('\nLatest 5 records:')
for row in cur.fetchall():
    print(row)
conn.close()
"
```

## Log Output Example

### Download Phase:
```
21:04:08 - INFO - ============================================================
21:04:08 - INFO - OTE Portal Production Downloader
21:04:08 - INFO - Date: 2025-11-11 21:04:08
21:04:08 - INFO - ============================================================
21:04:08 - INFO - Starting browser...
21:04:08 - INFO - Logging in...
21:04:08 - INFO - ✓ Login successful
21:04:08 - INFO - Navigating to Daily Payments...
21:04:08 - INFO - Downloading data: 04/11/2025 to 10/11/2025
21:04:08 - INFO - Data loaded, downloading...
21:04:08 - INFO - Exporting...
21:04:08 - INFO - ✓ File saved: /app/ote_files/2025/11/daily_payments_20251111_210420.xml
21:04:08 - INFO - ✓ SUCCESS - Daily Payments downloaded
```

### Upload Phase:
```
21:04:08 - INFO - ============================================================
21:04:08 - INFO - Starting database upload...
21:04:08 - INFO - ============================================================
21:04:08 - INFO - OTE Daily Payments Upload
21:04:08 - INFO - Time: 2025-11-11 21:04:08
21:04:08 - INFO - ============================================================
21:04:08 - INFO - Input file: /app/ote_files/2025/11/daily_payments_20251111_210420.xml
21:04:08 - INFO - File size: 45,678 bytes
21:04:08 - INFO - Parsing XML file: /app/ote_files/2025/11/daily_payments_20251111_210420.xml
21:04:08 - INFO - ✓ Found 142 records in XML file
21:04:08 - INFO - Connecting to database...
21:04:08 - INFO -   Host: james
21:04:08 - INFO -   Database: finance
21:04:08 - INFO -   User: user_finance
21:04:08 - INFO - ✓ Database connection established
21:04:08 - INFO - Processing 142 records:
21:04:08 - INFO - --------------------------------------------------------------------------------
21:04:08 - INFO - Upload Summary:
21:04:08 - INFO -   Total records: 142
21:04:08 - INFO -   ✓ Inserted: 89
21:04:08 - INFO -   ⊘ Skipped (duplicates): 53
21:04:08 - INFO - ✓ Database connection closed
21:04:08 - INFO - ✓ Database upload completed successfully
21:04:08 - INFO - ============================================================
21:04:08 - INFO - ✓ COMPLETE - Download and upload successful
21:04:08 - INFO - ============================================================
```

## Error Handling

### Scenarios Covered:
1. **Download Failure** → Upload is skipped, exit code 1
2. **Upload Failure** → File remains on disk for retry, exit code 1
3. **Duplicate Records** → Skipped gracefully, logged
4. **Database Connection Error** → Clear error message, exit code 1
5. **XML Parsing Error** → Detailed error, exit code 1

### Retry Strategy:
- Downloaded XML files are kept in `/app/ote_files`
- Can be manually re-uploaded using:
  ```bash
  docker exec python-cron-scheduler python3 /app/scripts/ote_upload_daily_payments.py <xml_file_path>
  ```

## Monitoring

### Check Cron Execution:
```bash
# View recent logs
tail -f logs/cron.log

# Check for errors
grep ERROR logs/cron.log
```

### Check Database State:
```sql
-- Total records
SELECT COUNT(*) FROM daily_payments;

-- Latest delivery day
SELECT MAX(delivery_day) FROM daily_payments;

-- Records by type
SELECT type_of_payment, COUNT(*)
FROM daily_payments
GROUP BY type_of_payment;

-- Recent uploads (based on unique combinations)
SELECT delivery_day, COUNT(*) as record_count
FROM daily_payments
GROUP BY delivery_day
ORDER BY delivery_day DESC
LIMIT 7;
```

## Production Checklist

- [x] Upload script created with duplicate detection
- [x] Production script modified to call upload automatically
- [x] Detailed logging implemented
- [x] Error handling for all failure scenarios
- [x] Documentation updated
- [x] Crontab configured for daily execution at 09:00
- [ ] Test with real OTE data
- [ ] Verify database table structure matches
- [ ] Monitor first few cron executions
- [ ] Set up alerting for failures (optional)

## Next Steps

1. **Test the complete workflow:**
   ```bash
   docker exec python-cron-scheduler python3 /app/scripts/ote_production.py
   ```

2. **Verify database records were inserted**

3. **Monitor the next scheduled cron execution** (09:00 daily)

4. **Check logs for any issues:**
   ```bash
   tail -f logs/cron.log
   ```

## Support

For issues:
- Check `/var/log/cron.log` for detailed execution logs
- Review database connection settings in `.env`
- Verify XML file format matches expected structure
- Test upload script independently with known-good XML file