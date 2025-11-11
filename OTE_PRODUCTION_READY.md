# OTE Portal Download - Production Ready ✅

## What We Have Now

### Single Script
- **`app/ote_final.py`** - The ONLY script that handles everything

### Features
✅ Language detection and switching
✅ Certificate-based login
✅ Date handling with Ctrl+A method
✅ XML export selection
✅ Screenshot after each action
✅ Automatic cleanup of old screenshots
✅ Complete error handling

## Production Usage

### Manual Run:
```bash
./ote_download.sh
```

### Add to Crontab (Daily at 09:00):
```bash
# Edit crontab
crontab -e

# Add this line for daily execution at 09:00
0 9 * * * /path/to/ws-finance/ote_download.sh >> /var/log/ote_download.log 2>&1
```

### Or Add to Docker Container's Crontab:
```bash
# Inside the container
docker exec python-cron-scheduler bash -c "echo '0 9 * * * cd /app/scripts && python3 ote_final.py --debug >> /var/log/ote_download.log 2>&1' | crontab -"

# Verify
docker exec python-cron-scheduler crontab -l
```

## What Happens Each Run

1. **Cleanup** - Deletes all old screenshots
2. **Login** - Uses certificate authentication
3. **Navigate** - Goes to Settlement > Report > Daily Payments
4. **Set Dates** - Sets date range (3 days ago to yesterday)
5. **Retrieve** - Fetches data
6. **Download** - Exports as XML
7. **Save** - Stores in `/app/ote_files/YYYY/MM/`
8. **Logout** - Clean exit

## Screenshots

Screenshots are saved with timestamps in `/var/log/`:
- `screenshot_HHMMSS_after_login_button.png`
- `screenshot_HHMMSS_daily_payments_page.png`
- `screenshot_HHMMSS_dates_set.png`
- `screenshot_HHMMSS_after_retrieve.png`
- `screenshot_HHMMSS_download_dialog_opened.png`
- `screenshot_HHMMSS_xml_selected.png`
- `screenshot_HHMMSS_after_export_click.png`

To retrieve screenshots after a run:
```bash
docker cp python-cron-scheduler:/var/log/screenshot_*.png ./logs/
```

## Files Structure

```
/app/
├── scripts/
│   └── ote_final.py          # The downloader script
├── downloads/                 # Temporary download location
├── ote_files/                 # Final storage
│   └── 2025/
│       └── 11/
│           └── daily_payments_*.xml
└── browser-profile/           # Certificate storage
```

## Troubleshooting

### If Login Fails:
```bash
# Reset browser profile
docker exec python-cron-scheduler rm -rf /app/browser-profile/*

# Re-run (will re-import certificate)
./ote_download.sh
```

### Check Logs:
```bash
# Container logs
docker logs python-cron-scheduler --tail 100

# Script output (if using crontab)
tail -f /var/log/ote_download.log
```

### Get Screenshots:
```bash
mkdir -p logs
docker cp python-cron-scheduler:/var/log/screenshot_*.png logs/
```

## Environment Variables Required

In `.env` file:
```
OTE_CERT_PATH=/app/certs/Klauco_1.p12
OTE_CERT_PASSWORD=your_cert_password
OTE_LOCAL_STORAGE_PASSWORD=your_storage_password
```

## That's It! 🎉

Just one script (`ote_final.py`) and one command (`./ote_download.sh`).

Ready for production use with daily crontab execution.