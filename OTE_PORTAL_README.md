# OTE Portal Downloader

## Overview

`app/download_ote_portal.py` is a headless browser automation script that logs into the OTE Portal (https://portal.ote-cr.cz) using client certificate authentication and downloads daily settlement reports in XML format.

## Purpose

Automatically download three types of reports from the OTE Portal:
1. **Daily Payments** (currently being implemented)
2. **Financial Security** (TODO)
3. **Daily Settlement** (TODO)

The script is designed to run via cron daily at 09:00.

## Authentication Method

Uses **client certificate authentication** stored in the browser's local storage:
- Certificate type: `.p12` (PKCS#12) file
- Certificate is imported into the portal's local storage on first setup
- Browser profile is persisted between runs to maintain certificate

## Current Status

### ✅ Working Components

1. **Certificate Setup** (`--setup` mode)
   - Navigates to Certificate settings
   - Sets local storage password
   - Imports P12 certificate into portal's local storage
   - Certificate persists in browser profile

2. **Login Flow**
   - Clicks "Log in" button
   - Enters local storage password
   - Confirms password
   - Selects certificate (if multiple)
   - Signs in using certificate
   - Successfully reaches dashboard

3. **Navigation to Daily Payments**
   - Clicks: Settlement → Report → Daily payments
   - Verifies page loaded by checking for "Table with results"

4. **Logout**
   - Clicks avatar menu
   - Clicks "Logout" option

### ⚠️ Needs Fixing

**Date Selection and Data Retrieval**
- Date fields are React components with datepicker
- Current approach: Setting values via JavaScript and triggering events
- **Problem**: Values may not properly register with the React component
- **Symptom**: After clicking "Retrieve", either no data loads or "No data matches your selected parameters" appears

**Download Button Location**
- After successful data retrieval, need to click download button
- Current selectors may not be finding the correct button

## Configuration

### Environment Variables (.env)

```bash
OTE_CERT_PATH=/app/certs/Klauco_1.p12
OTE_CERT_PASSWORD=_9S4N_EMenUPFtXJ5f_R
OTE_LOCAL_STORAGE_PASSWORD=MWy-iH3/u7-Fvc_C\sV9
```

### Docker Volumes

```yaml
volumes:
  - ./browser-profile:/app/browser-profile  # Persistent browser profile
  - ./downloads:/app/downloads              # Temporary download location
  - ./ote_files:/app/ote_files              # Final file storage
  - ./certs:/app/certs:ro                   # Client certificates
  - ./logs:/var/log                         # Screenshots and logs
```

## Usage

### First Time Setup (Import Certificate)

```bash
docker exec entsoe-ote-data-uploader bash -c "/usr/local/bin/python3 /app/scripts/download_ote_portal.py --setup --debug"
```

This only needs to be run once. The certificate is stored in the browser profile.

### Regular Download

```bash
docker exec entsoe-ote-data-uploader bash -c "/usr/local/bin/python3 /app/scripts/download_ote_portal.py --debug"
```

## Daily Payments Download Flow

### Expected Behavior

1. Login with certificate
2. Navigate: Settlement → Report → Daily payments
3. Wait for page to load (verify "Table with results" present)
4. Set date range:
   - **fromDate**: Today - 3 days
   - **toDate**: Yesterday (today - 1 day)
   - Format: DD/MM/YYYY
5. Click "Retrieve" button
6. Wait 10 seconds for data to load
7. Verify no "No data matches your selected parameters" message
8. Click download button (icon button with download SVG)
9. Select "XML" radio button from export dialog
10. Click "Export" button
11. Wait for download to complete
12. Move file from `/app/downloads` to `/app/ote_files/YYYY/MM/`
13. Logout

### Date Picker Issue

The date inputs are part of a React datepicker component:

```html
<input name="fromDate" placeholder="Od" size="12" autocomplete="off" value="07/11/2025">
<input name="toDate" placeholder="Do" size="12" autocomplete="off" value="09/11/2025">
```

Current code attempts to set values via JavaScript:
```javascript
arguments[0].removeAttribute('readonly');
arguments[0].value = arguments[1];
arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
```

This may not properly trigger the React component's state update.

**Alternative approaches to try:**
1. Click on the date input to open the datepicker calendar, then select dates
2. Use React DevTools to find and trigger the component's internal state setter
3. Clear the field and send keystrokes character-by-character
4. Trigger focus/blur events in addition to input/change

## Debugging

### Screenshots

All steps capture screenshots to `/var/log/screenshot_*.png`:
- `login_01_before_login_button.png`
- `login_02_after_login_button.png`
- `login_07_success.png`
- `daily_payments_01_page.png`
- `daily_payments_01b_dates_set.png`
- `daily_payments_02_after_retrieve.png`
- etc.

### Retrieve Screenshots

```bash
docker cp entsoe-ote-data-uploader:/var/log/screenshot_*.png logs/
```

### Check Logs

```bash
docker logs entsoe-ote-data-uploader
```

## Technical Details

### Browser Configuration

- Browser: Chromium (ARM64 compatible)
- Mode: Headless
- Profile: Persistent at `/app/browser-profile`
- Download directory: `/app/downloads`
- Platform: linux/aarch64 (Apple Silicon M1/M2)

### Key Functions

- `setup_certificate_in_portal()`: Imports certificate (setup mode)
- `login_to_portal()`: Authenticates with certificate
- `download_daily_payments()`: Downloads daily payments report
- `logout_from_portal()`: Logs out after downloads complete
- `take_screenshot()`: Captures screenshots at each step

### File Output

Downloaded files are saved to:
```
/app/ote_files/YYYY/MM/daily_payments_DD-MM-YYYY_to_DD-MM-YYYY.xml
```

Example:
```
/app/ote_files/2025/11/daily_payments_07-11-2025_to_09-11-2025.xml
```

## Known Limitations

1. **Certificate Login Limit**: The OTE Portal limits how many times you can login with a certificate. Excessive logins during testing can temporarily lock the certificate.

2. **Browser Profile Corruption**: If the browser profile gets corrupted, you must delete it and re-run setup (which counts as a login).

3. **React Component Interaction**: The datepicker is a complex React component that may not respond to standard Selenium input methods.

## Next Steps

1. **Fix date selection**: Ensure dates properly register with the React datepicker
2. **Fix download button**: Locate and click the correct download button
3. **Implement Financial Security download**: Similar flow to Daily Payments
4. **Implement Daily Settlement download**: Similar flow to Daily Payments
5. **Add cron schedule**: Configure crontab to run daily at 09:00

## Dependencies

```txt
selenium>=4.15.0
```

Browser binaries (installed in Docker):
- chromium
- chromium-driver
