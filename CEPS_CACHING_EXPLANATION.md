# CEPS Caching Problem - Technical Explanation

## What We're Actually Doing

### Step-by-Step Process:

1. **Selenium opens Chromium browser** (headless)
   - Fresh browser instance every time
   - No persistent profile
   - Download directory: `/app/downloads/ceps`

2. **Navigate to CEPS page WITH ANCHOR**
   ```
   https://www.ceps.cz/cs/data#AktualniCenaRE
   ```
   - `#AktualniCenaRE` = anchor to trigger dataset initialization

3. **Clear browser storage**
   ```javascript
   driver.delete_all_cookies()
   window.localStorage.clear()
   window.sessionStorage.clear()
   ```

4. **Wait 5 seconds** for page initialization
   - CEPS page loads JavaScript
   - Makes AJAX call to initialize dataset

5. **Inject JavaScript - Monkey-patch `serializeFilters()`**
   ```javascript
   // Save original function
   var originalSerializeFilters = serializeFilters;

   // Replace with our version
   window.serializeFilters = function(filters, others) {
       var result = originalSerializeFilters(filters, others);

       // FORCE our parameters
       result.date_from = "2025-12-15T00:00:00";
       result.date_to = "2025-12-15T23:59:59";
       result.method = "AktualniCenaRE";

       return result;
   };
   ```

6. **Call website's `filterData()` function with TXT format**
   ```javascript
   filterData(filter_data, "AktualniCenaRE", "day", "", "txt");
   ```
   - This triggers the website's own AJAX call
   - AJAX endpoint: `https://www.ceps.cz/cs/data?do=loadGraphData`
   - Parameters sent: date_from, date_to, method, download=txt
   - **Why TXT?** Same format as CSV but may have less aggressive caching

7. **Website makes AJAX request** (we don't control this)
   - The website's JavaScript makes the actual HTTP request
   - Server responds with `Cache-Control: public, max-age=120`
   - We just wait for the TXT file to appear in downloads

8. **Wait for TXT file to download**
   - Check `/app/downloads/ceps/*.txt` every second
   - Max wait: 30 seconds
   - Rename `.txt` → `.csv` automatically (same format)

9. **Validate downloaded data**
   - Check if CSV contains requested date
   - If wrong date → DELETE file and return error

---

## What Causes The Caching?

### SERVER-SIDE CACHING on CEPS infrastructure

Even though we:
- ✅ Use fresh browser instances
- ✅ Clear all cookies/localStorage/sessionStorage
- ✅ Monkey-patch to force our dates
- ✅ Quit browser between requests

**The CEPS server STILL caches responses based on:**

1. **Time Window (CONFIRMED: 120 seconds)**
   - Server sends: `Cache-Control: public, max-age=120`
   - Requests within 120 seconds get cached data
   - After 120 seconds, cache expires and fresh data is served

2. **IP Address**
   - Server may cache based on source IP
   - All our requests come from same Docker container IP

3. **PHP Session ID (PHPSESSID cookie)**
   - Server associates requests with session
   - Even with cleared cookies, new session gets cached response

4. **Dataset Type**
   - Server may have separate caches per dataset type
   - But still returns wrong dates within cache window

---

## Why It's NOT a Browser Cache

**Evidence:**
- Fresh browser instance = no browser cache
- Cleared localStorage/sessionStorage = no local storage
- Cleared cookies = no cookie cache

**But still returns wrong dates!**

This proves it's **SERVER-SIDE caching on CEPS**.

---

## What We CANNOT Control

We **CANNOT** control:
- Server-side caching on CEPS infrastructure
- AJAX endpoint caching logic (`Cache-Control: public, max-age=120`)
- PHP session caching
- Server-side cache invalidation

We can ONLY:
- Wait for cache to expire (120 seconds confirmed)
- Download different datasets (may have separate caches)
- Use validation to detect and reject wrong data
- Use TXT format instead of CSV (same format, possibly less cached)

---

## TESTED SOLUTION ✅

### For AUTOMATED CRON (Today's Data):
- Download TODAY's data every 15 minutes
- Cache is NOT a problem for same date (cache HAS the right data)
- 65-second delays between datasets prevent crosstalk
- **Runtime:** ~3.5 minutes per cycle
- **Status:** ✅ WORKS - No cache issues

### For MANUAL BACKFILL (Historical Data):
- **125-second delay** between each date (cache expires at 120s)
- Process day-by-day with countdown progress bar
- TXT format + validation ensures correct data
- **Runtime:** (2 min × days) + (125s × (days-1))
- **Example:** 30 days = ~120 minutes total
- **Status:** ✅ WORKS - Confirmed with Nov 2025 backfill

---

## Why Can't We Use Direct HTTP Requests?

**We tried:**
```python
requests.get(
    'https://www.ceps.cz/cs/data',
    params={'do': 'loadGraphData', 'method': 'AktualniCenaRE', ...}
)
```

**Result:** Returns HTML page, NOT CSV

**Why:** The AJAX endpoint requires:
- Valid PHP session (PHPSESSID)
- Correct Referer header
- JavaScript context
- Some kind of token/state from page initialization

Without the browser JavaScript context, the server refuses to return CSV.

---

## Summary

**What we use:**
- Selenium + JavaScript injection to call website's own AJAX
- TXT format instead of CSV (same structure, auto-renamed)
- Validation to ensure correct dates

**What causes caching:**
- CEPS server-side infrastructure with 120-second cache window
- `Cache-Control: public, max-age=120` response header
- IP-based and PHP session-based caching

**What we can do:**
- ✅ Download TODAY's data every 15 minutes (no cache issues)
- ✅ Wait 125 seconds between historical dates (bypasses 120s cache)
- ✅ Validate and reject wrong data automatically
- ✅ Use countdown progress bar for long backfills

**What we CANNOT do:**
- Bypass server-side cache completely
- Control AJAX endpoint caching behavior
- Use direct HTTP requests (server requires JS context)
- Download historical data faster than ~2 min per day
