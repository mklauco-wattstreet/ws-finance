# CEPS Data Downloader Guide

## Data Identifications

link | tag | default time unit [minutes] | downloadable | implementation status
| --- | --- | --- | --- | --- |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | AktualniSystemovaOdchylkaCR | 1 | TRUE | ✅ Implemented |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | AktualniCenaRE | 1 | TRUE | ✅ Implemented |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | Load | 1 | TRUE | ⏳ Pending |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | PowerBalance | 1 | TRUE | ⏳ Pending |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | CrossborderPowerFlows | 1 | TRUE | ⏳ Pending |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | EmergencyExchange | 1 | TRUE | ⏳ Pending |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | GenerationPlan | 1 | TRUE | ⏳ Pending |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | Generation | 1 | TRUE | ⏳ Pending |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | GenerationRES | 1 | TRUE | ⏳ Pending |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | OdhadovanaCenaOdchylky | 1 | TRUE | ⏳ Pending |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | NepredvidatelneOdmitnuteNabidky | 1 | TRUE | ⏳ Pending |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | AktivaceSVRvCR | 1 | TRUE | ✅ Implemented |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | Frekvence | 1 | TRUE | ⏳ Pending |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | MaximalniCenySVRnaDT | 1 | TRUE | ⏳ Pending |
| [https://www.ceps.cz/cs/data](https://www.ceps.cz/cs/data) | Emise | 1 | TRUE | ⏳ Pending |

---

## CRITICAL: How to Build New CEPS Downloaders

### The Working Approach (Selenium + JavaScript Monkey-Patching)

After extensive debugging of `AktualniSystemovaOdchylkaCR` (imbalance) and `AktualniCenaRE` (RE prices), we discovered the **only reliable approach** for downloading CEPS data programmatically.

### ✅ Key Requirements (DO NOT SKIP THESE!)

1. **MUST include anchor in URL**: `https://www.ceps.cz/cs/data#<DATA_TAG>`
   - Example: `https://www.ceps.cz/cs/data#AktualniCenaRE`
   - The anchor triggers correct page initialization for that dataset
   - WITHOUT anchor → page loads default/cached dataset

2. **MUST use fresh browser instance per download**
   - CEPS website has severe caching issues
   - Reusing browser = returns cached data from previous request
   - Solution: `driver.quit()` and create new instance for each date

3. **MUST monkey-patch `serializeFilters()` function**
   - Force BOTH dates AND method into AJAX request
   - CEPS page initialization overwrites our parameters otherwise

4. **MUST wait 5 seconds after page load**
   - Page needs time to complete initialization AJAX calls
   - Too short = our JavaScript runs before page is ready

5. **MUST validate downloaded data**
   - Check if CSV contains requested date
   - Delete file if validation fails
   - CEPS often returns wrong dates even when request succeeds

### 📋 Step-by-Step Implementation

Use `app/ceps/ceps_hybrid_downloader.py` or `app/ceps/ceps_re_price_downloader.py` as templates.

```python
def download_ceps_data(driver, data_tag, start_date, end_date, logger):
    """
    Template for CEPS data downloader.

    Args:
        driver: Selenium WebDriver instance
        data_tag: CEPS data tag (e.g., "Load", "Generation")
        start_date: Start datetime
        end_date: End datetime
        logger: Logger instance
    """

    # Graph ID - ALL datasets use 1040
    graph_id = 1040

    # Step 1: Clean download directory
    download_dir = Path("/app/downloads/ceps")
    for old_file in download_dir.glob("*.csv"):
        old_file.unlink()

    # Step 2: Clear session and navigate WITH ANCHOR
    driver.delete_all_cookies()
    driver.execute_script("window.localStorage.clear();")
    driver.execute_script("window.sessionStorage.clear();")

    cache_buster = int(time.time() * 1000)
    url = f"https://www.ceps.cz/cs/data?_cb={cache_buster}#{data_tag}"  # ← ANCHOR!
    driver.get(url)
    time.sleep(2)

    # Clear storage and reload
    driver.execute_script("window.localStorage.clear();")
    driver.execute_script("window.sessionStorage.clear();")

    cache_buster = int(time.time() * 1000)
    url = f"https://www.ceps.cz/cs/data?_cb={cache_buster}#{data_tag}"  # ← ANCHOR!
    driver.get(url)
    time.sleep(3)

    # Step 3: Accept cookies
    try:
        cookie_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "c-p-bn"))
        )
        driver.execute_script("arguments[0].click();", cookie_button)
        time.sleep(1)
    except:
        pass

    # Step 4: Wait for page initialization (CRITICAL!)
    time.sleep(5)  # ← DO NOT REDUCE THIS!

    # Step 5: Monkey-patch serializeFilters to force our parameters
    date_from_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    date_to_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

    js_code = f"""
    console.log('Monkey-patching serializeFilters...');

    var originalSerializeFilters = serializeFilters;

    var forcedDateFrom = "{date_from_str}".replace(" ", "T");
    var forcedDateTo = "{date_to_str}".replace(" ", "T");
    var forcedMethod = "{data_tag}";

    window.serializeFilters = function(filters, others) {{
        var result = originalSerializeFilters(filters, others);

        // FORCE our parameters
        result.date_from = forcedDateFrom;
        result.date_to = forcedDateTo;
        result.method = forcedMethod;  // ← CRITICAL: Force method!

        console.log('Forced params:', result);
        return result;
    }};

    // Build filter_data
    var filter_data = {{
        dateFrom: "{date_from_str}",
        dateTo: "{date_to_str}",
        dateType: "day",
        agregation: "MI",
        interval: "false",
        version: "RT",
        function: "AVG"
    }};

    // Call filterData with download="csv"
    filterData(filter_data, "{data_tag}", "day", "", "csv");

    return 'SUCCESS';
    """

    result = driver.execute_script(js_code)

    # Step 6: Wait for download
    max_wait = 30
    for i in range(max_wait):
        time.sleep(1)
        csv_files = list(download_dir.glob("*.csv"))
        if csv_files:
            latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)

            # Step 7: VALIDATE downloaded data
            with open(latest_file, 'r', encoding='utf-8') as f:
                f.readline()  # header
                metadata = f.readline().strip()

                date_check = start_date.strftime("%d.%m.%Y")
                if date_check not in metadata:
                    logger.error(f"VALIDATION FAILED: Expected {date_check}, got {metadata}")
                    latest_file.unlink()
                    return None

            return latest_file

    return None
```

### 🚫 Common Pitfalls (Things That DON'T Work)

1. ❌ **Direct AJAX with `requests` library**
   - Requires browser JavaScript context
   - Returns HTML instead of CSV

2. ❌ **Navigating to download URL directly**
   - `driver.get(f"https://www.ceps.cz/cs/data?do=loadGraphData&method={tag}...")`
   - Returns HTML page, not CSV

3. ❌ **URL without anchor**
   - Page loads default dataset, can't be overridden

4. ❌ **Clicking menu items via JavaScript**
   - Too unreliable, timing issues

5. ❌ **Setting `filter_settings` and `fake_click` before calling `filterData()`**
   - Page initialization AJAX calls overwrite these values
   - Must monkey-patch `serializeFilters()` instead

6. ❌ **Reusing browser instances**
   - CEPS website caches requests
   - Returns previous date's data

### 📊 Multi-Day Downloads

**ALWAYS download day-by-day** with fresh browser instances:

```python
for current_date in date_range:
    driver = init_browser()  # Fresh instance
    try:
        download_ceps_data(driver, data_tag, current_date, current_date, logger)
    finally:
        driver.quit()  # Clean up
    time.sleep(2)  # Delay between downloads
```

See `app/ceps/ceps_runner.py` for reference implementation.

### 🔍 SOAP API (Reference Only - Currently Broken)

CEPS provides a SOAP API at `https://www.ceps.cz/_layouts/CepsData.asmx`:

```xml
<AktualniCenaRE>
  <dateFrom>2025-08-23 00:00:00</dateFrom>
  <dateTo>2025-08-23 23:59:59</dateTo>
  <param1>All</param1>  <!-- All | aFRR | mFFR+ | mFRR- | mFRR5 -->
</AktualniCenaRE>
```

**Status**: Returns `SqlDateTime overflow` errors for all dates/methods. Not usable.

### 📝 Implementation Checklist

When creating a new CEPS downloader:

- [ ] Use anchor in URL: `#{DATA_TAG}`
- [ ] Fresh browser per download
- [ ] Wait 5 seconds after page load
- [ ] Monkey-patch `serializeFilters()` with dates + method
- [ ] Validate downloaded CSV dates
- [ ] Delete file if validation fails
- [ ] Handle download day-by-day for date ranges
- [ ] Add database tables (1min + 15min aggregated)
- [ ] Create uploader with UPSERT logic
- [ ] Integrate with `ceps_runner.py`

### 📚 Reference Implementations

- **Imbalance**: `app/ceps/ceps_hybrid_downloader.py` + `app/ceps/ceps_uploader.py`
- **RE Prices**: `app/ceps/ceps_re_price_downloader.py` + `app/ceps/ceps_re_price_uploader.py`
- **SVR Activation**: `app/ceps/ceps_svr_activation_downloader.py` + `app/ceps/ceps_svr_activation_uploader.py`
- **Runner**: `app/ceps/ceps_runner.py` (day-by-day orchestration)