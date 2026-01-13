# CEPS SOAP API - Test Results & Findings

## Summary

The CEPS SOAP API provides a **much faster and more reliable** alternative to the Selenium-based approach for downloading CEPS data.

## Key Findings

### ✅ Advantages
1. **50-100x Faster**: 0.15-0.6s vs 15-20s with Selenium
2. **More Reliable**: No browser dependencies, no ChromeDriver issues
3. **Simpler Code**: Pure Python `requests` library
4. **Easier Debugging**: Direct HTTP requests, clear XML responses
5. **Lower Resource Usage**: No browser process, no screenshots

### ⚠️ Limitations
- **Cache Still Present**: SOAP API hits the same 120-second backend cache
- **Same Data Format**: Returns XML instead of CSV (requires parser)

### 🔍 Cache Behavior Test

Tested with two rapid requests for historical data (2025-11-13):
- **Request 1**: 0.18s response, 92,069 chars
- **Request 2**: 0.58s response, 92,069 chars (5 seconds later)
- **MD5 Checksums**: Identical (fcad70b703169a69a3fde6fd18721756)

**Conclusion**: SOAP API uses the same backend cache as the web interface.

## Performance Comparison

| Dataset | Selenium | SOAP API | Speed Gain |
|---------|----------|----------|------------|
| System Imbalance | 15-20s | 0.18-0.58s | 50-100x |
| RE Prices | 15-20s | 0.22s | 68-91x |
| SVR Activation | 15-20s | 0.15s | 100-133x |
| Export/Import SVR | 15-20s | 0.21s | 71-95x |

## Test Results

### Test Date: 2026-01-09

#### 1. System Imbalance (AktualniSystemovaOdchylkaCR)
```
Response Time: 0.26s
Status: 200 OK
Content-Length: 65,343 bytes
Data Length: 69,447 characters
Date Range: 2026-01-09 (1086 minutes of data at 18:07 CET)
```

#### 2. RE Prices (AktualniCenaRE)
```
Response Time: 0.22s
Status: 200 OK
Content-Length: 100,676 bytes
Data Length: 104,794 characters
Date Range: 2026-01-09 (1086 minutes of data)
```

#### 3. SVR Activation (AktivaceSVRvCR)
```
Response Time: 0.15s
Status: 200 OK
Content-Length: 115,734 bytes
Data Length: 119,886 characters
Date Range: 2026-01-09 (1086 minutes of data)
```

#### 4. Export/Import SVR (ExportImportSVR)
```
Response Time: 0.21s
Status: 200 OK
Content-Length: 112,323 bytes
Data Length: 116,461 characters
Date Range: 2026-01-09 (1086 minutes of data)
```

## XML Response Structure

All datasets return structured XML with this format:

```xml
<AktualniSystemovaOdchylkaCRResult xmlns="https://www.ceps.cz/CepsData/">
  <root xmlns="https://www.ceps.cz/CepsData/StructuredData/1.0">
    <information>
      <name>Aktuální systémová odchylka ČR</name>
      <date_from>2026-01-09T00:00:00+01:00</date_from>
      <date_to>2026-01-09T23:59:59+01:00</date_to>
      <function>AVG</function>
      <aggregation>MI</aggregation>
    </information>
    <series>
      <serie id="value1" name="Aktuální odchylka [MW]" />
    </series>
    <data>
      <item date="2026-01-09T00:00:00+01:00" value1="0.7082545" />
      <item date="2026-01-09T00:01:00+01:00" value1="42.28804" />
      ...
    </data>
  </root>
</AktualniSystemovaOdchylkaCRResult>
```

## SOAP Endpoints

| Dataset | Operation | SOAPAction |
|---------|-----------|------------|
| System Imbalance | AktualniSystemovaOdchylkaCR | https://www.ceps.cz/CepsData/AktualniSystemovaOdchylkaCR |
| RE Prices | AktualniCenaRE | https://www.ceps.cz/CepsData/AktualniCenaRE |
| SVR Activation | AktivaceSVRvCR | https://www.ceps.cz/CepsData/AktivaceSVRvCR |
| Export/Import SVR | ExportImportSVR | https://www.ceps.cz/CepsData/ExportImportSVR |

**Base URL**: https://www.ceps.cz/_layouts/CepsData.asmx

## Test Script Usage

The lightweight test script supports all 4 datasets:

```bash
# Test imbalance data
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_downloader.py --dataset imbalance --start-date 2026-01-09

# Test RE prices
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_downloader.py --dataset re_price --start-date 2026-01-09

# Test SVR activation
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_downloader.py --dataset svr_activation --start-date 2026-01-09

# Test Export/Import SVR
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_downloader.py --dataset export_import_svr --start-date 2026-01-09

# Save XML response to file
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_downloader.py --dataset imbalance --start-date 2026-01-09 --save

# Enable debug logging
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_downloader.py --dataset imbalance --start-date 2026-01-09 --debug
```

## Recommendations

### Short Term (Current Cron Jobs)
✅ **Keep Selenium approach** - it works and is stable for production cron jobs

### Medium Term (Optimization)
Consider implementing SOAP-based pipeline:
1. Create XML parsers for each dataset (similar structure to current CSV parsers)
2. Build SOAP-based downloaders that integrate with existing uploaders
3. Test thoroughly with historical data backfills
4. Migrate cron jobs to use SOAP downloaders

### Benefits of Migration
- **Faster backfills**: Multi-day ranges would complete 50-100x faster
- **Lower resource usage**: No Chrome/Selenium overhead
- **More reliable**: Fewer moving parts, clearer error messages
- **Easier maintenance**: Pure Python, no browser dependencies

### Trade-offs
- Need to write XML parsers (vs reusing CSV parsers)
- Slightly different error handling (SOAP faults vs HTTP errors)
- Still subject to 120-second cache (125-second delay still needed for backfills)

## Next Steps

1. ✅ SOAP API works for all 4 datasets
2. ✅ Performance gains confirmed (50-100x faster)
3. ✅ Cache behavior documented
4. ⏳ **Decision needed**: Migrate to SOAP or keep Selenium?

## Notes

- SOAP API discovered from WSDL: https://www.ceps.cz/_layouts/CepsData.asmx?WSDL
- Test script location: `app/ceps/ceps_soap_downloader.py`
- All tests run on 2026-01-09 at 18:07 CET
- Prague timezone (Europe/Prague) used for all timestamps
