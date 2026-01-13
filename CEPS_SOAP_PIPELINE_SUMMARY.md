# CEPS SOAP API Pipeline - Implementation Summary

## What Was Implemented

Complete SOAP API pipeline for CEPS data with the following features:

### 1. Core Components

#### `ceps_soap_api_downloader.py`
- Downloads CEPS data via SOAP API
- Automatic chunking for requests > 30 days
- Supports all 4 datasets (imbalance, re_price, svr_activation, export_import_svr)
- Returns parsed XML root elements

#### `ceps_soap_xml_parser.py`
- Parses XML responses from SOAP API
- Converts to structured Python dictionaries
- Handles all 4 dataset formats
- Extracts all data columns with correct types

#### `ceps_soap_uploader.py`
- Uploads parsed data to PostgreSQL
- **UPSERT logic** (INSERT ... ON CONFLICT DO UPDATE)
- Safe for backfilling missing data
- Uses bulk inserts (`execute_values`)

#### `ceps_soap_pipeline.py`
- Complete end-to-end pipeline
- Download → Parse → Upload
- Automatic 30-day chunking
- Supports single dataset or all datasets

### 2. Key Features

✅ **Multi-Day Requests**
- Single SOAP request can fetch multiple days
- Tested up to 30 days in single request (2.6 MB, 1.7s)
- Auto-chunks requests > 30 days

✅ **UPSERT Logic**
- `ON CONFLICT DO UPDATE` ensures idempotent operations
- Safe to re-run for same date range
- Perfect for backfilling missing data

✅ **Performance**
- **50-100x faster** than Selenium (0.2s vs 15-20s per day)
- Single day: ~0.2s
- 7 days: ~0.5s
- 30 days: ~1.7s
- 40 days (2 chunks): ~3s

✅ **No Cache Issues**
- SOAP API uses same 120-second backend cache
- But speed makes it negligible
- No need for 125-second delays between requests

## Test Results

### Single Day (2026-01-09)
```
Response Time: 0.18s
Parsed: 1,105 records
Uploaded: 1,105 records
Status: ✓ SUCCESS
```

### 7 Days (2025-12-25 to 2025-12-31)
```
Response Time: 0.46s
Parsed: 10,080 records (7 × 1,440)
Uploaded: 10,080 records
Status: ✓ SUCCESS
```

### 40 Days with Chunking (2025-11-01 to 2025-12-10)
```
Chunk 1 (30 days): 1.75s → 43,200 records
Chunk 2 (10 days): 0.52s → 14,398 records
Total: 2.27s → 57,598 records
Status: ✓ SUCCESS
```

## Usage Examples

### Basic Usage

```bash
# Single day, single dataset
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset imbalance --start-date 2026-01-09

# Date range, single dataset
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset imbalance --start-date 2025-12-01 --end-date 2025-12-31

# All datasets, single day
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset all --start-date 2026-01-09

# Large backfill (auto-chunks)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset all --start-date 2024-12-01 --end-date 2025-12-31
```

### Advanced Usage

```bash
# Debug mode
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset imbalance --start-date 2026-01-09 --debug

# Download only (no upload) - testing
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_api_downloader.py --dataset imbalance --start-date 2025-11-01 --end-date 2025-11-30
```

## Database Tables

### 1-Minute Tables (Populated by SOAP Pipeline)
- `finance.ceps_actual_imbalance_1min`
- `finance.ceps_actual_re_price_1min`
- `finance.ceps_svr_activation_1min`
- `finance.ceps_export_import_svr_1min`

### 15-Minute Tables (NOT Populated by SOAP Pipeline)
- Aggregation is **skipped** by SOAP pipeline
- Use existing Selenium-based uploaders or manual aggregation
- Reason: Different table schemas across datasets

## UPSERT Behavior

### What Happens on Conflict

```sql
-- Example for imbalance data
INSERT INTO finance.ceps_actual_imbalance_1min (delivery_timestamp, load_mw)
VALUES (...)
ON CONFLICT (delivery_timestamp)
DO UPDATE SET
    load_mw = EXCLUDED.load_mw,
    created_at = CURRENT_TIMESTAMP
```

**Benefits:**
- Re-running same date range is safe
- Updates existing records if values changed
- No duplicate key errors
- Perfect for backfills

## XML Data Mapping

### System Imbalance
```xml
<item date="2026-01-09T00:00:00+01:00" value1="0.7082545" />
```
→ `delivery_timestamp`, `load_mw`

### RE Prices
```xml
<item date="..." value1="..." value2="..." value3="..." value4="..." />
```
→ `delivery_timestamp`, `afrr_price_eur_mwh`, `mfrr_up_price_eur_mwh`, `mfrr_down_price_eur_mwh`, `mfrr_price_eur_mwh`

### SVR Activation
```xml
<item date="..." value1="..." value2="..." value3="..." value4="..." value5="..." value6="..." />
```
→ `delivery_timestamp`, `afrr_up_mw`, `afrr_down_mw`, `mfrr_up_mw`, `mfrr_down_mw`, `rr_up_mw`, `rr_down_mw`

### Export/Import SVR
```xml
<item date="..." value2="..." value3="..." value4="..." value5="..." />
```
→ `delivery_timestamp`, `imbalance_netting_mw`, `mari_mfrr_mw`, `picasso_afrr_mw`, `sum_exchange_european_platforms_mw`

## Comparison: SOAP vs Selenium

| Feature | Selenium | SOAP API |
|---------|----------|----------|
| **Speed (1 day)** | 15-20s | 0.2s |
| **Speed (30 days)** | 30 × 15s = 7.5 min | 1.7s |
| **Multi-day** | No (1 day at a time) | Yes (up to 30 days) |
| **Chunking** | Manual 125s delay | Automatic |
| **Dependencies** | Chrome, ChromeDriver | Pure Python |
| **Reliability** | Browser issues | HTTP only |
| **Cache** | 120s cache | Same 120s cache |
| **UPSERT** | No (manual in code) | Yes (built-in) |

## Limitations

1. **15-Minute Aggregation**: Not implemented
   - Different table schemas across datasets
   - Use existing Selenium uploaders or manual aggregation

2. **Cache**: SOAP API hits same 120-second backend cache
   - But speed makes it mostly irrelevant
   - Still beneficial to wait 2+ minutes between repeated requests for same historical date

3. **Chunk Size**: Max 30 days per chunk
   - To avoid memory issues
   - Larger requests possible but not tested

## Migration Path

### Option 1: Gradual Migration
- Keep Selenium for cron (stable, works)
- Use SOAP for backfills and manual operations
- Migrate dataset by dataset

### Option 2: Full Migration
- Replace cron jobs with SOAP pipeline
- Update `crontab` to call `ceps_soap_pipeline.py`
- Benefits: Faster, simpler, more reliable

### Recommended: Option 1
- Use SOAP for backfills immediately (proven to work)
- Monitor stability for a few weeks
- Then migrate cron if desired

## Files Created

1. `/app/ceps/ceps_soap_api_downloader.py` - Main downloader
2. `/app/ceps/ceps_soap_xml_parser.py` - XML parser
3. `/app/ceps/ceps_soap_uploader.py` - Database uploader
4. `/app/ceps/ceps_soap_pipeline.py` - Complete pipeline
5. `CEPS_SOAP_API_FINDINGS.md` - Performance analysis
6. `CEPS_SOAP_PIPELINE_SUMMARY.md` - This file

## Next Steps

1. ✅ SOAP pipeline implemented and tested
2. ✅ UPSERT logic working correctly
3. ✅ Multi-day and chunking verified
4. ⏳ **Ready for production backfills**
5. ⏳ Optional: Migrate cron jobs to SOAP

## Commands Reference

See `CEPS_COMMANDS.md` for complete command reference.

Quick reference:
```bash
# Backfill single dataset
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset imbalance --start-date 2024-12-01 --end-date 2025-12-31

# Backfill all datasets
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset all --start-date 2024-12-01 --end-date 2025-12-31
```
