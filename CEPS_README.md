# CEPS Data Pipeline

Czech grid operator (CEPS) electricity market data via SOAP API.

## Overview

This module fetches real-time and historical data from the CEPS SOAP API, parses XML responses, and uploads to PostgreSQL using idempotent upserts.

## Datasets

| Dataset | Description | Granularity |
|---------|-------------|-------------|
| `imbalance` | System imbalance (MW) | 1-min -> 15-min aggregated |
| `re_price` | Reserve energy prices (aFRR/mFRR) | 1-min -> 15-min aggregated |
| `svr_activation` | SVR activation volumes | 1-min -> 15-min aggregated |
| `export_import_svr` | Cross-border SVR flows | 1-min -> 15-min aggregated |
| `generation_res` | Renewable generation | 1-min -> 15-min aggregated |
| `generation` | Generation by plant type | 15-min native |
| `generation_plan` | Planned total generation | 15-min native |
| `estimated_imbalance_price` | Estimated imbalance price | 15-min native |

## Directory Structure

```
app/ceps/
├── ceps_soap_pipeline.py      # Main pipeline (download + parse + upload)
├── ceps_soap_xml_parser.py    # XML parsing for SOAP responses
├── ceps_soap_uploader.py      # Database upsert logic
├── ceps_consistency_check.py  # Data validation utility
└── constants.py               # Constants

downloads/ceps/
└── soap/YYYY/MM/*.xml         # XML file storage (auto-cleaned)
```

## Usage

### Cron (Automatic)

Pipeline runs every 15 minutes (at :12, :27, :42, :57), fetching today's data for all datasets.

### Manual Execution

```bash
# Normal mode - fetch today's data for all datasets
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_soap_pipeline --dataset all

# Single dataset
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_soap_pipeline --dataset imbalance

# Specific date
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_soap_pipeline --dataset all --start 2026-01-15

# Date range (backfill)
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_soap_pipeline --dataset all --start 2025-12-01 --end 2025-12-31

# Dry run (fetch and parse without uploading)
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_soap_pipeline --dataset all --dry-run --debug
```

### Command-Line Arguments

| Argument | Description |
|----------|-------------|
| `--dataset` | Dataset to process (required): `imbalance`, `re_price`, `svr_activation`, `export_import_svr`, `generation_res`, `generation`, `generation_plan`, `estimated_imbalance_price`, or `all` |
| `--start YYYY-MM-DD` | Start date (defaults to today) |
| `--end YYYY-MM-DD` | End date (defaults to start date) |
| `--debug` | Enable verbose logging |
| `--dry-run` | Fetch and parse but skip database upload |

## Backfill

Large backfills are automatically chunked into 7-day blocks:

```bash
# Backfill entire year (auto-chunks)
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_soap_pipeline --dataset all --start 2025-01-01 --end 2025-12-31
```

## Data Consistency Check

Verify data completeness:

```bash
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_consistency_check
```

## Database Tables

### 1-Minute Raw Tables
- `ceps_actual_imbalance_1min` - System imbalance
- `ceps_actual_re_price_1min` - Reserve energy prices
- `ceps_svr_activation_1min` - SVR activation
- `ceps_export_import_svr_1min` - Cross-border SVR
- `ceps_generation_res_1min` - Renewable generation

### 15-Minute Aggregated Tables
- `ceps_actual_imbalance_15min`
- `ceps_actual_re_price_15min`
- `ceps_svr_activation_15min`
- `ceps_export_import_svr_15min`
- `ceps_generation_res_15min`
- `ceps_generation_15min` - Generation by plant type (native 15-min)
- `ceps_generation_plan_15min` - Planned generation (native 15-min)
- `ceps_estimated_imbalance_price_15min` - Estimated imbalance price (native 15-min)

## File Storage

XML files are stored in `downloads/ceps/soap/YYYY/MM/` and automatically cleaned after 1 day (cron at 02:05).

Container path: `/app/downloads/ceps/soap`
Host path: `./downloads/ceps/soap`

## Architecture

```
CEPS SOAP API
       │
       ▼
┌─────────────────┐
│  Build SOAP     │  Envelope with operation params
│  Envelope       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  HTTP POST      │  60s timeout, ~7s response
│  to CEPS        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Save to disk   │  downloads/ceps/soap/YYYY/MM/
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  XML Parser     │  Parse SOAP response
│  (per dataset)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Deduplicate    │  Remove duplicate timestamps
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bulk Upsert    │  ON CONFLICT DO UPDATE (idempotent)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PostgreSQL     │  1-min raw + 15-min aggregated
└─────────────────┘
```

## SOAP API Details

- Endpoint: `https://www.ceps.cz/_layouts/CepsData.asmx`
- Response time: ~7 seconds per dataset
- Chunk size: 7 days maximum per request
- Data delay: ~5 minutes from real-time

## Troubleshooting

### Connection Errors

1. Verify network access to `https://www.ceps.cz`
2. Run with `--debug` to see full responses

### Empty Results

1. CEPS may not have data for the requested period
2. Check CEPS website for service status

### View Logs

```bash
docker compose exec entsoe-ote-data-uploader tail -100 /var/log/ceps.log
```
