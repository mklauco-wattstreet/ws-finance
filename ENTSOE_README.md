# ENTSO-E Data Pipeline

Pan-European electricity market data from the ENTSO-E Transparency Platform.

## Overview

This module fetches real-time and historical data from the ENTSO-E API, parses XML responses, and uploads to PostgreSQL using idempotent upserts.

## Data Types

| Runner | Doc Type | Description | Areas |
|--------|----------|-------------|-------|
| `entsoe_unified_imbalance_runner` | A85, A86 | Imbalance prices & volumes | CZ |
| `entsoe_unified_load_runner` | A65 | Actual load & DA forecast | CZ |
| `entsoe_unified_gen_runner` | A75 | Generation by fuel type | CZ, DE, AT, PL, SK |
| `entsoe_unified_flow_runner` | A11 | Physical cross-border flows | CZ |
| `entsoe_unified_forecast_runner` | A69 | Wind/Solar DA forecasts | CZ |
| `entsoe_unified_balancing_runner` | A84 | Activated aFRR/mFRR reserves | CZ |
| `entsoe_unified_scheduled_runner` | A71 | Scheduled generation (DA) | CZ |
| `entsoe_unified_sched_flow_runner` | A09 | Scheduled cross-border exchanges | CZ |

## Directory Structure

```
app/
├── runners/                           # Pipeline runners
│   ├── base_runner.py                 # Shared infrastructure
│   ├── entsoe_unified_*.py            # 8 data pipelines
│   ├── entsoe_consistency_check.py    # Data validation utility
│   └── README.md                      # Runner documentation
│
└── entsoe/                            # ENTSO-E module
    ├── client.py                      # API client with retry logic
    ├── parsers.py                     # XML parsing for all doc types
    ├── constants.py                   # Area codes, document types
    └── xml_definitions/               # XSD validation schemas

downloads/
└── entsoe/                            # XML file storage (auto-cleaned)
    └── YYYY/MM/*.xml                  # Organized by year/month
```

## Configuration

Add to `.env`:

```bash
ENTSOE_SECURITY_TOKEN=your_token_here
ENTSOE_CONTROL_AREA_DOMAIN=10YCZ-CEPS-----N
ENTSOE_BASE_URL=https://web-api.tp.entsoe.eu/api
```

## Usage

### Cron (Automatic)

All runners execute every 15 minutes via crontab, fetching the last 3 hours of data.

### Manual Execution

```bash
# Normal mode - fetch last 3 hours
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner

# Backfill mode - historical data
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner --start 2024-12-01 --end 2024-12-31

# Dry run - fetch and parse without uploading
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner --dry-run --debug
```

### Command-Line Arguments

| Argument | Description |
|----------|-------------|
| `--debug` | Enable verbose logging |
| `--dry-run` | Fetch and parse but skip database upload |
| `--start YYYY-MM-DD` | Start date for backfill |
| `--end YYYY-MM-DD` | End date for backfill (defaults to today) |

## Backfill

Backfill automatically chunks requests into 7-day blocks (ENTSO-E API limit):

```bash
# Backfill all runners from Dec 1, 2024
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_load_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_gen_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_flow_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_forecast_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_balancing_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_scheduled_runner --start 2024-12-01
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_sched_flow_runner --start 2024-12-01
```

## Data Consistency Check

Verify data completeness across all datasets and countries:

```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_consistency_check
```

## Database Tables

| Table | Granularity | Key Fields |
|-------|-------------|------------|
| `entsoe_imbalance_prices` | 15-min | trade_date, period, area_id, country_code |
| `entsoe_load` | 15-min | trade_date, period, area_id, country_code |
| `entsoe_generation_actual` | 15-min | trade_date, period, area_id, country_code |
| `entsoe_cross_border_flows` | 15-min | trade_date, period, area_id, country_code |
| `entsoe_generation_forecast` | 15-min | trade_date, period, area_id, country_code |
| `entsoe_balancing_energy` | 15-min | trade_date, period, area_id, country_code |
| `entsoe_generation_scheduled` | 15-min | trade_date, period, area_id, country_code |
| `entsoe_scheduled_cross_border_flows` | 15-min | trade_date, period, area_id, country_code |
| `entsoe_areas` | Static | id, code (EIC), country_code |

Tables are partitioned by `country_code` for efficient querying.

## File Storage

XML files are stored in `downloads/entsoe/YYYY/MM/` and automatically cleaned after 1 day (cron at 02:00).

Container path: `/app/downloads/entsoe`
Host path: `./downloads/entsoe`

## Architecture

```
ENTSO-E API (REST)
       │
       ▼
┌─────────────────┐
│  EntsoeClient   │  Fetch XML with retry logic
│  (client.py)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Save to disk   │  downloads/entsoe/YYYY/MM/
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  XML Parsers    │  Parse A65, A69, A71, A75, A84, A85, A86, A09, A11
│  (parsers.py)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bulk Upsert    │  ON CONFLICT DO UPDATE (idempotent)
│  (base_runner)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PostgreSQL     │  Partitioned tables
└─────────────────┘
```

## Troubleshooting

### Connection Errors

1. Verify `ENTSOE_SECURITY_TOKEN` is valid
2. Check network access to `https://web-api.tp.entsoe.eu`
3. Run with `--debug` to see full API responses

### Empty Results

1. Data may not be available for the requested period
2. Verify control area domain is correct
3. Check ENTSO-E platform status

### View Logs

```bash
# Cron logs
docker compose exec entsoe-ote-data-uploader tail -100 /var/log/entsoe_imbalance.log

# All ENTSO-E logs
docker compose exec entsoe-ote-data-uploader tail -100 /var/log/entsoe_*.log
```

## API Reference

- ENTSO-E Transparency Platform: https://transparency.entsoe.eu
- API Documentation: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
