# ENTSO-E Data Extraction

## Overview

This module fetches electricity market data from the ENTSO-E Transparency Platform API and saves XML files for later processing.

## Data Types

The module supports fetching two types of data:

1. **Imbalance Prices (A85)** - 17.1.G Imbalance prices
2. **Total Imbalance Volumes (A86)** - 17.1.H Total Imbalance Volumes

## Configuration

### Environment Variables

Add the following to your `.env` file:

```bash
# ENTSO-E API Configuration
ENTSOE_SECURITY_TOKEN=your_security_token_here
ENTSOE_CONTROL_AREA_DOMAIN=10YCZ-CEPS-----N  # Czech Republic
```

## Usage

### Scheduled Execution (Cron)

The script is designed to run every 15 minutes and fetch the preceding 1 hour of data:

```bash
# Add to crontab
*/15 * * * * cd /app/scripts && /usr/local/bin/python3 entsoe/fetch_entsoe_data.py >> /var/log/entsoe_fetch.log 2>&1
```

### Manual Execution

Fetch all data types (prices and volumes):

```bash
python3 entsoe/fetch_entsoe_data.py
```

Fetch only imbalance prices:

```bash
python3 entsoe/fetch_entsoe_data.py --document-type A85
```

Fetch only imbalance volumes:

```bash
python3 entsoe/fetch_entsoe_data.py --document-type A86
```

Enable debug logging:

```bash
python3 entsoe/fetch_entsoe_data.py --debug
```

Specify output directory:

```bash
python3 entsoe/fetch_entsoe_data.py --output-dir /app/entsoe_files
```

## How It Works

### Time Range Calculation

When executed, the script calculates the preceding 1 hour time range based on the current time, rounded down to the nearest 15 minutes.

**Example:**
- Current time: `07:30`
- Period start: `06:30`
- Period end: `07:30`

This gives us 1 hour of data from 06:30 to 07:30, which includes 4 data points (15-minute intervals).

### Data Flow

1. **Fetch XML Data** - The `EntsoeClient` fetches data from ENTSO-E API
2. **Handle Zip** - If the response is zipped, it automatically unzips it
3. **Save XML** - XML files are saved to disk in YYYY/MM/ directory structure

### File Structure

XML files are saved with the following structure:

```
/app/scripts/entsoe/data/
├── 2025/
│   ├── 11/
│   │   ├── entsoe_imbalance_prices_20251111_0630_0730.xml
│   │   ├── entsoe_imbalance_prices_20251111_0645_0745.xml
│   │   ├── entsoe_imbalance_volumes_20251111_0630_0730.xml
│   │   └── entsoe_imbalance_volumes_20251111_0645_0745.xml
```

## Module Structure

```
app/entsoe/
├── __init__.py              # Package initialization
├── README.md                # This file
├── crontab.example          # Crontab configuration example
├── entsoe_client.py         # API client for fetching data
├── entsoe_parser.py         # XML parser for ENTSO-E data
└── fetch_entsoe_data.py     # Main script for scheduled execution
```

## API Client (`entsoe_client.py`)

The `EntsoeClient` class provides:

- `fetch_imbalance_prices(period_start, period_end)` - Fetch prices (A85)
- `fetch_imbalance_volumes(period_start, period_end)` - Fetch volumes (A86)
- `get_preceding_hour_range()` - Calculate time range for scheduled runs
- Automatic zip/unzip handling

## XML Parser (`entsoe_parser.py`)

The `EntsoeParser` class provides:

- `parse_imbalance_prices(xml_content)` - Parse prices XML
- `parse_imbalance_volumes(xml_content)` - Parse volumes XML
- `parse_generic(xml_content, document_type)` - Generic parser

Returns parsed data as list of dictionaries for further processing.

## Testing

### Test API Client

```bash
cd /app/scripts
python3 entsoe/entsoe_client.py
```

### Test Parser

```bash
cd /app/scripts
python3 entsoe/entsoe_parser.py
```

### Test in Docker

```bash
docker exec python-cron-scheduler bash -c "cd /app/scripts && python3 entsoe/fetch_entsoe_data.py --debug"
```

## Troubleshooting

### Connection Errors

If you get connection errors, verify:

1. ENTSO-E security token is valid
2. Control area domain is correct
3. Network connectivity to `https://web-api.tp.entsoe.eu`

### Empty Results

If no data is returned:

1. Check if data is available for the requested time period
2. Verify the control area domain is correct
3. Enable debug logging (`--debug`) to see API responses

## ENTSO-E API Reference

- API Documentation: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
- Document Types:
  - A85: Imbalance prices (17.1.G)
  - A86: Total imbalance volumes (17.1.H)

## Dependencies

- `requests` - HTTP requests to ENTSO-E API
- `python-dotenv` - Environment variable management

All dependencies are already included in `requirements.txt`.

## Next Steps

After fetching and saving XML files, you can process them separately:

1. Parse XML files using `entsoe_parser.py`
2. Insert data into database (to be implemented separately)
3. Generate reports or analytics from the data
