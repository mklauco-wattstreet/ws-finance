# Adding a New Bidding Zone

This guide describes how to add a new ENTSO-E bidding zone to the data pipeline.

## Prerequisites

- EIC code for the bidding zone (e.g., `10YHU-MAVIR----U` for Hungary)
- Country code (e.g., `HU`)
- Determine which data types to collect (generation, load, prices, etc.)

## Steps

### 1. Create Alembic Migration

Create a new migration file in `app/alembic/versions/`:

```python
"""Add [COUNTRY] to entsoe_areas and create partitions.

Revision ID: XXX
"""

def upgrade() -> None:
    # Add to entsoe_areas
    op.execute("""
        INSERT INTO entsoe_areas (id, code, country_name, country_code, is_active)
        VALUES (NEW_ID, 'EIC_CODE', 'Country Name', 'CC', true)
        ON CONFLICT (code) DO NOTHING;
    """)

    # Update sequence
    op.execute("SELECT setval('entsoe_areas_id_seq', GREATEST(NEW_ID, (SELECT MAX(id) FROM entsoe_areas)));")

    # Create partition for each required table
    op.execute("""
        CREATE TABLE entsoe_TABLE_NAME_cc
        PARTITION OF entsoe_TABLE_NAME
        FOR VALUES IN ('CC');
    """)
```

### 2. Update Constants

Edit `app/entsoe/constants.py`:

```python
# Add EIC code
XX_BZN = "10YXX-XXXXX----X"  # Country Name

# Add to AREA_IDS
AREA_IDS = {
    ...
    XX_BZN: NEW_ID,
}

# Add to appropriate ACTIVE_* list
ACTIVE_GENERATION_AREAS = [
    ...
    (NEW_ID, XX_BZN, "XX", "XX"),
]
```

### 3. Run Migration

```bash
docker compose exec entsoe-ote-data-uploader alembic upgrade head
```

### 4. Backfill Historical Data (Optional)

```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_RUNNER_NAME --start 2024-01-01 --end 2024-12-31
```

## Checklist

| Step | File | Action |
|------|------|--------|
| 1 | `app/alembic/versions/YYYYMMDD_XXXX_*.py` | Create migration |
| 2 | `app/entsoe/constants.py` | Add EIC code and AREA_IDS entry |
| 3 | `app/entsoe/constants.py` | Add to ACTIVE_*_AREAS list |
| 4 | Database | Run `alembic upgrade head` |
| 5 | (Optional) | Backfill historical data |

## Data Type Configuration

Different data types require different configurations:

| Data Type | ACTIVE_* List | Table | Runner |
|-----------|---------------|-------|--------|
| Generation | `ACTIVE_GENERATION_AREAS` | `entsoe_generation_actual` | `entsoe_unified_gen_runner` |
| Load | `ACTIVE_GENERATION_AREAS` | `entsoe_load` | `entsoe_unified_load_runner` |
| Imbalance | `ACTIVE_GENERATION_AREAS` | `entsoe_imbalance_prices` | `entsoe_unified_imbalance_runner` |
| Day-ahead Prices | `ACTIVE_DAY_AHEAD_AREAS` | `entsoe_day_ahead_prices` | `entsoe_unified_day_ahead_prices_runner` |

## Notes

- Partitions use `country_code` (string) for routing, not `area_id` (integer)
- Multiple TSOs can share the same country partition (e.g., DE has 4 TSOs)
- The `entsoe_areas` table is the source of truth for area metadata
- Always test with `--dry-run` before production deployment
