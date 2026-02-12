# Implementation Guide: Hungary Imbalance Prices

**Date**: 2026-02-11
**Status**: Pending Review
**Scope**: Add Hungary (HU) imbalance prices to `entsoe_imbalance_prices` table with currency-agnostic schema

---

## Overview

Add Hungarian imbalance prices to the existing `entsoe_imbalance_prices` partitioned table. This requires:
1. Schema changes to make columns currency-agnostic (Option B)
2. Adding a `currency` column to track price currency per record
3. Creating HU partition
4. Updating parser and runner to support HU

---

## Files to Modify

| File | Action | Lines Affected |
|------|--------|----------------|
| `app/alembic/versions/XXX_add_hu_imbalance_currency.py` | CREATE | New migration |
| `app/models.py` | MODIFY | Lines 53-81 (EntsoeImbalancePrices) |
| `app/entsoe/parsers.py` | MODIFY | Lines 154-419 (ImbalanceParser) |
| `app/entsoe/constants.py` | MODIFY | Add ACTIVE_IMBALANCE_AREAS |
| `app/runners/entsoe_unified_imbalance_runner.py` | MODIFY | Lines 37-44 (COLUMNS) |

### Files NOT to Touch
- `app/download_imbalance_prices.py` (OTE)
- `app/upload_imbalance_prices.py` (OTE)
- `app/ceps/*` (CEPS)
- `app/runners/ceps_*` (CEPS)

---

## 1. Alembic Migration

**File**: `app/alembic/versions/YYYYMMDD_XXXX_XXX_add_hu_imbalance_currency.py`

### Schema Changes

#### 1.1 Rename Columns (remove `_czk` suffix)

| Old Column Name | New Column Name |
|-----------------|-----------------|
| `pos_imb_price_czk_mwh` | `pos_imb_price_mwh` |
| `pos_imb_scarcity_czk_mwh` | `pos_imb_scarcity_mwh` |
| `pos_imb_incentive_czk_mwh` | `pos_imb_incentive_mwh` |
| `pos_imb_financial_neutrality_czk_mwh` | `pos_imb_financial_neutrality_mwh` |
| `neg_imb_price_czk_mwh` | `neg_imb_price_mwh` |
| `neg_imb_scarcity_czk_mwh` | `neg_imb_scarcity_mwh` |
| `neg_imb_incentive_czk_mwh` | `neg_imb_incentive_mwh` |
| `neg_imb_financial_neutrality_czk_mwh` | `neg_imb_financial_neutrality_mwh` |

#### 1.2 Add Currency Column

```sql
ALTER TABLE entsoe_imbalance_prices ADD COLUMN currency VARCHAR(3) NOT NULL DEFAULT 'EUR';
```

#### 1.3 Set CZ Data to CZK

```sql
UPDATE entsoe_imbalance_prices SET currency = 'CZK' WHERE country_code = 'CZ';
```

#### 1.4 Create HU Partition

```sql
CREATE TABLE entsoe_imbalance_prices_hu
PARTITION OF entsoe_imbalance_prices
FOR VALUES IN ('HU');
```

### Migration SQL (Upgrade)

```sql
-- Step 1: Rename columns on partitioned table (cascades to partitions)
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_price_czk_mwh TO pos_imb_price_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_scarcity_czk_mwh TO pos_imb_scarcity_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_incentive_czk_mwh TO pos_imb_incentive_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_financial_neutrality_czk_mwh TO pos_imb_financial_neutrality_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_price_czk_mwh TO neg_imb_price_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_scarcity_czk_mwh TO neg_imb_scarcity_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_incentive_czk_mwh TO neg_imb_incentive_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_financial_neutrality_czk_mwh TO neg_imb_financial_neutrality_mwh;

-- Step 2: Add currency column with default EUR
ALTER TABLE entsoe_imbalance_prices ADD COLUMN currency VARCHAR(3) NOT NULL DEFAULT 'EUR';

-- Step 3: Update existing CZ data to use CZK
UPDATE entsoe_imbalance_prices SET currency = 'CZK' WHERE country_code = 'CZ';

-- Step 4: Create HU partition
CREATE TABLE entsoe_imbalance_prices_hu
PARTITION OF entsoe_imbalance_prices
FOR VALUES IN ('HU');
```

### Migration SQL (Downgrade)

```sql
-- Step 1: Drop HU partition
DROP TABLE IF EXISTS entsoe_imbalance_prices_hu;

-- Step 2: Drop currency column
ALTER TABLE entsoe_imbalance_prices DROP COLUMN currency;

-- Step 3: Rename columns back
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_price_mwh TO pos_imb_price_czk_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_scarcity_mwh TO pos_imb_scarcity_czk_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_incentive_mwh TO pos_imb_incentive_czk_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN pos_imb_financial_neutrality_mwh TO pos_imb_financial_neutrality_czk_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_price_mwh TO neg_imb_price_czk_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_scarcity_mwh TO neg_imb_scarcity_czk_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_incentive_mwh TO neg_imb_incentive_czk_mwh;
ALTER TABLE entsoe_imbalance_prices RENAME COLUMN neg_imb_financial_neutrality_mwh TO neg_imb_financial_neutrality_czk_mwh;
```

---

## 2. Model Update (`app/models.py`)

### Current (Lines 53-81)

```python
class EntsoeImbalancePrices(Base):
    # ...
    pos_imb_price_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_scarcity_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_incentive_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_financial_neutrality_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_price_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_scarcity_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_incentive_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_financial_neutrality_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
```

### New (After Changes)

```python
class EntsoeImbalancePrices(Base):
    # ...
    pos_imb_price_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_scarcity_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_incentive_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_financial_neutrality_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_price_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_scarcity_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_incentive_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_financial_neutrality_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
```

### Additional Model Changes

Update `__table_args__` to reflect partitioned table structure:
- Primary Key: `(trade_date, period, area_id, country_code)` (already correct from migration 023)
- Add `area_id` and `country_code` columns if not present (already added in migration 023)

---

## 3. Parser Update (`app/entsoe/parsers.py`)

### Changes in `ImbalanceParser` class

#### 3.1 `_process_prices_period()` method (lines 205-306)

Change all dictionary keys:

| Old Key | New Key |
|---------|---------|
| `pos_imb_price_czk_mwh` | `pos_imb_price_mwh` |
| `pos_imb_scarcity_czk_mwh` | `pos_imb_scarcity_mwh` |
| `pos_imb_incentive_czk_mwh` | `pos_imb_incentive_mwh` |
| `pos_imb_financial_neutrality_czk_mwh` | `pos_imb_financial_neutrality_mwh` |
| `neg_imb_price_czk_mwh` | `neg_imb_price_mwh` |
| `neg_imb_scarcity_czk_mwh` | `neg_imb_scarcity_mwh` |
| `neg_imb_incentive_czk_mwh` | `neg_imb_incentive_mwh` |
| `neg_imb_financial_neutrality_czk_mwh` | `neg_imb_financial_neutrality_mwh` |

#### 3.2 `combine_data()` method (lines 374-419)

Add currency field based on country_code:

```python
def combine_data(self) -> List[Dict]:
    # ... existing code ...

    for key in sorted(all_keys):
        # ... existing record building ...

        # Add currency based on country_code
        if self.country_code == 'CZ':
            record['currency'] = 'CZK'
        else:
            record['currency'] = 'EUR'  # HU, DE, AT, PL, SK all use EUR

        # ... rest of method ...
```

---

## 4. Constants Update (`app/entsoe/constants.py`)

### Add New List for Imbalance-Specific Areas

```python
# Active areas for imbalance prices fetching
# Tuple format: (area_id, eic_code, display_label, country_code)
# Note: Imbalance prices are fetched per control area, not per TSO
ACTIVE_IMBALANCE_AREAS = [
    (1, CZ_BZN, "CZ", "CZ"),
    (9, HU_BZN, "HU", "HU"),
]
```

### Currency Mapping Reference

| Country Code | Currency | Notes |
|--------------|----------|-------|
| CZ | CZK | Czech Koruna - Czech Republic |
| HU | EUR | Euro - Hungary uses EUR in ENTSO-E |
| DE | EUR | Euro - Germany |
| AT | EUR | Euro - Austria |
| PL | EUR | Euro - Poland (ENTSO-E uses EUR) |
| SK | EUR | Euro - Slovakia |

---

## 5. Runner Update (`app/runners/entsoe_unified_imbalance_runner.py`)

### 5.1 Update COLUMNS list (lines 37-44)

```python
COLUMNS = [
    "trade_date", "period", "area_id", "country_code", "time_interval",
    "pos_imb_price_mwh", "pos_imb_scarcity_mwh",
    "pos_imb_incentive_mwh", "pos_imb_financial_neutrality_mwh",
    "neg_imb_price_mwh", "neg_imb_scarcity_mwh",
    "neg_imb_incentive_mwh", "neg_imb_financial_neutrality_mwh",
    "imbalance_mwh", "difference_mwh", "situation", "status", "currency"
]
```

### 5.2 Update imports (line 27)

```python
from entsoe.constants import ACTIVE_IMBALANCE_AREAS  # Changed from ACTIVE_GENERATION_AREAS
```

### 5.3 Update area iteration (lines 188, 210, 229)

Change `ACTIVE_GENERATION_AREAS` to `ACTIVE_IMBALANCE_AREAS`

### 5.4 Update `_prepare_records()` method (lines 102-125)

Add currency to the tuple:

```python
def _prepare_records(self, data: List[dict]) -> List[Tuple]:
    records = []
    for record in data:
        records.append((
            record['trade_date'],
            record['period'],
            record['area_id'],
            record['country_code'],
            record['time_interval'],
            record.get('pos_imb_price_mwh'),
            record.get('pos_imb_scarcity_mwh'),
            record.get('pos_imb_incentive_mwh'),
            record.get('pos_imb_financial_neutrality_mwh'),
            record.get('neg_imb_price_mwh'),
            record.get('neg_imb_scarcity_mwh'),
            record.get('neg_imb_incentive_mwh'),
            record.get('neg_imb_financial_neutrality_mwh'),
            record.get('imbalance_mwh'),
            record.get('difference_mwh'),
            record.get('situation'),
            record.get('status'),
            record.get('currency'),
        ))
    return records
```

---

## 6. Execution Plan

### Step 1: Create Migration File

```bash
# Generate migration file (run outside Docker for file creation)
touch app/alembic/versions/20260211_0024_024_add_hu_imbalance_currency.py
```

### Step 2: Run Migration

```bash
docker compose exec entsoe-ote-data-uploader alembic upgrade head
```

### Step 3: Update Application Files

1. `app/models.py`
2. `app/entsoe/parsers.py`
3. `app/entsoe/constants.py`
4. `app/runners/entsoe_unified_imbalance_runner.py`

### Step 4: Rebuild Container (if needed)

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build entsoe-ote-data-uploader
```

### Step 5: Test with Dry Run

```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner --dry-run --debug
```

### Step 6: Manual Backfill (2026 data)

**Note**: Per CLAUDE.md, backfill commands that may take >30 seconds should be run manually.

```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner --start 2026-01-01 --end 2026-02-11
```

---

## 7. Verification Queries

### Check HU Partition Exists

```sql
SELECT tablename FROM pg_tables
WHERE schemaname = 'finance' AND tablename LIKE 'entsoe_imbalance_prices%';
```

Expected output:
```
entsoe_imbalance_prices
entsoe_imbalance_prices_cz
entsoe_imbalance_prices_de
entsoe_imbalance_prices_at
entsoe_imbalance_prices_pl
entsoe_imbalance_prices_sk
entsoe_imbalance_prices_hu  -- NEW
```

### Check Column Structure

```sql
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'finance' AND table_name = 'entsoe_imbalance_prices'
ORDER BY ordinal_position;
```

### Check Currency Values

```sql
SELECT country_code, currency, COUNT(*)
FROM finance.entsoe_imbalance_prices
GROUP BY country_code, currency;
```

Expected:
```
CZ | CZK | (existing count)
HU | EUR | (new count after backfill)
```

### Check HU Data

```sql
SELECT trade_date, period, pos_imb_price_mwh, currency
FROM finance.entsoe_imbalance_prices
WHERE country_code = 'HU'
ORDER BY trade_date DESC, period DESC
LIMIT 10;
```

---

## 8. Rollback Plan

If issues occur, run migration downgrade:

```bash
docker compose exec entsoe-ote-data-uploader alembic downgrade -1
```

This will:
1. Drop HU partition
2. Remove currency column
3. Rename columns back to `*_czk_mwh`

**Note**: HU data will be lost on rollback. CZ data is preserved.

---

## 9. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Column rename breaks existing queries | Medium | Update all dependent code before migration |
| HU API returns different XML structure | Low | Parser already handles standard ENTSO-E format |
| Currency column default causes issues | Low | Explicitly set currency in parser based on country |
| Migration fails mid-way | High | Test on staging first; migration is transactional |

---

## 10. Questions to Verify

1. **Hungary EIC code**: Is `10YHU-MAVIR----U` correct for imbalance price queries? A: YES
2. **Currency assumption**: Is it confirmed that Hungary ENTSO-E imbalance prices are in EUR? A: YES
3. **Scheduling**: Should the HU cron job run at the same time as CZ (potential API rate limits)? A: 15 minutes
4. **Data availability**: Does ENTSO-E have 2026 imbalance data for Hungary? A: yes

---

## Appendix: Area Configuration Reference

From `entsoe_areas` table:

| id | code | country_name | country_code |
|----|------|--------------|--------------|
| 1 | 10YCZ-CEPS-----N | Czech Republic | CZ |
| 9 | 10YHU-MAVIR----U | Hungary | HU |

From `constants.py`:

```python
HU_BZN = "10YHU-MAVIR----U"  # Hungary
AREA_IDS = {
    HU_BZN: 9,  # Hungary
}
```
