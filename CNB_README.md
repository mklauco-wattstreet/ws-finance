# CNB FX Rate Pipeline

Czech National Bank CZK/EUR daily exchange rate, used to convert CZ-currency imbalance and OTE prices to EUR for cross-country joins.

## Overview

The CNB publishes a single official CZK/EUR rate per business day at ~14:30 Prague. This pipeline polls every 2 hours from 15:00–21:00 on weekdays, retrying until the rate for the day appears.

| Property | Value |
|----------|-------|
| **Runner** | `app/runners/cnb_exchange_rate_runner.py` |
| **Source** | CNB public daily FX feed |
| **Schedule** | `0 15,17,19,21 * * 1-5` (weekdays only) |
| **Log** | `/var/log/cnb_fx.log` |

## Database Schema

**Table: `finance.cnb_exchange_rate`** — one row per business day.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Auto |
| `rate_date` | DATE | Business day the rate applies to (PK) |
| `czk_eur` | NUMERIC(10,6) | CZK per 1 EUR |
| `created_at` / `updated_at` | TIMESTAMP | Audit columns |

PK: `(rate_date)`. Index on `rate_date`.

## Usage

### Manual Execution

```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.cnb_exchange_rate_runner --debug
docker compose exec entsoe-ote-data-uploader python3 -m runners.cnb_exchange_rate_runner --start 2024-01-01 --end 2026-04-30
```

### Joining for EUR conversion

```sql
SELECT i.trade_date, i.period,
       i.pos_imb_price_mwh / fx.czk_eur AS pos_imb_price_eur_mwh
FROM finance.entsoe_imbalance_prices i
JOIN finance.cnb_exchange_rate fx ON fx.rate_date = i.trade_date
WHERE i.country_code = 'CZ' AND i.currency = 'CZK';
```

## Migration History

| Rev | Date | Description |
|-----|------|-------------|
| 046 | 2026-03-12 | Create `cnb_exchange_rate` table |
