# OTE Data Pipeline

Czech electricity market data from OTE-CR (Operator Trhu s Elektrinou).

## Overview

This module downloads and uploads Czech day-ahead market data, imbalance prices, intraday market prices, daily payments, and trade balances from the OTE-CR website and portal. Five independent pipelines feed distinct PostgreSQL tables.

## Philosophy

OTE is the **Czech market operator** and the authoritative source for settlement prices, auction results, and trading activity. While ENTSO-E republishes some of this data, OTE provides it first and with more detail (bid-level granularity, financial components, intraday continuous trading).

**Design principles:**
- **Download-then-upload pattern** — Each pipeline has a separate downloader (HTTP or Selenium) and uploader (parser + DB insert). The downloader saves files to disk, then calls the uploader. Files persist for debugging and re-upload.
- **Auto mode** — Downloaders detect the last file on disk, calculate the missing date range, and fetch only what is needed. No manual date tracking required.
- **Three-tier DAM tables** — Day-ahead auction data is stored at three abstraction levels (raw bids -> clearing summary -> curve depth) to support both auditability and ML feature engineering without expensive queries against the 17,000 row/day bid table.
- **15-minute alignment** — All tables use `(trade_date, period)` or `(trade_date, time_interval)` as the natural key, aligning with the 96-period settlement structure used by OTE, CEPS, and ENTSO-E.

---

## Pipelines

### 1. Day-Ahead Market Curves (DAM)

Downloads OTE matching curve XMLs and computes three tables at increasing abstraction.

| Property | Value |
|----------|-------|
| **Downloader** | `app/download_dam_curves.py` |
| **Uploader** | `app/upload_dam_curves.py` |
| **Source** | OTE-CR website: `MC_DD_MM_YYYY_EN.xml` |
| **Schedule** | `30 14 * * *` (14:30, after ~13:00 auction) |
| **Fetches** | D+1 data (next day's auction results) |
| **File storage** | `./downloads/` (XML) |

**Tables:**

**`da_bid`** — Raw bid stack (~17,000 rows/day). One row per bid step x period x side. Never query directly for ML.

| Column | Description |
|--------|-------------|
| `delivery_date` | Delivery day (D+1 from auction) |
| `period` | 1-96 (15-min intervals) |
| `side` | `sell` (supply) or `buy` (demand) |
| `price` | Bid price EUR/MWh |
| `volume_bid` | Volume offered at this price |
| `volume_matched` | Volume accepted at clearing (0 = rejected) |
| `order_resolution` | `15min` or `60min` product |

**`da_period_summary`** — Clearing summary (96 rows/day). Computed from `da_bid` after each upload.

| Column | Description |
|--------|-------------|
| `clearing_price` | Market Clearing Price for this period |
| `clearing_volume` | Total MW matched |
| `supply_next_price` | First unmatched sell bid above MCP |
| `supply_next_volume` | Volume of that bid |
| `supply_price_gap` | supply_next_price - clearing_price |
| `supply_volume_gap` | Unmatched sell volume between clearing and first step |
| `demand_next_price` | First unmatched buy bid below MCP |
| `demand_price_gap` | clearing_price - demand_next_price |
| `demand_volume_gap` | Unmatched buy volume between clearing and first step |

Critical flag: **`supply_volume_gap = 0`** means the first unmatched sell bid sits immediately above clearing. Present in 100% of top-30 extreme imbalance price events (Jan-Mar 2026).

**`da_curve_depth`** — Curve steepness at fixed MW offsets (960 rows/day = 96 x 2 sides x 5 offsets).

| Column | Description |
|--------|-------------|
| `side` | `sell` (above MCP) or `buy` (below MCP) |
| `offset_mw` | 50, 100, 200, 500, or 1000 MW beyond clearing |
| `price_at_offset` | Price where cumulative unmatched volume reaches offset_mw (NULL if curve exhausted) |
| `volume_available` | Total unmatched volume on this side |

Offsets defined in `CURVE_DEPTH_OFFSETS_MW` constant. Adding new offsets = Python change only, no migration.

See `DA_MARKET_TABLES.md` for full analysis, correlation results, and ML feature recommendations.

---

### 2. Day-Ahead Prices

Downloads OTE day-ahead settlement price reports (Excel).

| Property | Value |
|----------|-------|
| **Downloader** | `app/download_day_ahead_prices.py` |
| **Uploader** | `app/upload_day_ahead_prices.py` |
| **Source** | OTE-CR website: `DM_DD_MM_YYYY_EN.xlsx` (or `DM_15MIN_*` from Oct 2025) |
| **Schedule** | `0 16 * * *` (16:00 daily) |
| **File storage** | `./ote_files/` (Excel) |

**Table: `ote_prices_day_ahead`** (96 rows/day)

| Column | Type | Description |
|--------|------|-------------|
| `price_15min_eur_mwh` | NUMERIC | 15-min product price EUR/MWh |
| `price_60min_ref_eur_mwh` | NUMERIC | 60-min reference price EUR/MWh |
| `volume_mwh` | NUMERIC | Traded volume |
| `purchase_15min_products_mwh` | NUMERIC | 15-min purchase volume |
| `purchase_60min_products_mwh` | NUMERIC | 60-min purchase volume |
| `sale_15min_products_mwh` | NUMERIC | 15-min sale volume |
| `sale_60min_products_mwh` | NUMERIC | 60-min sale volume |
| `saldo_dm_mwh` | NUMERIC | Day-ahead market balance |
| `export_mwh` | NUMERIC | Export volume |
| `import_mwh` | NUMERIC | Import volume |
| `is_15min` | BOOLEAN | True for native 15-min (from Oct 2025), False for legacy hourly |

PK: `(trade_date, period)`. Upsert on conflict.

**Format transition:** Before Oct 1, 2025, hourly data is expanded to 96 periods (volume/4, prices same). From Oct 1, direct 15-min mapping.

---

### 3. Imbalance Prices

Downloads OTE imbalance/balancing price reports (Excel). This is the **ML prediction target**.

| Property | Value |
|----------|-------|
| **Downloader** | `app/download_imbalance_prices.py` |
| **Uploader** | `app/upload_imbalance_prices.py` |
| **Source** | OTE-CR website: `Imbalances_DD_MM_YYYY_V0_EN.xlsx` |
| **Schedule** | `0 */2 * * *` (every 2 hours) |
| **File storage** | `./ote_files/` (Excel) |

**Table: `ote_prices_imbalance`** (96 rows/day)

| Column | Type | Description |
|--------|------|-------------|
| `system_imbalance_mwh` | NUMERIC | System imbalance (+surplus/-deficit) |
| `absolute_imbalance_sum_mwh` | NUMERIC | Absolute sum of all imbalances |
| `positive_imbalance_mwh` | NUMERIC | Positive imbalance volume |
| `negative_imbalance_mwh` | NUMERIC | Negative imbalance volume |
| `rounded_imbalance_mwh` | NUMERIC | Rounded imbalance |
| `cost_of_be_czk` | NUMERIC | Cost of balancing energy (CZK) |
| `cost_of_imbalance_czk` | NUMERIC | Cost of imbalance (CZK) |
| `settlement_price_imbalance_czk_mwh` | NUMERIC | **Main imbalance price (ML target)** |
| `settlement_price_counter_imbalance_czk_mwh` | NUMERIC | Counter-imbalance settlement price |
| `price_protective_be_component_czk_mwh` | NUMERIC | Scarcity signal component |
| `price_be_component_czk_mwh` | NUMERIC | Balancing energy component |
| `price_im_component_czk_mwh` | NUMERIC | Intraday margin component |
| `price_si_component_czk_mwh` | NUMERIC | System imbalance component |
| `price_not_performed_activation_czk_mwh` | NUMERIC | Reserve activation cost |

PK: `(trade_date, period)`. Bulk INSERT (no upsert — expects unique data per upload).

---

### 4. Intraday Market Prices

Downloads OTE intraday continuous trading reports (Excel). Updates throughout the trading day.

| Property | Value |
|----------|-------|
| **Downloader** | `app/download_intraday_prices.py` |
| **Uploader** | `app/upload_intraday_prices.py` |
| **Source** | OTE-CR website: `IM_15MIN_DD_MM_YYYY_EN.xlsx` |
| **Schedule** | `*/15 * * * *` (every 15 minutes) |
| **File storage** | `./ote_files/` (Excel) |

**Table: `ote_prices_intraday_market`** (96 rows/day)

| Column | Type | Description |
|--------|------|-------------|
| `traded_volume_mwh` | NUMERIC | Total traded volume |
| `traded_volume_purchased_mwh` | NUMERIC | Purchase volume |
| `traded_volume_sold_mwh` | NUMERIC | Sell volume |
| `weighted_avg_price_eur_mwh` | NUMERIC | VWAP (used for intraday_premium calc) |
| `min_price_eur_mwh` | NUMERIC | Minimum traded price |
| `max_price_eur_mwh` | NUMERIC | Maximum traded price |
| `last_price_eur_mwh` | NUMERIC | Last traded price |

PK: `(trade_date, period)`. Upsert on conflict (file updates intra-day).

**ML feature:** `intraday_premium = weighted_avg_price - clearing_price`. Best single predictor when combined with DA steepness (r=0.248).

---

### 5. OTE Portal Downloads (Selenium)

Certificate-authenticated portal automation for settlement reports not available via public HTTP.

#### Daily Payments

| Property | Value |
|----------|-------|
| **Script** | `app/ote_production.py` |
| **Uploader** | `app/ote_upload_daily_payments.py` |
| **Schedule** | `0 09 * * *` (09:00 daily) |
| **Auth** | X.509 client certificate (`.p12` in `/app/certs/`) |

**Table: `ote_daily_payments`**

| Column | Type | Description |
|--------|------|-------------|
| `delivery_day` | DATE | Settlement delivery day |
| `settlement_version` | TEXT | Version identifier |
| `settlement_item` | TEXT | Item identifier |
| `type_of_payment` | TEXT | Payment type (BALANCE, REGULATION, etc.) |
| `volume_mwh` | NUMERIC | Volume |
| `amount_excl_vat` | NUMERIC | Amount excluding VAT |
| `currency_of_payment` | TEXT | Currency (CZK) |
| `currency_rate` | NUMERIC | Exchange rate |
| `system` | TEXT | System identifier |
| `message` | TEXT | Message text |

Duplicate detection on `(delivery_day, settlement_version, settlement_item, type_of_payment)`.

#### Trade Balance

| Property | Value |
|----------|-------|
| **Script** | `app/ote_trade_balance_downloader.py` |
| **Uploader** | `app/upload_ote_trade_balance.py` |
| **Schedule** | `0 */2 * * *` (every 2 hours) |

**Table: `ote_trade_balance`** (96 rows/day)

| Column | Type | Description |
|--------|------|-------------|
| `total_buy_mw` / `total_sell_mw` | NUMERIC | Aggregate buy/sell |
| `daily_market_buy_mw` / `_sell_mw` | NUMERIC | Day-ahead market |
| `intraday_auction_buy_mw` / `_sell_mw` | NUMERIC | Intraday auction |
| `intraday_market_buy_mw` / `_sell_mw` | NUMERIC | Intraday continuous |
| `realization_diagrams_buy_mw` / `_sell_mw` | NUMERIC | Realization diagrams |
| *(same columns in MWh)* | | |

PK: `(delivery_date, time_interval)`. Upsert on conflict with `IS DISTINCT FROM` change detection.

---

## Portal Setup

### Initial Certificate Import

```bash
# 1. Set env vars in .env
OTE_CERT_PATH=/app/certs/certificate.p12
OTE_CERT_PASSWORD=your_cert_password
OTE_LOCAL_STORAGE_PASSWORD=your_storage_password

# 2. Import certificate (run once)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ote_production.py --setup

# 3. Verify login
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ote_test_login.py
```

### Portal Troubleshooting

```bash
# Reset certificate
docker compose exec entsoe-ote-data-uploader bash -c "rm -rf /app/browser-profile/* && python3 /app/scripts/ote_production.py --setup"

# Check screenshots on failure
ls -la logs/screenshot_*.png
```

---

## Common Utilities (`app/common.py`)

Shared across all OTE downloaders:
- `auto_determine_date_range()` — Finds last downloaded file, calculates missing dates
- `download_file()` — HTTP download with retry (exponential backoff)
- `run_upload_script()` — Subprocess call to uploader after download
- `setup_logging()` — File + console logging

---

## File Storage

| Pipeline | Host Path | Container Path | Retention |
|----------|-----------|---------------|-----------|
| DAM curves | `./downloads/` | `/app/downloads/` | 7 days |
| DA prices, imbalance, intraday | `./ote_files/` | `/app/ote_files/` | 7 days |
| Portal downloads | `./ote_files/` | `/app/ote_files/` | 7 days |
| Browser profile | `./browser-profile/` | `/app/browser-profile/` | Persistent |

Cleanup cron at 02:10 deletes OTE files older than 7 days.

## Cron Schedule Summary

```
*/15 * * * *  download_intraday_prices.py       (continuous intraday updates)
0 */2 * * *   download_imbalance_prices.py      (imbalance settlement data)
0 */2 * * *   ote_trade_balance_downloader.py   (trade balance from portal)
0 09 * * *    ote_production.py                 (daily payments from portal)
30 14 * * *   download_dam_curves.py            (D+1 auction matching curves)
0 16 * * *    download_day_ahead_prices.py      (DA settlement prices)
```

## Troubleshooting

### View Logs

```bash
docker compose exec entsoe-ote-data-uploader tail -100 /var/log/ote.log
docker compose exec entsoe-ote-data-uploader tail -100 /var/log/ote_download.log
docker compose exec entsoe-ote-data-uploader tail -100 /var/log/cron.log
```

### Manual Re-Upload

```bash
# Re-upload specific DAM curve file
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/upload_dam_curves.py /app/downloads/MC_07_03_2026_EN.xml

# Re-upload daily payments XML
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ote_upload_daily_payments.py /app/ote_files/2026/03/daily_payments_20260307_090000.xml
```
