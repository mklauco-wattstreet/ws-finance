# DAM Tables: Philosophy, Design & Analysis

**Created:** 2026-03-04
**Tables:** `da_bid`, `da_period_summary`, `da_curve_depth`
**Source:** OTE-CR matching curve XML files (MC_DD_MM_YYYY_EN.xml)
**Download script:** `app/download_dam_curves.py`
**Upload script:** `app/upload_dam_curves.py`

---

## 1. The Day-Ahead Auction

Every day at ~13:00, OTE runs a day-ahead auction for the next day's electricity delivery.
Buyers and sellers submit bids. OTE matches them and publishes the result as an XML file.

The auction finds the **Market Clearing Price (MCP)**: the price where cumulative supply
equals cumulative demand. All matched volume trades at MCP.

```
SELL (supply)                    BUY (demand)

Price                            Price
  |              ░░░░░             |░░░░
  |        ░░░░░░                  |    ░░░░░
  |  ░░░░░░                        |         ░░░░░░
  |░░                              |               ░░░░░
  +──────────────► Volume          +──────────────► Volume
          ^
          MCP (clearing price)
```

---

## 2. Three Tables, Three Levels of Abstraction

```
da_bid              raw data       every bid step (~17,000 rows/day)
da_period_summary   summary        what cleared, what is next (96 rows/day)
da_curve_depth      shape          largest price-jump (wall) per direction (96 rows/day)
```

---

## 3. `da_bid` — Raw Bid Stack

One row per bid step × period × side. Source of truth for all derived tables.

| Column | Meaning |
|--------|---------|
| `delivery_date` | Delivery day (D+1 from auction) |
| `period` | 1–96 (15-minute intervals) |
| `side` | `sell` = supply / `buy` = demand |
| `price` | Bid price in EUR/MWh |
| `volume_bid` | Total volume offered at this price |
| `volume_matched` | Volume accepted at clearing. `> 0` = matched, `= 0` = rejected |
| `order_resolution` | `15min` or `60min` product |

**Do not query directly for ML features.** Too large, too granular.

---

## 4. `da_period_summary` — Clearing Summary

One row per period (96/day). Computed from `da_bid` after each upload.

| Column | Meaning |
|--------|---------|
| `clearing_price` | MCP for this period |
| `clearing_volume` | Total MW matched |
| `supply_next_price` | Price of first unmatched sell bid above MCP |
| `supply_next_volume` | Volume of that bid |
| `supply_price_gap` | `supply_next_price - clearing_price` |
| `supply_volume_gap` | Unmatched sell volume between clearing and first step |
| `demand_next_price` | Price of first unmatched buy bid below MCP |
| `demand_price_gap` | `clearing_price - demand_next_price` |
| `demand_volume_gap` | Unmatched buy volume between clearing and first step |

### The critical flag: `supply_volume_gap = 0`

When zero, the first unmatched sell bid sits *immediately* above clearing with no empty
space. The TSO activating even 1 MW of upward reserve immediately hits expensive bids.

**Every single top-30 extreme imbalance price event (2026-01-01 to 2026-03-04) has
`supply_volume_gap = 0`.** This is the strongest binary risk indicator in the dataset.

### Limitation

`da_period_summary` only describes the *first* step above/below clearing. It tells you
there is a cliff, but not how high or how wide it is beyond that first step.
That is what `da_curve_depth` solves.

---

## 5. `da_curve_depth` — Largest Price-Jump (Wall) per Direction

One row per period (96/day), keyed on `(delivery_date, period)`. For each contract, the curve is walked outward from clearing in four directions, and the single largest price jump in each direction is recorded.

| Direction        | Curve walked                                | Walk order |
|------------------|---------------------------------------------|------------|
| `supply`         | sell bids with price > clearing (unmatched) | ASC |
| `supply_matched` | sell bids with price < clearing (matched)   | DESC |
| `demand`         | buy bids with price < clearing (unmatched)  | DESC |
| `demand_matched` | buy bids with price > clearing (matched)    | ASC |

Per direction, three columns (`<dir>_mw_from_clearing`, `<dir>_price_from_clearing`, `<dir>_slope`):

| Column | Meaning |
|--------|---------|
| `<dir>_mw_from_clearing`    | Cumulative MW from clearing to the foot of the jump (>= 0) |
| `<dir>_price_from_clearing` | Signed price distance to top of jump: `price_top - clearing_price`. Positive for `supply` / `demand_matched`, negative for `supply_matched` / `demand`. |
| `<dir>_slope`               | `price_from_clearing / mw_from_clearing` (€/MWh per MW). Same sign as `price_from_clearing`. |

Plus `clearing_price` (NUMERIC(10, 2), NOT NULL).

### Tie-break

When two consecutive pairs share the same |jump|, the one closest to clearing (smallest `mw_from_clearing`) is taken.

### NULL semantics

A direction's three fields are NULL together when that side has < 2 bids in the relevant range:
- `supply_matched_*` and `demand_matched_*` are NULL in normal regimes (supply/demand curves typically start at clearing — no inframarginal step pair to score).
- `supply_*` NULL is rare — extreme oversupply with thin sell curve above clearing.
- `demand_*` NULL is rare — extreme scarcity with thin buy curve below clearing.

NULLs are valid output. Do not substitute zero.

### Legacy table

The previous offset-sampled table is preserved as `da_curve_depth_legacy_offset_mw` until the wall-detection backfill is verified. Drop after sign-off.

---

## 6. Analysis Results (2026-01-01 to 2026-03-04)

> **Note:** Sections 6 and 7 below were produced against the legacy MW-offset schema (`sell_50mw`, `sell_100mw`, etc.). They are kept for historical reference. The new wall-detection schema requires fresh feature engineering and re-analysis.


### 6.1 Correlation with imbalance price (deficit periods, n=4,154)

| Feature | Pearson correlation |
|---------|-------------------|
| `intraday_premium x sell_100mw` | **0.248** (best found) |
| `intraday_vwap` | 0.240 |
| `intraday_premium` | 0.185 |
| `sell_50mw` | 0.171 |
| `sell_100mw` | 0.135 |
| `abs(system_imbalance_mwh)` | 0.107 |

DA curve steepness alone is a weak linear predictor. Its signal is conditional on:
- Time of day (see 6.2)
- Intraday market confirmation (see 6.3)

### 6.2 Steepness signal by hour of day (deficit periods)

| Hour | Correlation steepness vs imb price |
|------|-----------------------------------|
| 0–6 | 0.02–0.08 (near zero — irrelevant at night) |
| 7 | 0.612 |
| 10 | 0.334 |
| 11 | **0.803** (peak) |
| 12 | 0.617 |
| 14 | 0.498 |
| 18–19 | 0.46–0.55 |
| 20–23 | 0.15–0.22 |

Steepness is only a meaningful signal during business hours (7–19).

### 6.3 Intraday premium × steepness interaction

`intraday_premium = intraday_vwap - da_clearing_price`

When intraday trades above DA clearing, the market is pricing in scarcity.
When steep curve AND intraday premium both point to risk, prices spike.
When intraday trades below DA (market sees surplus), steep curves do not matter.

| Intraday signal | Steepness | Avg imb price | Median |
|----------------|-----------|--------------|--------|
| well above DA (> +15 EUR) | steep (200+) | 10,400 CZK | 7,564 CZK |
| well above DA (> +15 EUR) | flat (< 200) | 7,086 CZK | 4,470 CZK |
| below DA | steep (200+) | ~4,000 CZK | ~4,200 CZK |
| below DA | flat (< 200) | ~3,200 CZK | ~2,950 CZK |

### 6.4 Surplus vs deficit

- **Deficit** (system_imbalance < 0): steepness of sell curve adds signal
- **Surplus** (system_imbalance > 0): imbalance prices near zero regardless of buy curve.
  Buy-side steepness is not a useful predictor in practice.

### 6.5 What the DA curve does NOT explain

Most extreme spikes are driven by **balancing reserve scarcity** (mFRR/aFRR activation
prices), not by DA curve shape. Events like:

| Date | Period | sell_100mw | Imbalance | Imb price |
|------|--------|-----------|-----------|-----------|
| 2026-01-07 | p96 | 125 EUR (flat) | -16.7 MWh | 286,417 CZK |
| 2026-01-01 | p69 | 121 EUR (flat) | -0.53 MWh | 62,229 CZK |

These are unpredictable from DA data alone. The CEPS aFRR/mFRR activation prices
(`ceps_actual_re_price_15min`) are the relevant signal for these events.

---

## 7. Recommended ML Features (from this analysis)

> **Note:** the ranking below was produced on the legacy MW-offset schema. `da_sell_50mw` / `da_sell_100mw` / `da_sell_steepness_ratio` no longer exist on `da_curve_depth` — they live only on the preserved `da_curve_depth_legacy_offset_mw` table. The "Current source" column maps each legacy feature to the closest concept in the new wall-detection schema. Correlations have not been re-measured against the new columns; treat the priority order as a starting hypothesis, not a verified result.

All DA features use **target period** as lag (D-1 publication, no lag needed).

| # | Legacy feature | Current source | Notes |
|---|---|---|---|
| 1 | `intraday_premium x da_sell_100mw` | `intraday_premium x supply_slope` (or `x supply_price_from_clearing`) | Best single signal historically; needs re-fit. |
| 2 | `intraday_premium` | `ote_prices_intraday_market.weighted_avg_price_eur_mwh - da_period_summary.clearing_price` | Unchanged. |
| 3 | `da_supply_gap_zero` | `da_period_summary.supply_volume_gap = 0` | Unchanged — present in 100% of top-30 events. |
| 4 | `da_sell_100mw x is_business_hour` | `supply_slope x is_business_hour` (hours 7–19) | Conditional regime gate. |
| 5 | `da_sell_50mw`, `da_sell_100mw` | `supply_mw_from_clearing`, `supply_price_from_clearing` | Wall location and height instead of fixed-offset price. |
| 6 | `da_sell_steepness_ratio` | `supply_slope` (€/MWh per MW) | Slope is the natural ratio in the new schema. |
| 7 | `da_clearing_price` | `da_period_summary.clearing_price` (or `da_curve_depth.clearing_price`) | Unchanged. |

---

## 8. Cron Schedule

DAM curves are downloaded and uploaded every 2h from **14:30** to **20:30** (auction publishes ~13:00; the extra slots are retries on failure):

```
30 14,16,18,20 * * * cd /app/scripts && python3 download_dam_curves.py
```

`download_dam_curves.py` runs in AUTO mode: finds last downloaded file, downloads
missing dates up to tomorrow (D+1), then automatically calls `upload_dam_curves.py`
which upserts `da_bid`, recomputes `da_period_summary`, and recomputes `da_curve_depth`.
