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
da_curve_depth      shape          how steep is the curve beyond clearing (960 rows/day)
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

## 5. `da_curve_depth` — Curve Shape at Fixed MW Offsets

One row per period × side × offset_mw (960/day = 96 periods × 2 sides × 5 offsets).

| Column | Meaning |
|--------|---------|
| `side` | `sell` = supply curve above MCP / `buy` = demand curve below MCP |
| `offset_mw` | How many MW beyond clearing we are asking about |
| `price_at_offset` | Price of the bid where cumulative unmatched volume first reaches `offset_mw`. NULL if curve exhausted. |
| `volume_available` | Total unmatched volume on this side — tells you when curve runs out |

### Offsets

Defined in `CURVE_DEPTH_OFFSETS_MW = [50, 100, 200, 500, 1000]` in `upload_dam_curves.py`.
Adding new offsets requires only changing this constant — no schema migration needed.

| Offset | Represents |
|--------|-----------|
| 50 MW | Small aFRR activation |
| 100 MW | Moderate aFRR / small mFRR |
| 200 MW | Large mFRR activation |
| 500 MW | Major grid event |
| 1000 MW | Extreme scenario (large plant outage) |

### NULL semantics

`price_at_offset IS NULL` means total `volume_available` is less than `offset_mw`.
The curve runs out before reaching that depth. Use `volume_available` to understand how
much supply actually exists regardless of offset.

### Example: Jan 12 2026, period 73 (18:00–18:15)

Clearing price: 241.27 EUR. `supply_price_gap` = 28.65 EUR — looks modest.
But `da_curve_depth` reveals the cliff:

```
Price (EUR)
  1000 |                  ████████████████████  (1718 MW available)
       |                  |
   270 |           █      |
   241 |───────────┤ MCP  |
       |           |      |
       +-----------+------+----> Cumulative unmatched volume (MW)
                  +50    +100
```

At just +50 MW, price jumps to 1000 EUR. `da_period_summary` alone missed this entirely.
Imbalance price for this period: **301,455 CZK/MWh**.

---

## 6. Analysis Results (2026-01-01 to 2026-03-04)

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

All DA features use **target period** as lag (D-1 publication, no lag needed).

| Feature | Source | Priority |
|---------|--------|---------|
| `intraday_premium x da_sell_100mw` | derived | 1 — best single signal |
| `intraday_premium` | derived from intraday vwap - clearing | 2 |
| `da_supply_gap_zero` | `da_period_summary.supply_volume_gap = 0` | 3 |
| `da_sell_100mw x is_business_hour` | derived | 4 |
| `da_sell_50mw`, `da_sell_100mw` | `da_curve_depth` | 5 |
| `da_sell_steepness_ratio` | `da_sell_100mw / da_clearing_price` | 6 |
| `da_clearing_price` | `da_period_summary` | 7 |

---

## 8. Cron Schedule

DAM curves are downloaded and uploaded daily at **14:30** (auction publishes ~13:00):

```
30 14 * * * cd /app/scripts && python3 download_dam_curves.py
```

`download_dam_curves.py` runs in AUTO mode: finds last downloaded file, downloads
missing dates up to tomorrow (D+1), then automatically calls `upload_dam_curves.py`
which upserts `da_bid`, recomputes `da_period_summary`, and recomputes `da_curve_depth`.
