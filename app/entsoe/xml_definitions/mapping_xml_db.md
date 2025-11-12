# ENTSO-E XML to Database Mapping

## Mapping Table: XML Elements to Database Columns

| Database Column | XML Source | Description |
|-----------------|------------|-------------|
| **trade_date** | `Period/timeInterval/start` (date part) | Extract date from start timestamp (e.g., 2025-11-12) |
| **period** | Calculate: `(hour × 4) + (minute ÷ 15) + 1` | Period number for the day (1-96 for 15-min intervals) |
| **time_interval** | Calculate from `Point/position` + `resolution` | Format as "HH:MM-HH:MM" (e.g., "13:45-14:00") |
| **pos_imb_price_czk_mwh** | `TimeSeries[category=A04]/Point/imbalance_Price.amount` (A85) | Excess balance price (surplus situation) |
| **pos_imb_scarcity_czk_mwh** | `TimeSeries[category=A04]/Point/Financial_Price[type=A01]/amount` (A85) | Scarcity component for excess (default 0 if missing) |
| **pos_imb_incentive_czk_mwh** | `TimeSeries[category=A04]/Point/Financial_Price[type=A02]/amount` (A85) | Incentive component for excess (default 0 if missing) |
| **pos_imb_financial_neutrality_czk_mwh** | `TimeSeries[category=A04]/Point/Financial_Price[type=A03]/amount` (A85) | Financial neutrality for excess (default 0 if missing) |
| **neg_imb_price_czk_mwh** | `TimeSeries[category=A05]/Point/imbalance_Price.amount` (A85) | Insufficient balance price (deficit situation) |
| **neg_imb_scarcity_czk_mwh** | `TimeSeries[category=A05]/Point/Financial_Price[type=A01]/amount` (A85) | Scarcity component for insufficient (default 0 if missing) |
| **neg_imb_incentive_czk_mwh** | `TimeSeries[category=A05]/Point/Financial_Price[type=A02]/amount` (A85) | Incentive component for insufficient (default 0 if missing) |
| **neg_imb_financial_neutrality_czk_mwh** | `TimeSeries[category=A05]/Point/Financial_Price[type=A03]/amount` (A85) | Financial neutrality for insufficient (default 0 if missing) |
| **imbalance_mwh** | `TimeSeries/Point/quantity` (A86) | Total imbalance volume in MWh |
| **difference_mwh** | `TimeSeries/Point/secondaryQuantity` (A86) | Difference between measured and scheduled flows |
| **situation** | `TimeSeries/flowDirection.direction` (A86) | "surplus" (A01) or "deficit" (A02) or "balanced" (A03) |
| **status** | `docStatus/value` (A85 or A86) | "A01" (Intermediate) or "A02" (Final) or "A13" (Withdrawn) |

### Code Definitions

**From A85 (Imbalance Prices):**
- **A04** = Excess balance (positive imbalance / surplus)
- **A05** = Insufficient balance (negative imbalance / deficit)
- **A01** (priceDescriptor.type) = Scarcity component
- **A02** (priceDescriptor.type) = Incentive component
- **A03** (priceDescriptor.type) = Financial neutrality component

**From A86 (Imbalance Volumes):**
- **A01** (flowDirection.direction) = Surplus
- **A02** (flowDirection.direction) = Deficit
- **A03** (flowDirection.direction) = Balanced (when quantity is 0)

**Status Codes:**
- **A01** = Intermediate
- **A02** = Final
- **A13** = Withdrawn

### Period Calculation Formula

For 15-minute intervals starting at 00:00 as period 1:

```
period = (hour × 4) + (minute ÷ 15) + 1

Examples:
- 00:00 → (0 × 4) + (0 ÷ 15) + 1 = 1
- 00:15 → (0 × 4) + (15 ÷ 15) + 1 = 2
- 13:45 → (13 × 4) + (45 ÷ 15) + 1 = 56
- 14:00 → (14 × 4) + (0 ÷ 15) + 1 = 57
- 14:15 → (14 × 4) + (15 ÷ 15) + 1 = 58
- 14:30 → (14 × 4) + (30 ÷ 15) + 1 = 59
- 23:45 → (23 × 4) + (45 ÷ 15) + 1 = 96
```

---

## Example Data Mapping

### Source Files
- **Prices:** `entsoe_imbalance_prices_20251112_1345_1445.xml`
- **Volumes:** `entsoe_imbalance_volumes_20251112_1345_1445.xml`

### Mapped Data Table

| trade_date | period | time_interval | pos_imb_price | pos_scarcity | pos_incentive | pos_neutrality | neg_imb_price | neg_scarcity | neg_incentive | neg_neutrality | imbalance_mwh | difference_mwh | situation | status |
|------------|--------|---------------|---------------|--------------|---------------|----------------|---------------|--------------|---------------|----------------|---------------|----------------|-----------|--------|
| 2025-11-12 | 56 | 13:45-14:00 | 3416.380 | 0.000 | 0.000 | 0.000 | 3416.380 | 0.000 | 0.000 | 0.000 | 8.76000 | NULL | deficit | A01 |
| 2025-11-12 | 57 | 14:00-14:15 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 69.64000 | NULL | surplus | A01 |
| 2025-11-12 | 58 | 14:15-14:30 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.00000 | 0.00000 | surplus | A01 |
| 2025-11-12 | 59 | 14:30-14:45 | 4649.630 | 0.000 | 0.000 | 0.000 | 4649.630 | 0.000 | 0.000 | 0.000 | NULL | NULL | NULL | A01 |

### Data Sources Breakdown

**Period 56 (13:45-14:00):**
- **Prices (A85):** TimeSeries[1]/Point[position=1] (A04) + TimeSeries[2]/Point[position=1] (A05)
- **Volumes (A86):** TimeSeries[1]/Point[position=1] with flowDirection=A02 (deficit), quantity=8.76

**Period 57 (14:00-14:15):**
- **Prices (A85):** TimeSeries[1]/Point[position=2] (A04) + TimeSeries[2]/Point[position=2] (A05)
- **Volumes (A86):** TimeSeries[2]/Point[position=1] with flowDirection=A01 (surplus), quantity=69.64

**Period 58 (14:15-14:30):**
- **Data missing** - no Point with position=3 in either document. Filled with zeros and situation="surplus"

**Period 59 (14:30-14:45):**
- **Prices (A85):** TimeSeries[1]/Point[position=4] (A04) + TimeSeries[2]/Point[position=4] (A05)
- **Volumes (A86):** No volume data for this period

### Important Notes

1. **Position vs Period:** The XML `position` is relative to the Period's start time, NOT the day. Calculate absolute period using the Period's `timeInterval/start`.

2. **Missing Data:** When data is missing for a time interval, fill the entire row with 0 values and set situation="surplus".

3. **Data Alignment:** Prices (A85) and Volumes (A86) must be joined by matching `trade_date`, `period`, and `time_interval`.

4. **Situation Logic:**
   - Use `flowDirection.direction` from A86 document
   - A01 = "surplus" (excess generation)
   - A02 = "deficit" (insufficient generation)
   - When quantity=0, situation should be "balanced"

5. **Financial Components:** In this dataset, all Financial_Price components are 0, but they should still be stored as they can have values in other periods.

6. **NULL Handling:** When volume data is missing but price data exists (like period 59), keep volume fields as NULL. When both prices and volumes are missing (like period 58), fill entire row with 0 and situation="surplus".