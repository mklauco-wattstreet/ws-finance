# CEPS Database Schema

All tables in `finance` schema. Timestamps are naive (Europe/Prague local time).

---

## 1. System Imbalance

### ceps_actual_imbalance_1min
```sql
CREATE TABLE finance.ceps_actual_imbalance_1min (
    id BIGSERIAL,
    delivery_timestamp TIMESTAMP NOT NULL,
    load_mw NUMERIC(12,5) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_1min_delivery_timestamp UNIQUE (delivery_timestamp)
) PARTITION BY RANGE (delivery_timestamp);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

### ceps_actual_imbalance_15min
```sql
CREATE TABLE finance.ceps_actual_imbalance_15min (
    id BIGSERIAL,
    trade_date DATE NOT NULL,
    time_interval VARCHAR(11) NOT NULL,  -- "HH:MM-HH:MM"
    load_mean_mw NUMERIC(12,5),
    load_median_mw NUMERIC(12,5),
    last_load_at_interval_mw NUMERIC(12,5),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_15min_trade_date_interval UNIQUE (trade_date, time_interval)
) PARTITION BY RANGE (trade_date);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

---

## 2. RE Price (Balancing Energy Prices)

### ceps_actual_re_price_1min
```sql
CREATE TABLE finance.ceps_actual_re_price_1min (
    id BIGSERIAL,
    delivery_timestamp TIMESTAMP NOT NULL,
    price_afrr_plus_eur_mwh NUMERIC(15,3),
    price_afrr_minus_eur_mwh NUMERIC(15,3),
    price_mfrr_plus_eur_mwh NUMERIC(15,3),
    price_mfrr_minus_eur_mwh NUMERIC(15,3),
    price_mfrr_5_eur_mwh NUMERIC(15,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_re_price_1min_delivery_timestamp UNIQUE (delivery_timestamp)
) PARTITION BY RANGE (delivery_timestamp);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

### ceps_actual_re_price_15min
```sql
CREATE TABLE finance.ceps_actual_re_price_15min (
    id BIGSERIAL,
    trade_date DATE NOT NULL,
    time_interval VARCHAR(11) NOT NULL,
    price_afrr_plus_mean_eur_mwh NUMERIC(15,3),
    price_afrr_minus_mean_eur_mwh NUMERIC(15,3),
    price_mfrr_plus_mean_eur_mwh NUMERIC(15,3),
    price_mfrr_minus_mean_eur_mwh NUMERIC(15,3),
    price_mfrr_5_mean_eur_mwh NUMERIC(15,3),
    price_afrr_plus_median_eur_mwh NUMERIC(15,3),
    price_afrr_minus_median_eur_mwh NUMERIC(15,3),
    price_mfrr_plus_median_eur_mwh NUMERIC(15,3),
    price_mfrr_minus_median_eur_mwh NUMERIC(15,3),
    price_mfrr_5_median_eur_mwh NUMERIC(15,3),
    price_afrr_plus_last_at_interval_eur_mwh NUMERIC(15,3),
    price_afrr_minus_last_at_interval_eur_mwh NUMERIC(15,3),
    price_mfrr_plus_last_at_interval_eur_mwh NUMERIC(15,3),
    price_mfrr_minus_last_at_interval_eur_mwh NUMERIC(15,3),
    price_mfrr_5_last_at_interval_eur_mwh NUMERIC(15,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_re_price_15min_trade_date_interval UNIQUE (trade_date, time_interval)
) PARTITION BY RANGE (trade_date);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

---

## 3. SVR Activation (Reserve Activation)

### ceps_svr_activation_1min
```sql
CREATE TABLE finance.ceps_svr_activation_1min (
    id BIGSERIAL,
    delivery_timestamp TIMESTAMP NOT NULL,
    afrr_plus_mw NUMERIC(15,3),
    afrr_minus_mw NUMERIC(15,3),
    mfrr_plus_mw NUMERIC(15,3),
    mfrr_minus_mw NUMERIC(15,3),
    mfrr_5_mw NUMERIC(15,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_svr_activation_1min_delivery_timestamp UNIQUE (delivery_timestamp)
) PARTITION BY RANGE (delivery_timestamp);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

### ceps_svr_activation_15min
```sql
CREATE TABLE finance.ceps_svr_activation_15min (
    id BIGSERIAL,
    trade_date DATE NOT NULL,
    time_interval VARCHAR(11) NOT NULL,
    afrr_plus_mean_mw NUMERIC(15,3),
    afrr_minus_mean_mw NUMERIC(15,3),
    mfrr_plus_mean_mw NUMERIC(15,3),
    mfrr_minus_mean_mw NUMERIC(15,3),
    mfrr_5_mean_mw NUMERIC(15,3),
    afrr_plus_median_mw NUMERIC(15,3),
    afrr_minus_median_mw NUMERIC(15,3),
    mfrr_plus_median_mw NUMERIC(15,3),
    mfrr_minus_median_mw NUMERIC(15,3),
    mfrr_5_median_mw NUMERIC(15,3),
    afrr_plus_last_at_interval_mw NUMERIC(15,3),
    afrr_minus_last_at_interval_mw NUMERIC(15,3),
    mfrr_plus_last_at_interval_mw NUMERIC(15,3),
    mfrr_minus_last_at_interval_mw NUMERIC(15,3),
    mfrr_5_last_at_interval_mw NUMERIC(15,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_svr_activation_15min_trade_date_interval UNIQUE (trade_date, time_interval)
) PARTITION BY RANGE (trade_date);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

---

## 4. Export/Import SVR (Cross-Border Balancing)

### ceps_export_import_svr_1min
```sql
CREATE TABLE finance.ceps_export_import_svr_1min (
    id BIGSERIAL,
    delivery_timestamp TIMESTAMP NOT NULL,
    imbalance_netting_mw NUMERIC(15,5),
    mari_mfrr_mw NUMERIC(15,5),
    picasso_afrr_mw NUMERIC(15,5),
    sum_exchange_european_platforms_mw NUMERIC(15,5),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_export_import_svr_1min_delivery_timestamp UNIQUE (delivery_timestamp)
) PARTITION BY RANGE (delivery_timestamp);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

### ceps_export_import_svr_15min
```sql
CREATE TABLE finance.ceps_export_import_svr_15min (
    id BIGSERIAL,
    trade_date DATE NOT NULL,
    time_interval VARCHAR(11) NOT NULL,
    imbalance_netting_mean_mw NUMERIC(15,5),
    mari_mfrr_mean_mw NUMERIC(15,5),
    picasso_afrr_mean_mw NUMERIC(15,5),
    sum_exchange_mean_mw NUMERIC(15,5),
    imbalance_netting_median_mw NUMERIC(15,5),
    mari_mfrr_median_mw NUMERIC(15,5),
    picasso_afrr_median_mw NUMERIC(15,5),
    sum_exchange_median_mw NUMERIC(15,5),
    imbalance_netting_last_at_interval_mw NUMERIC(15,5),
    mari_mfrr_last_at_interval_mw NUMERIC(15,5),
    picasso_afrr_last_at_interval_mw NUMERIC(15,5),
    sum_exchange_last_at_interval_mw NUMERIC(15,5),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_export_import_svr_15min_trade_date_interval UNIQUE (trade_date, time_interval)
) PARTITION BY RANGE (trade_date);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

---

## 5. Generation RES (Renewable Energy Sources)

### ceps_generation_res_1min
```sql
CREATE TABLE finance.ceps_generation_res_1min (
    id BIGSERIAL,
    delivery_timestamp TIMESTAMP NOT NULL,
    wind_mw NUMERIC(12,3),    -- VTE
    solar_mw NUMERIC(12,3),   -- FVE
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_generation_res_1min_delivery_timestamp UNIQUE (delivery_timestamp)
) PARTITION BY RANGE (delivery_timestamp);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

### ceps_generation_res_15min
```sql
CREATE TABLE finance.ceps_generation_res_15min (
    id BIGSERIAL,
    trade_date DATE NOT NULL,
    time_interval VARCHAR(11) NOT NULL,
    wind_mean_mw NUMERIC(12,3),
    wind_median_mw NUMERIC(12,3),
    wind_last_at_interval_mw NUMERIC(12,3),
    solar_mean_mw NUMERIC(12,3),
    solar_median_mw NUMERIC(12,3),
    solar_last_at_interval_mw NUMERIC(12,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_generation_res_15min_trade_date_interval UNIQUE (trade_date, time_interval)
) PARTITION BY RANGE (trade_date);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

---

## 6. Generation (Actual by Plant Type)

Total gross electricity generation by power plant type. Native 15-min data.

**Plant Types:** TPP (Thermal), CCGT (Gas Turbine), NPP (Nuclear), HPP (Hydro), PsPP (Pumped-Storage), AltPP (Alternative), ApPP (Autoproducer - canceled Oct 2014), WPP (Wind), PVPP (Photovoltaic)

### ceps_generation_15min
```sql
CREATE TABLE finance.ceps_generation_15min (
    id BIGSERIAL,
    trade_date DATE NOT NULL,
    time_interval VARCHAR(11) NOT NULL,
    tpp_mw NUMERIC(12,3),
    ccgt_mw NUMERIC(12,3),
    npp_mw NUMERIC(12,3),
    hpp_mw NUMERIC(12,3),
    pspp_mw NUMERIC(12,3),
    altpp_mw NUMERIC(12,3),
    appp_mw NUMERIC(12,3),
    wpp_mw NUMERIC(12,3),
    pvpp_mw NUMERIC(12,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_generation_15min_trade_date_interval UNIQUE (trade_date, time_interval)
) PARTITION BY RANGE (trade_date);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

---

## 7. Generation Plan (Total Planned)

Planned total generation. Native 15-min data.

### ceps_generation_plan_15min
```sql
CREATE TABLE finance.ceps_generation_plan_15min (
    id BIGSERIAL,
    trade_date DATE NOT NULL,
    time_interval VARCHAR(11) NOT NULL,
    total_mw NUMERIC(12,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_generation_plan_15min_trade_date_interval UNIQUE (trade_date, time_interval)
) PARTITION BY RANGE (trade_date);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

---

## 8. Estimated Imbalance Price (OdhadovanaCenaOdchylky)

Estimated deviation/imbalance price in CZK/MWh. Native 15-min data.

### ceps_estimated_imbalance_price_15min
```sql
CREATE TABLE finance.ceps_estimated_imbalance_price_15min (
    id BIGSERIAL,
    trade_date DATE NOT NULL,
    time_interval VARCHAR(11) NOT NULL,
    estimated_price_czk_mwh NUMERIC(12,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_estimated_imbalance_price_15min_trade_date_interval UNIQUE (trade_date, time_interval)
) PARTITION BY RANGE (trade_date);
-- Partitions: 2024, 2025, 2026, 2027, 2028
```

---

## Migrations

| Rev | Date | Description |
|-----|------|-------------|
| 027 | 2026-01-07 | Create imbalance tables |
| 028 | 2026-01-07 | Add last_load_at_interval_mw |
| 029 | 2026-01-07 | Convert TIMESTAMPTZ → TIMESTAMP |
| 030 | 2026-01-08 | Create RE price tables |
| 031 | 2026-01-08 | Create SVR activation tables |
| 032 | 2026-01-09 | Create export/import SVR tables |
| 033 | 2026-01-16 | Create generation RES tables |
| 034 | 2026-01-16 | Create generation + generation plan tables |
| 035 | 2026-01-17 | Create estimated imbalance price table |

---

## Data Pipeline

```bash
# Cron (every 15 min at :12, :27, :42, :57)
ceps_soap_pipeline.py --dataset all
```

Aggregation queries: `app/ceps/CEPS_AGGREGATION_QUERIES.md`
