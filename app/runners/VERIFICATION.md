# Data Verification: ENTSO-E Generation Actual

Verification queries and ENTSO-E web links for comparing database data with source.

**Backfill Period:** 2025-12-01 to 2025-12-22

---

## Quick Verification Command

Run this to get sample data for all areas:

```bash
docker compose exec entsoe-ote-data-uploader python3 -c "
import psycopg2
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT
conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=DB_PORT)
cur = conn.cursor()
cur.execute('''
    SELECT a.country_code, g.trade_date, g.period, g.time_interval,
           g.gen_nuclear_mw, g.gen_coal_mw, g.gen_gas_mw, g.gen_solar_mw,
           g.gen_wind_mw, g.gen_wind_offshore_mw
    FROM finance.entsoe_generation_actual g
    JOIN finance.entsoe_areas a ON g.area_id = a.id
    WHERE g.trade_date = '2025-12-15' AND g.period = 48
    ORDER BY a.id
''')
print('Area | Date       | Period | Interval    | Nuclear | Coal    | Gas     | Solar   | Wind    | Offshore')
print('-----|------------|--------|-------------|---------|---------|---------|---------|---------|----------')
for r in cur.fetchall():
    print(f'{r[0]:4} | {r[1]} | {r[2]:6} | {r[3]:11} | {str(r[4]):7} | {str(r[5]):7} | {str(r[6]):7} | {str(r[7]):7} | {str(r[8]):7} | {str(r[9])}')
conn.close()
"
```

---

## ENTSO-E Web Platform Links

### Actual Generation per Production Type (A75)

| Area | Link |
|------|------|
| **CZ** | [Czech Republic](https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&datepicker-day-offset-select-dv-date-from_input=D&dateTime.dateTime=15.12.2025+00:00\|CET\|DAYTIMERANGE&dateTime.endDateTime=15.12.2025+00:00\|CET\|DAYTIMERANGE&area.values=CTY\|10YCZ-CEPS-----N!BZN\|10YCZ-CEPS-----N&productionType.values=B01&productionType.values=B02&productionType.values=B03&productionType.values=B04&productionType.values=B05&productionType.values=B06&productionType.values=B09&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B13&productionType.values=B14&productionType.values=B15&productionType.values=B16&productionType.values=B17&productionType.values=B18&productionType.values=B19&productionType.values=B20&dateTime.timezone=CET\|CET&dateTime.timezone_input=CET+(UTC+1)) |
| **DE** | [Germany (TenneT)](https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&datepicker-day-offset-select-dv-date-from_input=D&dateTime.dateTime=15.12.2025+00:00\|CET\|DAYTIMERANGE&dateTime.endDateTime=15.12.2025+00:00\|CET\|DAYTIMERANGE&area.values=CTY\|10YDE-EON------1!BZN\|10YDE-EON------1&productionType.values=B01&productionType.values=B02&productionType.values=B03&productionType.values=B04&productionType.values=B05&productionType.values=B06&productionType.values=B09&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B13&productionType.values=B14&productionType.values=B15&productionType.values=B16&productionType.values=B17&productionType.values=B18&productionType.values=B19&productionType.values=B20&dateTime.timezone=CET\|CET&dateTime.timezone_input=CET+(UTC+1)) |
| **AT** | [Austria](https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&datepicker-day-offset-select-dv-date-from_input=D&dateTime.dateTime=15.12.2025+00:00\|CET\|DAYTIMERANGE&dateTime.endDateTime=15.12.2025+00:00\|CET\|DAYTIMERANGE&area.values=CTY\|10YAT-APG------L!BZN\|10YAT-APG------L&productionType.values=B01&productionType.values=B02&productionType.values=B03&productionType.values=B04&productionType.values=B05&productionType.values=B06&productionType.values=B09&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B13&productionType.values=B14&productionType.values=B15&productionType.values=B16&productionType.values=B17&productionType.values=B18&productionType.values=B19&productionType.values=B20&dateTime.timezone=CET\|CET&dateTime.timezone_input=CET+(UTC+1)) |
| **PL** | [Poland](https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&datepicker-day-offset-select-dv-date-from_input=D&dateTime.dateTime=15.12.2025+00:00\|CET\|DAYTIMERANGE&dateTime.endDateTime=15.12.2025+00:00\|CET\|DAYTIMERANGE&area.values=CTY\|10YPL-AREA-----S!BZN\|10YPL-AREA-----S&productionType.values=B01&productionType.values=B02&productionType.values=B03&productionType.values=B04&productionType.values=B05&productionType.values=B06&productionType.values=B09&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B13&productionType.values=B14&productionType.values=B15&productionType.values=B16&productionType.values=B17&productionType.values=B18&productionType.values=B19&productionType.values=B20&dateTime.timezone=CET\|CET&dateTime.timezone_input=CET+(UTC+1)) |
| **SK** | [Slovakia](https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&datepicker-day-offset-select-dv-date-from_input=D&dateTime.dateTime=15.12.2025+00:00\|CET\|DAYTIMERANGE&dateTime.endDateTime=15.12.2025+00:00\|CET\|DAYTIMERANGE&area.values=CTY\|10YSK-SEPS-----K!BZN\|10YSK-SEPS-----K&productionType.values=B01&productionType.values=B02&productionType.values=B03&productionType.values=B04&productionType.values=B05&productionType.values=B06&productionType.values=B09&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B13&productionType.values=B14&productionType.values=B15&productionType.values=B16&productionType.values=B17&productionType.values=B18&productionType.values=B19&productionType.values=B20&dateTime.timezone=CET\|CET&dateTime.timezone_input=CET+(UTC+1)) |

> **Note:** Change the date in the URL (`15.12.2025`) to match your test date.

---

## SQL Verification Queries

### 1. Czech Republic (CZ) - area_id=1

**Test Date: 2025-12-05, Period 32 (08:00 CET)**

```sql
SELECT trade_date, period, time_interval,
       gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
       gen_wind_mw, gen_hydro_pumped_mw, gen_biomass_mw, gen_hydro_other_mw
FROM finance.entsoe_generation_actual
WHERE area_id = 1 AND trade_date = '2025-12-05' AND period = 32;
```

**ENTSO-E Link:** [CZ - Dec 5, 2025](https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&dateTime.dateTime=05.12.2025+00:00|CET|DAYTIMERANGE&dateTime.endDateTime=05.12.2025+00:00|CET|DAYTIMERANGE&area.values=CTY|10YCZ-CEPS-----N!BZN|10YCZ-CEPS-----N&productionType.values=B01&productionType.values=B02&productionType.values=B04&productionType.values=B05&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B14&productionType.values=B16&productionType.values=B19&dateTime.timezone=CET|CET)

---

### 2. Germany TenneT (DE) - area_id=2

**Test Date: 2025-12-10, Period 48 (12:00 CET)**

```sql
SELECT trade_date, period, time_interval,
       gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
       gen_wind_mw, gen_wind_offshore_mw, gen_hydro_pumped_mw, gen_biomass_mw
FROM finance.entsoe_generation_actual
WHERE area_id = 2 AND trade_date = '2025-12-10' AND period = 48;
```

**ENTSO-E Link:** [DE TenneT - Dec 10, 2025](https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&dateTime.dateTime=10.12.2025+00:00|CET|DAYTIMERANGE&dateTime.endDateTime=10.12.2025+00:00|CET|DAYTIMERANGE&area.values=CTY|10YDE-EON------1!BZN|10YDE-EON------1&productionType.values=B01&productionType.values=B02&productionType.values=B04&productionType.values=B05&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B14&productionType.values=B16&productionType.values=B18&productionType.values=B19&dateTime.timezone=CET|CET)

---

### 3. Austria (AT) - area_id=3

**Test Date: 2025-12-15, Period 64 (16:00 CET)**

```sql
SELECT trade_date, period, time_interval,
       gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
       gen_wind_mw, gen_hydro_pumped_mw, gen_biomass_mw, gen_hydro_other_mw
FROM finance.entsoe_generation_actual
WHERE area_id = 3 AND trade_date = '2025-12-15' AND period = 64;
```

**ENTSO-E Link:** [AT - Dec 15, 2025](https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&dateTime.dateTime=15.12.2025+00:00|CET|DAYTIMERANGE&dateTime.endDateTime=15.12.2025+00:00|CET|DAYTIMERANGE&area.values=CTY|10YAT-APG------L!BZN|10YAT-APG------L&productionType.values=B01&productionType.values=B02&productionType.values=B04&productionType.values=B05&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B14&productionType.values=B16&productionType.values=B19&dateTime.timezone=CET|CET)

---

### 4. Poland (PL) - area_id=4

**Test Date: 2025-12-08, Period 20 (05:00 CET)**

```sql
SELECT trade_date, period, time_interval,
       gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
       gen_wind_mw, gen_hydro_pumped_mw, gen_biomass_mw, gen_hydro_other_mw
FROM finance.entsoe_generation_actual
WHERE area_id = 4 AND trade_date = '2025-12-08' AND period = 20;
```

**ENTSO-E Link:** [PL - Dec 8, 2025](https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&dateTime.dateTime=08.12.2025+00:00|CET|DAYTIMERANGE&dateTime.endDateTime=08.12.2025+00:00|CET|DAYTIMERANGE&area.values=CTY|10YPL-AREA-----S!BZN|10YPL-AREA-----S&productionType.values=B01&productionType.values=B02&productionType.values=B04&productionType.values=B05&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B14&productionType.values=B16&productionType.values=B19&dateTime.timezone=CET|CET)

---

### 5. Slovakia (SK) - area_id=5

**Test Date: 2025-12-20, Period 80 (20:00 CET)**

```sql
SELECT trade_date, period, time_interval,
       gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
       gen_wind_mw, gen_hydro_pumped_mw, gen_biomass_mw, gen_hydro_other_mw
FROM finance.entsoe_generation_actual
WHERE area_id = 5 AND trade_date = '2025-12-20' AND period = 80;
```

**ENTSO-E Link:** [SK - Dec 20, 2025](https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&dateTime.dateTime=20.12.2025+00:00|CET|DAYTIMERANGE&dateTime.endDateTime=20.12.2025+00:00|CET|DAYTIMERANGE&area.values=CTY|10YSK-SEPS-----K!BZN|10YSK-SEPS-----K&productionType.values=B01&productionType.values=B02&productionType.values=B04&productionType.values=B05&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B14&productionType.values=B16&productionType.values=B19&dateTime.timezone=CET|CET)

---

## Period to Time Mapping

| Period | CET Time | Period | CET Time | Period | CET Time | Period | CET Time |
|--------|----------|--------|----------|--------|----------|--------|----------|
| 1 | 00:00-00:15 | 25 | 06:00-06:15 | 49 | 12:00-12:15 | 73 | 18:00-18:15 |
| 2 | 00:15-00:30 | 26 | 06:15-06:30 | 50 | 12:15-12:30 | 74 | 18:15-18:30 |
| 3 | 00:30-00:45 | 27 | 06:30-06:45 | 51 | 12:30-12:45 | 75 | 18:30-18:45 |
| 4 | 00:45-01:00 | 28 | 06:45-07:00 | 52 | 12:45-13:00 | 76 | 18:45-19:00 |
| ... | ... | ... | ... | ... | ... | ... | ... |
| 32 | 08:00-08:15 | 48 | 12:00 (noon) | 64 | 16:00-16:15 | 80 | 20:00-20:15 |

**Formula:** `Period = (Hour * 4) + (Minute / 15) + 1`

---

## Column Mapping: Database → ENTSO-E

| Database Column | ENTSO-E PSR Types | Description |
|-----------------|-------------------|-------------|
| `gen_nuclear_mw` | B14 | Nuclear |
| `gen_coal_mw` | B02 + B05 | Brown coal/Lignite + Hard coal |
| `gen_gas_mw` | B04 | Fossil Gas |
| `gen_solar_mw` | B16 | Solar |
| `gen_wind_mw` | B19 | Wind Onshore |
| `gen_wind_offshore_mw` | B18 | Wind Offshore |
| `gen_hydro_pumped_mw` | B10 | Hydro Pumped Storage |
| `gen_biomass_mw` | B01 | Biomass |
| `gen_hydro_other_mw` | B11 + B12 | Run-of-river + Water Reservoir |

---

## Batch Verification Query (All Areas, Multiple Dates)

```sql
SELECT
    a.country_code,
    g.trade_date,
    g.period,
    g.time_interval,
    ROUND(g.gen_nuclear_mw::numeric, 0) as nuclear,
    ROUND(g.gen_coal_mw::numeric, 0) as coal,
    ROUND(g.gen_gas_mw::numeric, 0) as gas,
    ROUND(g.gen_solar_mw::numeric, 0) as solar,
    ROUND(g.gen_wind_mw::numeric, 0) as wind,
    ROUND(g.gen_wind_offshore_mw::numeric, 0) as offshore
FROM finance.entsoe_generation_actual g
JOIN finance.entsoe_areas a ON g.area_id = a.id
WHERE (g.trade_date, g.period) IN (
    ('2025-12-05', 32),  -- 08:00
    ('2025-12-10', 48),  -- 12:00
    ('2025-12-15', 64),  -- 16:00
    ('2025-12-20', 80)   -- 20:00
)
ORDER BY g.trade_date, g.period, a.id;
```

---

## Data Quality Checks

### Check for Missing Data

```sql
-- Count records per area per day
SELECT
    a.country_code,
    g.trade_date,
    COUNT(*) as periods,
    CASE WHEN COUNT(*) = 96 THEN 'OK' ELSE 'MISSING' END as status
FROM finance.entsoe_generation_actual g
JOIN finance.entsoe_areas a ON g.area_id = a.id
WHERE g.trade_date BETWEEN '2025-12-01' AND '2025-12-22'
GROUP BY a.country_code, g.trade_date
ORDER BY g.trade_date, a.country_code;
```

### Check for NULL Values

```sql
-- Find records with all generation columns NULL (suspicious)
SELECT a.country_code, g.trade_date, g.period
FROM finance.entsoe_generation_actual g
JOIN finance.entsoe_areas a ON g.area_id = a.id
WHERE g.gen_nuclear_mw IS NULL
  AND g.gen_coal_mw IS NULL
  AND g.gen_gas_mw IS NULL
  AND g.gen_solar_mw IS NULL
  AND g.gen_wind_mw IS NULL
  AND g.gen_hydro_pumped_mw IS NULL
  AND g.gen_biomass_mw IS NULL
  AND g.gen_hydro_other_mw IS NULL
ORDER BY g.trade_date, g.period, a.id;
```

### Summary Statistics

```sql
SELECT
    a.country_code,
    COUNT(*) as total_records,
    MIN(g.trade_date) as first_date,
    MAX(g.trade_date) as last_date,
    ROUND(AVG(g.gen_nuclear_mw)::numeric, 0) as avg_nuclear,
    ROUND(AVG(g.gen_coal_mw)::numeric, 0) as avg_coal,
    ROUND(AVG(g.gen_solar_mw)::numeric, 0) as avg_solar,
    ROUND(AVG(g.gen_wind_mw)::numeric, 0) as avg_wind
FROM finance.entsoe_generation_actual g
JOIN finance.entsoe_areas a ON g.area_id = a.id
GROUP BY a.country_code
ORDER BY a.country_code;
```
