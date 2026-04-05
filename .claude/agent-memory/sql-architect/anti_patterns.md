---
name: Anti-Patterns
description: Recurring DB anti-patterns found in ws-finance codebase during 2026-04-03 audit
type: project
---

## Critical
1. search_path via SET (session-level) is incompatible with pgbouncer transaction-mode pooling.
   All queries in upload scripts use SET search_path TO finance at connection time, then switch
   autocommit back. In pgbouncer transaction mode the search_path set in one transaction does NOT
   persist to the next because the connection may be reassigned. Mitigation: schema-qualify all
   table names OR set search_path in the pgbouncer config (server_reset_query).

2. No timezone on most DateTime columns. Models use SQLAlchemy DateTime (maps to TIMESTAMP WITHOUT
   TIME ZONE) for created_at, delivery_datetime (EntsoeCrossBorderFlows), delivery_timestamp
   (CepsActualRePrice1Min), updated_at. Only EntsoeImbalancePrices.delivery_datetime uses
   TIMESTAMP(timezone=True). Project TZ is Europe/Prague — DST transitions create ambiguous timestamps
   in naive columns.

3. OteDailyPayments.updated_at has server_default='CURRENT_TIMESTAMP' but no ON UPDATE trigger.
   The column never actually updates, misleading consumers.

## Warning
4. da_bid table has redundant indexes: idx_da_bid_delivery_date is a prefix of
   idx_da_bid_delivery_date_period. The single-column index is never faster than the composite.

5. Aggregation queries in ceps_soap_uploader.py and preprocess_ceps_data.py use
   WHERE (trade_date, time_interval) IN %s with a Python tuple. At large backfill sizes
   this tuple can exceed 65535 parameters, causing psycopg2 errors. Should use a date-range
   filter (WHERE delivery_timestamp BETWEEN min_date AND max_date + 1 day) instead — the
   IN %s filter on affected_intervals is already applied, but the outer range filter alone
   would work correctly given the bounded date window.

12. upload_dam_curves.py compute_and_upsert_period_summary passes delivery_date 5 times as
    separate %s parameters (lines 311). Could use a single $1 with repeated reference, but
    this is a minor clarity issue in psycopg2-style queries.

13. upload_imbalance_prices.py uses plain INSERT (no ON CONFLICT). Re-running on an already-
    uploaded date raises IntegrityError rather than silently succeeding. All other OTE uploaders
    use UPSERT. This asymmetry means manual intervention is required to re-process any day.

14. ceps_consistency_check.py calls get_summary_stats_1min and get_missing_dates_1min twice per
    dataset in the OVERALL SUMMARY loop (lines 406-415), even though those results were already
    computed and printed earlier. Each call is a full table scan; wasted round-trips on large
    CEPS tables.

15. aggregate_derived_features in preprocess_ceps_data.py builds lookback_dates by including the
    previous day of each affected date. This is correct for rolling windows, but the lookback
    tuple is passed to the IN clause for a table without a composite index on (trade_date,
    time_interval). The window function ORDER BY relies on sort, not an index — acceptable but
    worth noting for future index planning.

16. ARRAY_AGG(...ORDER BY delivery_timestamp DESC)[1] pattern is used in 5 aggregation functions
    in ceps_soap_uploader.py to get the last value per interval. This builds a full sorted array
    in memory and discards all but element 0. LAST_VALUE() window function or
    DISTINCT ON with ORDER BY DESC would be cleaner, but within an aggregate GROUP BY context
    the ARRAY_AGG pattern is a legitimate PostgreSQL idiom.

17. plot_dam_curves.py:fetch_bids fetches all columns including volume_matched which is only used
    to identify matched vs unmatched. The query is correct but SELECT * style (side, price,
    volume_bid, volume_matched) — acceptable for a plotting utility.

6. No connection pooling at the application layer. Each upload script opens a raw psycopg2
   connection and closes it. Under cron concurrency (8 jobs staggered), DB has up to 8
   simultaneous connections direct to pgbouncer. No max_connections guard in application code.

7. Missing trade_date index on entsoe_generation_forecast (unpartitioned CZ-only table).
   The partition migration (019) creates an index only on the partitioned version.

8. Consistency check queries use fully schema-qualified table names (finance.table_name)
   while upload scripts use SET search_path + unqualified names — inconsistency but not a bug.

## Suggestion
9. da_bid PK includes price (Numeric 10,2). Floating-point price equality in a PK is fragile —
   Decimal is used but be aware that any upstream source that produces differing precision will
   create duplicate logical rows.

10. time_interval stored as VARCHAR(11) "HH:MM-HH:MM" in every 15-min table (20+ occurrences).
    This is a derived column that can always be computed from trade_date+period. Storing it wastes
    ~15 bytes per row and creates a consistency risk. For da_bid (~17k rows/day) this is ~250KB/day
    of redundant storage.

11. entsoe_load, entsoe_balancing_energy, entsoe_generation_scheduled, entsoe_generation_forecast
    are single-country (CZ) flat tables. They were never partitioned. Queries against them do not
    benefit from partition pruning. Low risk given small cardinality.
