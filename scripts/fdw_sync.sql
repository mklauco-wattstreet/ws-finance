-- =============================================================================
-- FDW SYNC: Production to Development
-- =============================================================================
-- Run this anytime on the DEV database to catch up with production.
-- Safe to run multiple times (idempotent). Never deletes local data.
--
-- Prerequisite: fdw_setup.sql has been executed successfully.
--
-- How it works:
--   - For each table, inserts rows from production that are missing locally
--   - Uses ON CONFLICT DO NOTHING to skip existing rows (no duplicates)
--   - Lookback window (default 90 days) limits the scan range for efficiency
--   - For empty local tables, syncs everything from production
--   - Excludes 'id' column (auto-generated locally)
-- =============================================================================

DO $$
DECLARE
    v_lookback_days INT := 90;
    v_cols TEXT;
    v_count BIGINT;
    v_total BIGINT := 0;
    v_cutoff TEXT;
    v_max TEXT;
    v_table TEXT;
    v_dt TEXT;
    v_cast TEXT;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'FDW SYNC START: %', now();
    RAISE NOTICE 'Lookback window: % days', v_lookback_days;
    RAISE NOTICE '========================================';

    -- ==========================================================
    -- SECTION 0: Lookup table (full replace)
    -- ==========================================================
    RAISE NOTICE '';
    RAISE NOTICE '--- Lookup Tables ---';

    TRUNCATE finance.entsoe_areas;
    INSERT INTO finance.entsoe_areas (id, code, country_name, country_code, is_active)
    SELECT id, code, country_name, country_code, is_active
    FROM prod_fdw.entsoe_areas;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_total := v_total + v_count;
    RAISE NOTICE '  entsoe_areas: % rows (full replace)', v_count;
    PERFORM setval(pg_get_serial_sequence('finance.entsoe_areas', 'id'),
                   COALESCE((SELECT MAX(id) FROM finance.entsoe_areas), 1));

    -- ==========================================================
    -- SECTION 1: Tables with unique constraints (ON CONFLICT)
    -- ==========================================================
    RAISE NOTICE '';
    RAISE NOTICE '--- Data Tables (gap-fill sync) ---';

    FOR v_table, v_dt, v_cast IN VALUES
        -- OTE tables
        ('ote_prices_day_ahead',                'trade_date',          'date'),
        ('ote_prices_imbalance',                'trade_date',          'date'),
        ('ote_prices_intraday_market',          'trade_date',          'date'),
        ('ote_trade_balance',                   'delivery_date',       'date'),
        -- ENTSO-E tables
        ('entsoe_imbalance_prices',             'trade_date',          'date'),
        ('entsoe_generation_actual',            'trade_date',          'date'),
        ('entsoe_day_ahead_prices',             'trade_date',          'date'),
        ('entsoe_load',                         'trade_date',          'date'),
        ('entsoe_generation_forecast',          'trade_date',          'date'),
        ('entsoe_balancing_energy',             'trade_date',          'date'),
        ('entsoe_generation_scheduled',         'trade_date',          'date'),
        ('entsoe_cross_border_flows',           'trade_date',          'date'),
        ('entsoe_scheduled_cross_border_flows', 'trade_date',          'date'),
        -- CEPS 1-min tables
        ('ceps_actual_imbalance_1min',          'delivery_timestamp',  'timestamp'),
        ('ceps_actual_re_price_1min',           'delivery_timestamp',  'timestamp'),
        ('ceps_svr_activation_1min',            'delivery_timestamp',  'timestamp'),
        ('ceps_export_import_svr_1min',         'delivery_timestamp',  'timestamp'),
        ('ceps_generation_res_1min',            'delivery_timestamp',  'timestamp'),
        -- CEPS 15-min tables
        ('ceps_actual_imbalance_15min',         'trade_date',          'date'),
        ('ceps_actual_re_price_15min',          'trade_date',          'date'),
        ('ceps_svr_activation_15min',           'trade_date',          'date'),
        ('ceps_export_import_svr_15min',        'trade_date',          'date'),
        ('ceps_generation_res_15min',           'trade_date',          'date'),
        ('ceps_generation_15min',               'trade_date',          'date'),
        ('ceps_generation_plan_15min',          'trade_date',          'date'),
        ('ceps_estimated_imbalance_price_15min','trade_date',          'date')
    LOOP
        SELECT string_agg(column_name, ', ' ORDER BY ordinal_position)
        INTO v_cols
        FROM information_schema.columns
        WHERE table_schema = 'finance'
          AND table_name = v_table
          AND column_name != 'id';

        IF v_cols IS NULL THEN
            RAISE NOTICE '  %: SKIPPED (no columns found)', v_table;
            CONTINUE;
        END IF;

        EXECUTE format(
            'SELECT COALESCE((MAX(%I) - INTERVAL ''%s days'')::%s, ''1970-01-01''::%s)::text FROM finance.%I',
            v_dt, v_lookback_days, v_cast, v_cast, v_table
        ) INTO v_cutoff;

        BEGIN
            EXECUTE format(
                'INSERT INTO finance.%I (%s) SELECT %s FROM prod_fdw.%I WHERE %I >= %L::%s ON CONFLICT DO NOTHING',
                v_table, v_cols, v_cols, v_table, v_dt, v_cutoff, v_cast
            );
            GET DIAGNOSTICS v_count = ROW_COUNT;
            v_total := v_total + v_count;
            RAISE NOTICE '  %: % new rows (cutoff: %)', v_table, v_count, v_cutoff;
        EXCEPTION WHEN undefined_table THEN
            RAISE NOTICE '  %: SKIPPED (foreign table not found)', v_table;
        END;
    END LOOP;

    -- ==========================================================
    -- SECTION 2: ote_daily_payments (no unique constraint, append only)
    -- ==========================================================
    RAISE NOTICE '';
    RAISE NOTICE '--- Append-only Tables ---';

    SELECT string_agg(column_name, ', ' ORDER BY ordinal_position)
    INTO v_cols
    FROM information_schema.columns
    WHERE table_schema = 'finance'
      AND table_name = 'ote_daily_payments'
      AND column_name != 'id';

    EXECUTE 'SELECT MAX(delivery_day)::text FROM finance.ote_daily_payments' INTO v_max;

    BEGIN
        EXECUTE format(
            'INSERT INTO finance.ote_daily_payments (%s) SELECT %s FROM prod_fdw.ote_daily_payments WHERE delivery_day > COALESCE(%L, ''1970-01-01'')::date',
            v_cols, v_cols, v_max
        );
        GET DIAGNOSTICS v_count = ROW_COUNT;
        v_total := v_total + v_count;
        RAISE NOTICE '  ote_daily_payments: % rows (max was: %)', v_count, COALESCE(v_max, 'empty');
    EXCEPTION WHEN undefined_table THEN
        RAISE NOTICE '  ote_daily_payments: SKIPPED (foreign table not found)';
    END;

    -- ==========================================================
    -- SUMMARY
    -- ==========================================================
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'FDW SYNC COMPLETE: % total rows synced', v_total;
    RAISE NOTICE 'Finished at: %', now();
    RAISE NOTICE '========================================';

END $$;
