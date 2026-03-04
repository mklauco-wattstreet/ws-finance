-- =============================================================================
-- FDW SYNC: Delta sync from Production to Development
-- =============================================================================
-- Run this anytime on the DEV database to catch up with production.
-- Safe to run multiple times (idempotent).
--
-- Prerequisite: fdw_setup.sql has been executed successfully.
--
-- How it works:
--   - For each table, finds MAX(datetime) in local dev table
--   - Inserts only rows from production that are newer
--   - Excludes 'id' column (auto-generated locally)
--   - Handles partitioned tables via child partitions
-- =============================================================================

DO $$
DECLARE
    v_cols TEXT;
    v_count BIGINT;
    v_total BIGINT := 0;
    v_max TEXT;
    v_table TEXT;
    v_dt TEXT;
    v_fdw_table TEXT;
    v_country TEXT;
    v_year INT;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'FDW SYNC START: %', now();
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
    -- Reset sequence to match production IDs
    PERFORM setval(pg_get_serial_sequence('finance.entsoe_areas', 'id'),
                   COALESCE((SELECT MAX(id) FROM finance.entsoe_areas), 1));

    -- ==========================================================
    -- SECTION 1: Non-partitioned tables (delta by MAX datetime)
    -- ==========================================================
    RAISE NOTICE '';
    RAISE NOTICE '--- Non-Partitioned Tables ---';

    FOR v_table, v_dt IN VALUES
        ('ote_daily_payments',         'delivery_day'),
        ('ote_prices_day_ahead',       'trade_date'),
        ('ote_prices_imbalance',       'trade_date'),
        ('ote_prices_intraday_market', 'trade_date'),
        ('ote_trade_balance',          'delivery_date')
    LOOP
        -- Get column list excluding 'id'
        SELECT string_agg(column_name, ', ' ORDER BY ordinal_position)
        INTO v_cols
        FROM information_schema.columns
        WHERE table_schema = 'finance'
          AND table_name = v_table
          AND column_name != 'id';

        -- Get current max datetime in local table
        EXECUTE format('SELECT MAX(%I)::text FROM finance.%I', v_dt, v_table)
        INTO v_max;

        -- Delta insert: only rows newer than local max
        EXECUTE format(
            'INSERT INTO finance.%I (%s) SELECT %s FROM prod_fdw.%I WHERE %I > COALESCE(%L, ''1970-01-01'')::date',
            v_table, v_cols, v_cols, v_table, v_dt, v_max
        );
        GET DIAGNOSTICS v_count = ROW_COUNT;
        v_total := v_total + v_count;
        RAISE NOTICE '  %: % rows (max was: %)', v_table, v_count, COALESCE(v_max, 'empty');
    END LOOP;

    -- ==========================================================
    -- SECTION 2: Country-partitioned ENTSO-E tables
    -- ==========================================================
    -- FDW imports child partitions as individual foreign tables
    -- (e.g., prod_fdw.entsoe_imbalance_prices_cz)
    -- We insert into the local parent; PG routes to local partitions.
    RAISE NOTICE '';
    RAISE NOTICE '--- Country-Partitioned Tables ---';

    FOR v_table, v_dt IN VALUES
        ('entsoe_imbalance_prices',              'trade_date'),
        ('entsoe_generation_actual',             'trade_date'),
        ('entsoe_day_ahead_prices',              'trade_date'),
        ('entsoe_load',                          'trade_date'),
        ('entsoe_generation_forecast',           'trade_date'),
        ('entsoe_balancing_energy',              'trade_date'),
        ('entsoe_generation_scheduled',          'trade_date'),
        ('entsoe_cross_border_flows',            'trade_date'),
        ('entsoe_scheduled_cross_border_flows',  'trade_date')
    LOOP
        -- Get column list from local parent (excluding 'id')
        SELECT string_agg(column_name, ', ' ORDER BY ordinal_position)
        INTO v_cols
        FROM information_schema.columns
        WHERE table_schema = 'finance'
          AND table_name = v_table
          AND column_name != 'id';

        -- Sync each country partition
        FOR v_country IN VALUES ('CZ'), ('DE'), ('AT'), ('PL'), ('SK'), ('HU')
        LOOP
            v_fdw_table := v_table || '_' || lower(v_country);

            -- Get per-country max from local table
            EXECUTE format(
                'SELECT MAX(%I)::text FROM finance.%I WHERE country_code = %L',
                v_dt, v_table, v_country
            ) INTO v_max;

            BEGIN
                EXECUTE format(
                    'INSERT INTO finance.%I (%s) SELECT %s FROM prod_fdw.%I WHERE %I > COALESCE(%L, ''1970-01-01'')::date',
                    v_table, v_cols, v_cols, v_fdw_table, v_dt, v_max
                );
                GET DIAGNOSTICS v_count = ROW_COUNT;
                v_total := v_total + v_count;
                IF v_count > 0 THEN
                    RAISE NOTICE '  % [%]: % rows (max was: %)', v_table, v_country, v_count, COALESCE(v_max, 'empty');
                END IF;
            EXCEPTION WHEN undefined_table THEN
                -- Partition doesn't exist on remote (e.g., no HU partition for generation_actual)
                NULL;
            END;
        END LOOP;
    END LOOP;

    -- ==========================================================
    -- SECTION 3: Year-partitioned CEPS tables (1-min resolution)
    -- ==========================================================
    -- These use delivery_timestamp as the datetime column.
    -- Child partitions: table_name_YYYY (2024-2028)
    RAISE NOTICE '';
    RAISE NOTICE '--- CEPS 1-min Tables (year-partitioned) ---';

    FOR v_table IN VALUES
        ('ceps_actual_imbalance_1min'),
        ('ceps_actual_re_price_1min'),
        ('ceps_svr_activation_1min'),
        ('ceps_export_import_svr_1min'),
        ('ceps_generation_res_1min')
    LOOP
        SELECT string_agg(column_name, ', ' ORDER BY ordinal_position)
        INTO v_cols
        FROM information_schema.columns
        WHERE table_schema = 'finance'
          AND table_name = v_table
          AND column_name != 'id';

        EXECUTE format('SELECT MAX(delivery_timestamp)::text FROM finance.%I', v_table)
        INTO v_max;

        FOR v_year IN 2024..2028 LOOP
            v_fdw_table := v_table || '_' || v_year;
            BEGIN
                EXECUTE format(
                    'INSERT INTO finance.%I (%s) SELECT %s FROM prod_fdw.%I WHERE delivery_timestamp > COALESCE(%L, ''1970-01-01'')::timestamp',
                    v_table, v_cols, v_cols, v_fdw_table, v_max
                );
                GET DIAGNOSTICS v_count = ROW_COUNT;
                v_total := v_total + v_count;
                IF v_count > 0 THEN
                    RAISE NOTICE '  % [%]: % rows', v_table, v_year, v_count;
                END IF;
            EXCEPTION WHEN undefined_table THEN
                NULL;
            END;
        END LOOP;

        RAISE NOTICE '  % sync done (max was: %)', v_table, COALESCE(v_max, 'empty');
    END LOOP;

    -- ==========================================================
    -- SECTION 4: Year-partitioned CEPS tables (15-min resolution)
    -- ==========================================================
    -- These use trade_date as the datetime column.
    RAISE NOTICE '';
    RAISE NOTICE '--- CEPS 15-min Tables (year-partitioned) ---';

    FOR v_table IN VALUES
        ('ceps_actual_imbalance_15min'),
        ('ceps_actual_re_price_15min'),
        ('ceps_svr_activation_15min'),
        ('ceps_export_import_svr_15min'),
        ('ceps_generation_res_15min'),
        ('ceps_generation_15min'),
        ('ceps_generation_plan_15min'),
        ('ceps_estimated_imbalance_price_15min')
    LOOP
        SELECT string_agg(column_name, ', ' ORDER BY ordinal_position)
        INTO v_cols
        FROM information_schema.columns
        WHERE table_schema = 'finance'
          AND table_name = v_table
          AND column_name != 'id';

        EXECUTE format('SELECT MAX(trade_date)::text FROM finance.%I', v_table)
        INTO v_max;

        FOR v_year IN 2024..2028 LOOP
            v_fdw_table := v_table || '_' || v_year;
            BEGIN
                EXECUTE format(
                    'INSERT INTO finance.%I (%s) SELECT %s FROM prod_fdw.%I WHERE trade_date > COALESCE(%L, ''1970-01-01'')::date',
                    v_table, v_cols, v_cols, v_fdw_table, v_max
                );
                GET DIAGNOSTICS v_count = ROW_COUNT;
                v_total := v_total + v_count;
                IF v_count > 0 THEN
                    RAISE NOTICE '  % [%]: % rows', v_table, v_year, v_count;
                END IF;
            EXCEPTION WHEN undefined_table THEN
                NULL;
            END;
        END LOOP;

        RAISE NOTICE '  % sync done (max was: %)', v_table, COALESCE(v_max, 'empty');
    END LOOP;

    -- ==========================================================
    -- SUMMARY
    -- ==========================================================
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'FDW SYNC COMPLETE: % total rows synced', v_total;
    RAISE NOTICE 'Finished at: %', now();
    RAISE NOTICE '========================================';

END $$;
