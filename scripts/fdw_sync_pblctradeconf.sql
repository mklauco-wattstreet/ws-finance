-- =============================================================================
-- FDW SYNC: public.pblctradeconf (Production -> Dev)
-- =============================================================================
-- Run on the DEVELOPMENT database (via psql or DataGrip).
-- Prerequisite: fdw_setup.sql has been executed and prod_fdw.pblctradeconf exists.
--
-- Table: public.pblctradeconf
-- PK: id (auto-increment, locally generated — excluded from sync)
-- Delta column: "tradeExecTime" (timestamp)
-- Method: append-only — inserts rows from prod where tradeExecTime > local max
-- Safe to run multiple times (idempotent, no duplicates possible via timestamp delta)
-- =============================================================================

DO $$
DECLARE
    v_cols  TEXT;
    v_max   TEXT;
    v_count BIGINT;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'SYNC START: public.pblctradeconf';
    RAISE NOTICE 'Started at: %', clock_timestamp();
    RAISE NOTICE '========================================';

    -- Build column list excluding auto-generated id (quote_ident preserves mixed-case names)
    SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
    INTO v_cols
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'pblctradeconf'
      AND column_name  != 'id';

    -- Find local high-water mark
    SELECT MAX("tradeExecTime")::text
    INTO v_max
    FROM public.pblctradeconf;

    RAISE NOTICE 'Local max tradeExecTime: %', COALESCE(v_max, 'empty — full sync');

    -- Insert new rows from production
    EXECUTE format(
        'INSERT INTO public.pblctradeconf (%s)
         SELECT %s
         FROM prod_fdw.pblctradeconf
         WHERE "tradeExecTime" > COALESCE(%L, ''1970-01-01'')::timestamp',
        v_cols, v_cols, v_max
    );
    GET DIAGNOSTICS v_count = ROW_COUNT;

    RAISE NOTICE '========================================';
    RAISE NOTICE 'SYNC COMPLETE: % rows inserted', v_count;
    RAISE NOTICE 'Finished at: %', clock_timestamp();
    RAISE NOTICE '========================================';
END $$;
