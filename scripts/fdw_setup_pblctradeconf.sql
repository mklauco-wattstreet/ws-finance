-- =============================================================================
-- FDW SETUP: pblctradeconf only (Production -> Dev)
-- =============================================================================
-- Run ONCE on the DEVELOPMENT database as SUPERUSER (psql -U postgres or DataGrip).
-- Sets up FDW for the single table public.pblctradeconf from production.
--
-- Prerequisites:
--   - Dev DB can reach Prod DB via Tailscale (100.79.143.77)
--   - user_finance_readonly exists on Prod with SELECT on public.pblctradeconf
-- =============================================================================

-- Step 1: Enable the postgres_fdw extension (requires superuser)
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Step 2: Create the foreign server (skip if already exists from fdw_setup.sql)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = 'prod_server') THEN
        CREATE SERVER prod_server
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host '100.79.143.77', port '5432', dbname 'postgres', fetch_size '50000');
        RAISE NOTICE 'Created foreign server prod_server';
    ELSE
        RAISE NOTICE 'Foreign server prod_server already exists';
    END IF;
END $$;

-- Step 3: Map postgres (superuser, needed for IMPORT FOREIGN SCHEMA)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_user_mappings
        WHERE srvname = 'prod_server' AND usename = 'postgres'
    ) THEN
        CREATE USER MAPPING FOR postgres
            SERVER prod_server
            OPTIONS (user 'user_finance_readonly', password 'YZjx7VjDJYZ7TpDeqg_s_2TtyHfh5ReA');
        RAISE NOTICE 'Created user mapping for postgres';
    ELSE
        RAISE NOTICE 'User mapping already exists for postgres';
    END IF;
END $$;

-- Step 4: Map user_finance (runtime user for the sync runner)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_user_mappings
        WHERE srvname = 'prod_server' AND usename = 'user_finance'
    ) THEN
        CREATE USER MAPPING FOR user_finance
            SERVER prod_server
            OPTIONS (user 'user_finance_readonly', password 'YZjx7VjDJYZ7TpDeqg_s_2TtyHfh5ReA');
        RAISE NOTICE 'Created user mapping for user_finance';
    ELSE
        RAISE NOTICE 'User mapping already exists for user_finance';
    END IF;
END $$;

-- Step 4: Grant usage on the foreign server to user_finance
GRANT USAGE ON FOREIGN SERVER prod_server TO user_finance;

-- Step 5: Create schema for foreign tables
CREATE SCHEMA IF NOT EXISTS prod_fdw;
GRANT USAGE ON SCHEMA prod_fdw TO user_finance;

-- Step 6: Import ONLY pblctradeconf from production's public schema
IMPORT FOREIGN SCHEMA public
    LIMIT TO (pblctradeconf)
    FROM SERVER prod_server
    INTO prod_fdw;

-- Grant SELECT on the foreign table
GRANT SELECT ON prod_fdw.pblctradeconf TO user_finance;

-- =============================================================================
-- VERIFICATION
-- =============================================================================
SELECT foreign_table_name
FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'prod_fdw';

SELECT COUNT(*) AS recent_prod_rows FROM prod_fdw.pblctradeconf
WHERE "tradeExecTime" > NOW() - INTERVAL '1 hour';
