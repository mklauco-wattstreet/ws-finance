-- =============================================================================
-- FDW SETUP: Production-to-Dev Database Link
-- =============================================================================
-- Run this ONCE on the DEVELOPMENT database (via psql or DataGrip).
-- This creates a permanent Foreign Data Wrapper connection to production.
--
-- Prerequisites:
--   - Dev DB can reach Prod DB via Tailscale
--   - user_finance_readonly exists on Prod with SELECT on finance schema
--   - pg_hba.conf on Prod allows dev's Tailscale IP
-- =============================================================================

-- FILL IN THESE VALUES BEFORE RUNNING:
-- <TAILSCALE_IP>       : Production server's Tailscale IP (e.g., 100.x.y.z)
-- <DEV_DB_USER>        : Your local dev database username
-- <READONLY_PASSWORD>  : Password for user_finance_readonly on production

-- Step 1: Enable the postgres_fdw extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Step 2: Create the foreign server pointing to production
CREATE SERVER prod_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host '100.79.143.77',
        port '5432',
        dbname 'postgres',
        fetch_size '50000'
    );

-- Step 3: Map your local dev user to the production readonly user
CREATE USER MAPPING FOR imbalance
    SERVER prod_server
    OPTIONS (
        user 'user_finance_readonly',
        password 'YZjx7VjDJYZ7TpDeqg_s_2TtyHfh5ReA'
    );

-- Step 4: Create a local schema to hold the foreign tables
CREATE SCHEMA IF NOT EXISTS prod_fdw;

-- Step 5: Import all tables from production's finance schema
IMPORT FOREIGN SCHEMA finance
    FROM SERVER prod_server
    INTO prod_fdw;

-- =============================================================================
-- VERIFICATION
-- =============================================================================

-- Check that the foreign server was created
SELECT srvname, srvoptions FROM pg_foreign_server WHERE srvname = 'prod_server';

-- Check user mapping
SELECT * FROM pg_user_mappings WHERE srvname = 'prod_server';

-- List all imported foreign tables
SELECT foreign_table_name
FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'prod_fdw'
ORDER BY foreign_table_name;

-- Quick data test: count rows in a small table
SELECT COUNT(*) AS prod_entsoe_areas_count FROM prod_fdw.entsoe_areas;

-- Compare row counts between local and remote for a sample table
SELECT
    (SELECT COUNT(*) FROM finance.ote_prices_day_ahead) AS local_count,
    (SELECT COUNT(*) FROM prod_fdw.ote_prices_day_ahead) AS prod_count;

-- =============================================================================
-- MAINTENANCE COMMANDS (run manually when needed)
-- =============================================================================

-- If production schema changes (new tables/columns), re-import:
--   DROP SCHEMA prod_fdw CASCADE;
--   CREATE SCHEMA prod_fdw;
--   IMPORT FOREIGN SCHEMA finance FROM SERVER prod_server INTO prod_fdw;

-- To remove the FDW setup entirely:
--   DROP SCHEMA prod_fdw CASCADE;
--   DROP USER MAPPING FOR <DEV_DB_USER> SERVER prod_server;
--   DROP SERVER prod_server;
--   DROP EXTENSION postgres_fdw;
