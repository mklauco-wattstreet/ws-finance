-- Reassign every object in the `finance` schema to user_finance.
--
-- WHY THIS EXISTS
-- Some finance tables are owned by `postgres` or `klauco_finance` rather than
-- `user_finance` (a legacy of how they were originally created). Alembic runs
-- as user_finance, and PostgreSQL requires OWNERSHIP - not merely GRANTed
-- privileges - for:
--
--     ALTER TABLE ... / CREATE TABLE ... PARTITION OF ...
--
-- so any migration touching one of those tables dies with:
--     psycopg2.errors.InsufficientPrivilege: must be owner of table X
--
-- This has now bitten twice: migration 070 (audit columns -> timestamptz) and
-- migration 073 (SK/PL day-ahead partitions, where the parent
-- entsoe_day_ahead_prices_60min was owned by postgres).
--
-- HOW TO RUN
-- Must be executed by a SUPERUSER (or the current owner). Alembic cannot do
-- this itself - it runs as user_finance, which is exactly the role that lacks
-- the ownership. Run it BEFORE applying migrations on a fresh environment.
--
--     psql -U postgres -d <db> -f scripts/fix_table_ownership.sql
--
-- Idempotent and safe to re-run: objects already owned by user_finance are
-- skipped. Ownership of a table cascades to its indexes and constraints, so
-- those need no separate handling.

DO $$
DECLARE
    r record;
    n_tables  int := 0;
    n_seqs    int := 0;
    n_views   int := 0;
BEGIN
    -- Tables ('r') and partitioned tables ('p'). Partitions are themselves
    -- relkind 'r' and are listed here too, so the whole tree is covered.
    FOR r IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'finance'
          AND c.relkind IN ('r', 'p')
          AND pg_get_userbyid(c.relowner) <> 'user_finance'
        ORDER BY c.relname
    LOOP
        EXECUTE format('ALTER TABLE finance.%I OWNER TO user_finance', r.relname);
        n_tables := n_tables + 1;
    END LOOP;

    FOR r IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'finance'
          AND c.relkind = 'S'
          AND pg_get_userbyid(c.relowner) <> 'user_finance'
        ORDER BY c.relname
    LOOP
        EXECUTE format('ALTER SEQUENCE finance.%I OWNER TO user_finance', r.relname);
        n_seqs := n_seqs + 1;
    END LOOP;

    FOR r IN
        SELECT c.relname, c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'finance'
          AND c.relkind IN ('v', 'm')
          AND pg_get_userbyid(c.relowner) <> 'user_finance'
        ORDER BY c.relname
    LOOP
        IF r.relkind = 'm' THEN
            EXECUTE format('ALTER MATERIALIZED VIEW finance.%I OWNER TO user_finance', r.relname);
        ELSE
            EXECUTE format('ALTER VIEW finance.%I OWNER TO user_finance', r.relname);
        END IF;
        n_views := n_views + 1;
    END LOOP;

    RAISE NOTICE 'Reassigned to user_finance: % table(s), % sequence(s), % view(s)',
                 n_tables, n_seqs, n_views;
END $$;

-- Verification: should return no rows once the block above has run.
SELECT c.relname,
       c.relkind,
       pg_get_userbyid(c.relowner) AS owner
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'finance'
  AND c.relkind IN ('r', 'p', 'S', 'v', 'm')
  AND pg_get_userbyid(c.relowner) <> 'user_finance'
ORDER BY c.relname;
