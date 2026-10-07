-- Least-privilege grants matching the .env role layout.
-- Run with psql: psql -U postgres -f 06_roles_grants.sql
BEGIN;

GRANT USAGE ON SCHEMA bronze, silver, gold, ops, dq, analytics
    TO etl_writer, dq_runner, analyst_ro, dashboard_ro;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    IN SCHEMA bronze, silver, gold, ops, dq, analytics
    GRANT SELECT ON TABLES TO dq_runner, analyst_ro, dashboard_ro;

COMMIT;
