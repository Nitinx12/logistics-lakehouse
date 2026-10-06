-- Creates the layer schemas inside the existing lakehouse database.
-- The database itself comes from POSTGRES_DB (auto-created by compose).
-- Roles are login-only here; passwords are set out of band from PG_*_PASSWORD.
-- Run with psql: psql -h localhost -U postgres -d lakehouse -f 00_init_schema.sql
BEGIN;

-- ROLES
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT
            1
        FROM pg_roles
        WHERE rolname = 'etl_writer'
    ) THEN
        CREATE ROLE etl_writer WITH LOGIN;
    END IF;
    IF NOT EXISTS (
        SELECT
            1
        FROM pg_roles
        WHERE rolname = 'dq_runner'
    ) THEN
        CREATE ROLE dq_runner WITH LOGIN;
    END IF;
    IF NOT EXISTS (
        SELECT
            1
        FROM pg_roles
        WHERE rolname = 'analyst_ro'
    ) THEN
        CREATE ROLE analyst_ro WITH LOGIN;
    END IF;
    IF NOT EXISTS (
        SELECT
            1
        FROM pg_roles
        WHERE rolname = 'dashboard_ro'
    ) THEN
        CREATE ROLE dashboard_ro WITH LOGIN;
    END IF;
END
$$;

-- CREATE SCHEMA
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS dq;
CREATE SCHEMA IF NOT EXISTS analytics;

-- descriptions
COMMENT ON SCHEMA bronze IS 'Raw source-system landing layer. Minimal transformation.';
COMMENT ON SCHEMA silver IS 'Data cleansing, standardization, validation and intermediate transformations.';
COMMENT ON SCHEMA gold IS 'Production business warehouse containing dimensions and fact tables.';
COMMENT ON SCHEMA ops IS 'Watermarks, run logs, freshness and SLO status.';
COMMENT ON SCHEMA dq IS 'Test catalog, test results and quarantine tables.';
COMMENT ON SCHEMA analytics IS 'Reporting, analytical marts, KPIs and BI-facing objects.';

COMMIT;
