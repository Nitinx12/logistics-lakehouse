-- Lets etl_writer create staging and audit tables inside layer schemas.
-- Readers keep the usage grants from 06_roles_grants.sql.
BEGIN;

GRANT CREATE ON SCHEMA bronze TO etl_writer;
GRANT CREATE ON SCHEMA silver TO etl_writer;
GRANT CREATE ON SCHEMA gold TO etl_writer;
GRANT CREATE ON SCHEMA ops TO etl_writer;
GRANT CREATE ON SCHEMA dq TO etl_writer;
GRANT CREATE ON SCHEMA source TO etl_writer;
GRANT CREATE ON SCHEMA analytics TO etl_writer;
GRANT USAGE ON SCHEMA source TO dq_runner, analyst_ro, dashboard_ro;

COMMIT;
