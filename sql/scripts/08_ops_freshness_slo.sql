-- Freshness and SLO status written by the daily DAG after the gold gate passes.
BEGIN;

CREATE TABLE IF NOT EXISTS ops.freshness (
    layer VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    max_loaded_at TIMESTAMPTZ,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_fresh BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_freshness PRIMARY KEY (layer, table_name)
);

CREATE TABLE IF NOT EXISTS ops.slo_status (
    sli VARCHAR NOT NULL,
    measured NUMERIC,
    target_value NUMERIC,
    is_met BOOLEAN NOT NULL DEFAULT TRUE,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_slo_status PRIMARY KEY (sli)
);

GRANT ALL ON ops.freshness TO etl_writer;
GRANT ALL ON ops.slo_status TO etl_writer;
GRANT SELECT ON ops.freshness TO dq_runner;
GRANT SELECT ON ops.slo_status TO dq_runner;

COMMIT;
