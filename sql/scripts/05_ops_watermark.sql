-- Watermark registry; rows advance only after the gold gate passes.
-- Run with psql: psql -U postgres -f 05_ops_watermark.sql
BEGIN;

CREATE TABLE IF NOT EXISTS ops.watermark (
    source_table VARCHAR NOT NULL,
    batch_id TEXT NOT NULL,
    advanced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_watermark PRIMARY KEY (source_table)
);

CREATE OR REPLACE PROCEDURE ops.advance_watermark(p_batch_id TEXT)
LANGUAGE plpgsql
AS $watermark$
DECLARE
    v_table TEXT;
BEGIN
    FOREACH v_table IN ARRAY ARRAY[
        'customers',
        'drivers',
        'facilities',
        'routes',
        'trailers',
        'trucks',
        'loads',
        'trips',
        'fuel_purchases',
        'delivery_events',
        'maintenance_records',
        'safety_incidents'
    ]
    LOOP
        INSERT INTO ops.watermark AS w (source_table, batch_id, advanced_at)
        VALUES (v_table, p_batch_id, NOW())
        ON CONFLICT (source_table)
        DO UPDATE SET
            batch_id = EXCLUDED.batch_id,
            advanced_at = EXCLUDED.advanced_at;
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'advance_watermark failed batch=%: %', p_batch_id, SQLERRM;
        RAISE;
END;
$watermark$;

GRANT ALL ON ops.watermark TO etl_writer;
GRANT SELECT ON ops.watermark TO dq_runner;

COMMIT;
