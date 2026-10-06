# src

Python package for the lakehouse batch pipeline. Extractors land raw text in
`bronze`, PL/pgSQL procedures reshape it through `silver` into `gold`, and
shared helpers keep connections, logging, and run tracking in one place.

## Layout

```text
src/
  jobs/
    extract/
      mongo/mongo_to_postgres.py          Mongo collections to bronze
      databricks/databricks_to_postgres.py  Databricks tables to bronze
    transform/
      mongo/proc_silver_load_*.sql        Silver loaders plus mongo master
      databricks/proc_silver_load_*.sql   Silver loaders plus databricks master
    load/
      proc_gold_load_*.sql                Gold dims, facts, plus gold master
    _report.py                            Shared extract summary rendering
  utils/
    connection.py                         Postgres, Mongo, Databricks clients
    tracking.py                           Run log, stage metrics, snapshots
    logger.py                             Shared logger
    session.py                            Spark session builder
    engine.py                             SQLAlchemy engine builder
```

## Conventions

Silver procedures are named `silver.load_<table>` and gold procedures
`gold.load_<table>`, each taking `p_batch_id TEXT`. Every procedure writes
one audit row per run to its layer `etl_logs` table, holds a Postgres
advisory lock while running, and reruns safely: incremental loads filter on
the `loaded_at` watermark and upsert guarded by it, while the two tiny
reference tables reload fully inside one transaction.

Master procedures run a whole layer with one batch id in dependency order:
`silver.load_mongo_all`, `silver.load_databricks_all`, `gold.load_gold_all`.
They call per table procedures and never touch table data directly.

## Run

Full pipeline with gates and final report:

```bash
uv run python main.py
```

Single layer masters with audit summaries:

```bash
uv run python scripts/run_silver_mongo.py --batch-id my_batch
uv run python scripts/run_silver_databricks.py --batch-id my_batch
uv run python scripts/run_gold_all.py --batch-id my_batch
```

One procedure directly with psql:

```sql
CALL silver.load_databricks_all('my_batch');
CALL gold.load_gold_all('my_batch');
```

## Add a table

1. Add the extractor contract under `contracts/` and land bronze columns.
2. Add `transform/<source>/proc_silver_load_<table>.sql` following a sibling
   loader, then register it in the matching master procedure.
3. Add the gold dimension or fact under `load/` plus its master call.
4. Add loop checks under `tests/dq/` and expectations in the GX runner.
