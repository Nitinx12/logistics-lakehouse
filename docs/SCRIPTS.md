# Scripts

Every `scripts/run_*.py` is a thin entry point: shared helpers first,
one Rich summary at the end, no business logic. Airflow calls these,
never inline code.

| Script | Purpose |
|---|---|
| run_migrate.py | Applies `sql/scripts/` once each, tracked in `ops.schema_migrations` |
| run_mongo_job.py | Mongo extracts into bronze (`--collections`, `--full-load`) |
| run_databricks_job.py | Databricks extracts into bronze (`--tables`, `--full-load`) |
| run_silver_mongo.py, run_silver_databricks.py | Silver master loads for one batch id |
| run_gold_all.py | Gold dims then facts in key safe order |
| run_gx_bronze.py, run_gx_silver.py, run_gx_gold.py | Great Expectations gates, exit 1 on failure |
| run_dq_checks.py | Every `tests/dq/**/*.sql` check in filename order |
| run_replay.py | Kafka replay producer (`--limit` caps events) |
| run_trip_status_job.py | Status job with watermark and alert logic |
| run_pg_sink.py, run_redis_sink.py, run_alert_consumer.py | Bronze, cache, and Alertmanager sinks |
| ensure_topics.py | Creates the four Kafka topics with arch settings |
| run_notebooks.py | Rebuilds EDA plots and stats from the notebooks |

```bash
uv run scripts/run_mongo_job.py --collections safety_incidents
uv run scripts/run_gx_bronze.py
uv run scripts/run_gold_all.py --batch-id manual_202401
```
