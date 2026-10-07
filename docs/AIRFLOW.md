# Airflow

Seven DAGs, no business logic inside. Tasks call `scripts/run_*.py`
entry points and SQL procedures through shared helpers in
`airflow/include/`. Failure mail and SLA callbacks come from
`include/alerts.py` with recipients from the environment.

```mermaid
flowchart LR
    P["preflight"] --> W["watermarks"]
    W --> E["extracts"]
    E --> B{{"bronze gate"}}
    B --> S["silver loads"]
    S --> G{{"silver gate"}}
    G --> D["gold load"]
    D --> Q{{"gold gate"}}
    Q --> R["reconcile + freshness"]
    R --> A["advance watermarks"]

    classDef orch fill:#166534,stroke:#86efac,color:#fff
    classDef qual fill:#6b21a8,stroke:#d8b4fe,color:#fff
    class P,W,E,S,D,R,A orch
    class B,G,Q qual
```

| DAG | Schedule | Purpose |
|---|---|---|
| lh_daily_batch | Daily | Main ELT chain above; publishes the gold dataset |
| lh_dq_nightly | After batch dataset | Full GX revalidation |
| lh_report_publish | After batch dataset | Gold gate then report artifact |
| lh_stream_replay | Manual | Replay producer for a date range |
| lh_stream_reconcile | Hourly | Stream versus batch count check |
| lh_maintenance | Weekly | Retention purge plus analyze |
| lh_backfill | Manual | Parameterized rerun with date range and table list |

Pools are `spark_extract`, `pg_transform`, and `dq`. Extracts retry
three times, transforms once, gates fail fast with zero retries. The
watermark advances only in the final task after the gold gate passes.

```bash
airflow dags trigger lh_daily_batch
airflow dags trigger lh_backfill --conf '{"batch_id":"manual_202401","tables":"trips"}'
airflow tasks logs lh_daily_batch extract_mongo 2026-01-01
```
