# PROJECT_STATUS.md — Lakehouse Logistics Data Platform

Snapshot of the project on 2026-10-06. Design authority is `ARCHITECTURE.md`.
This file records what is built, what is proven, what is missing, and what
comes next. Branch `feat/dag-alerts` is pushed to origin. Working tree is
clean. Unit suite reads 106 passed with 7 skipped.

## 1. Overall progress: about 55 percent

| Milestone | Exit criteria from §24 | State | Done |
|---|---|---|---|
| M0 EDA | Profiling scripts, findings, load mode table, grains | Complete, recorded in `docs/FINDINGS.md` | 100 |
| M1 Foundation | Compose core, Makefile, migrations, ops and dq schemas, CI skeleton | Live proof done 2026-10-06: double `make migrate` applied 8 then skipped 8, registry reads 8, 6 schemas present; CI job still unproven | 90 |
| M2 Bronze | Extractors, watermarks, bronze DQ, idempotent loads | Live proof done 2026-10-06 on both paths: mongo safety 170, maintenance 2920, delivery 170820 plus all 9 Databricks tables, matching counts, GX bronze gate green on all 12 tables, reruns absorbed by upserts or skipped by watermarks | 95 |
| M3 Silver | Typed SCD1 procedures, quarantine, silver DQ gate | Live proof done 2026-10-06 on both paths: mongo safety 170/170 with 0 rejected plus databricks master 0 rejected across all 9 tables, GX silver gate green; delta loads correctly process 0 rows when silver is current | 95 |
| M4 Gold | Dims, facts, reconciliation tests | Built and unit tested, never run end to end | 85 |
| M5 Orchestration | Airflow DAGs, gates, retries, SLAs, daily run end to end | 7 DAGs use shared plumbing plus arch pools plus dataset triggers, DagBag green; first live run proved preflight wiring but full green waits on the worker image build; `run-batch` wired | 70 |
| M6 Serving | Streamlit dashboard, LaTeX report from gold views only | Not started, no `dashboard/` or `report/` directory | 0 |
| M7 Observability | Prometheus, Grafana, alerts, runbook with an entry per alert | `monitoring/` folders exist but are empty, only code level logging exists | 10 |
| M8 Streaming | Kafka topics, producer, Flink, sinks, Redis, live tab, lag SLO met | Code and tests exist, never run, no SLO proof | 50 |
| M9 Hardening | 10x load test, backup restore drill, chaos test, security scan | Not started | 0 |
| M10 Release | README, docs, ADRs, fresh clone runs with `make up` | Architecture doc solid, `README.md` is one line | 10 |

The batch core (M0 through M5) is built but unproven. No layer has processed
real data in this environment yet. That single fact is the main risk in the
whole status below.

## 2. Sources and EDA verdicts

Twelve sources profiled live on 2026-10-05. Code sits in
`notebooks/mongo_eda.ipynb` and `notebooks/databricks_eda.ipynb`. Generated
plots and stats are gitignored and rebuilt with `make eda`.

| Source | Table | Rows | Key | Load mode |
|---|---|---|---|---|
| mongo `fleet_operations` | delivery_events | 170,820 | event_id, unique | incremental |
| mongo `fleet_operations` | maintenance_records | 2,920 | maintenance_id, unique | incremental |
| mongo `fleet_operations` | safety_incidents | 170 | incident_id, unique | incremental |
| databricks `logistics_operations.default` | customers | 200 | customer_id, unique | incremental |
| databricks `logistics_operations.default` | drivers | 150 | driver_id, unique | incremental |
| databricks `logistics_operations.default` | facilities | 50 | facility_id, unique | full reload |
| databricks `logistics_operations.default` | fuel_purchases | 196,442 | fuel_purchase_id, unique | incremental |
| databricks `logistics_operations.default` | loads | 85,410 | load_id, unique | incremental |
| databricks `logistics_operations.default` | trips | 85,410 | trip_id, unique | incremental |
| databricks `logistics_operations.default` | routes | 58 | route_id, unique | full reload |
| databricks `logistics_operations.default` | trailers | 180 | trailer_id, unique | incremental |
| databricks `logistics_operations.default` | trucks | 120 | truck_id, unique | incremental |

Cross cutting verdicts, all recorded in `docs/FINDINGS.md`:

* Every candidate key is unique. Zero duplicate rows in all 12 tables.
* `loaded_at` is 0 percent null everywhere but holds a single distinct value
  per table, so it is useless as an incremental filter until it is refreshed
  per batch. Until then the plan is to increment on business dates such as
  `load_date`, `dispatch_date`, `purchase_date`, and event datetimes.
* Zero orphan foreign keys across all 16 checked relationships.
* Grain confirmed in data: 85,410 loads equals 85,410 trips equals 2 times
  85,410 delivery events. One to one loads to trips, exactly Pickup plus
  Delivery per trip.
* 10x projection: largest table 196k rows scales to about 2M rows, inside
  single host Postgres capacity.
* Silver typing watchlist: `purchase_date` is an int in YYYYMMDD form and
  needs a format cast, about 2 percent null vehicle assignments on trips and
  fuel need the unknown member or null tolerant joins in gold.

Open questions from §23: items 1 through 4 (load modes, keys and grains,
`loaded_at` verdict, volumes) are resolved by EDA. Items 5 through 9 remain
open: Flink versus Spark Structured Streaming, read replica need, Schema
Registry timing, SCD Type 2 candidates, and the production deploy target.

## 3. Pipeline state by layer

### 3.1 Extraction

Built, not run. `src/jobs/extract/mongo/mongo_to_postgres.py` and
`src/jobs/extract/databricks/databricks_to_postgres.py` implement
partitioned incremental reads with an overlap window and batched COPY or
JDBC batch writes into bronze. Twelve per table YAML contracts live in
`contracts/mongo/` (3 files) and `contracts/databricks/` (9 files) and
carry mode, key, watermark column, and partition settings. Entry points are
`scripts/run_mongo_job.py` and `scripts/run_databricks_job.py`, each printing
one Rich audit summary from `bronze.etl_logs` and recording the stage through
`src/utils/tracking.py`. Spark submit helper lives in `scripts/_submit.py`
and report helpers live in `scripts/_report.py`, so runners share code
instead of duplicating it. Jars are expected in `jars/`.

### 3.2 Bronze

DDL exists, never loaded. `sql/scripts/01_bronze_etl_logs.sql` creates the
`bronze.etl_logs` audit table with a single started run guard. Bronze tables
themselves are created by the extract jobs as all text plus `batch_id`,
`_ingested_at`, and `_source`. Quality coverage: `scripts/run_gx_bronze.py`
validates row counts, ordered columns, key not null and unique, and
`loaded_at` shape for all 12 tables, backed by committed suites in
`gx/expectations/bronze/` and `gx/validation_definitions/`. Seven SQL loop
checks live in `tests/dq/bronze/`.

### 3.3 Silver

Procedures exist, never executed. Fourteen procedure files sit in
`src/jobs/transform/`: 3 per table mongo loaders plus the
`silver.load_mongo_all` orchestrator, and 9 per table databricks loaders
plus the `silver.load_databricks_all` orchestrator. Small reference tables
use full reload through a staging swap, everything else is an incremental
upsert guarded by `loaded_at` with latest row wins dedupe. Type failures go
to `dq.quarantine_*` with a reason code. Audit rows land in
`silver.etl_logs` (`sql/scripts/03_silver_etl_logs.sql`). Entry points are
`scripts/run_silver_mongo.py` and `scripts/run_silver_databricks.py`, each
calling its master procedure once with one shared batch id. Quality coverage
is `scripts/run_gx_silver.py` plus suites and 17 SQL loop checks in
`tests/dq/silver/`.

### 3.4 Gold

Procedures exist, never executed. Fourteen files sit in `src/jobs/load/`:
7 dimension loaders with stable surrogate keys and the `-1` unknown member,
6 fact loaders with declared grains and FK resolution, and the
`gold.load_gold_all` master that runs in FK safe order. Audit rows land in
`gold.etl_logs` (`sql/scripts/04_gold_etl_logs.sql`). Entry point is
`scripts/run_gold_all.py`. Quality coverage is `scripts/run_gx_gold.py`
plus suites and 18 SQL loop checks in `tests/dq/gold/`, including join
stability checks.

### 3.5 Orchestration

Written and unit tested, never run. Seven DAGs live in `airflow/dags/`:
`lh_daily_batch`, `lh_dq_nightly`, `lh_report_publish`, `lh_stream_replay`,
`lh_stream_reconcile`, `lh_maintenance`, `lh_backfill`. The daily DAG follows
the §9.2 chain: preflight, watermarks, parallel extracts, bronze gate,
parallel silver loads, silver gate, gold load, gold gate, reconcile,
freshness, watermark advance, snapshot. DAGs hold no business logic. They
call the scripts above through BashOperators and shared helpers in
`airflow/include/ops.py`, with failure mail defaults in
`airflow/include/alerts.py`. Every DAG has a `DagBag` integrity test in
`tests/unit/`. Watermarks advance only in the final task after all gates
pass, enforced by `sql/scripts/05_ops_watermark.sql` and the
`ops.advance_watermark` procedure.

### 3.6 Streaming

Code complete, never run. `streaming/producer/replay.py` replays historical
`delivery_events` in event time order at an accelerated rate.
`streaming/flink/trip_status_job.py` with `streaming/flink/logic.py` keeps
latest status per trip keyed by `trip_id` with watermarks, allowed lateness,
and stuck trip detection. Sinks in `streaming/sinks/` write the Postgres
landing table (`bronze.delivery_events_stream` from
`sql/scripts/07_stream_tables.sql`), the Redis live state, and the alert
consumer. Topics and message helpers are `streaming/topics.py` and
`streaming/message.py`, with JSON Schemas in `streaming/schemas/` for the
event, trip status, and alert versions. Entry points are
`scripts/run_replay.py`, `scripts/run_trip_status_job.py`,
`scripts/run_pg_sink.py`, `scripts/run_redis_sink.py`,
`scripts/run_alert_consumer.py`, and `scripts/ensure_topics.py`. Four
pytest modules in `tests/streaming/` cover message validation, topics, the
sink upsert, and the trip status logic. Compose `stream` profile provides
KRaft Kafka and the cache Redis. The stream writes nowhere near silver or
gold by design.

### 3.7 Data quality

Two mechanisms, both built. Great Expectations suites exist for all 12
bronze tables, all silver tables, and all gold dims and facts: 38 suite
files in `gx/expectations/`, 37 validation definitions in
`gx/validation_definitions/`, and `bronze`, `silver`, `gold` checkpoints in
`gx/checkpoints/`. Forty two SQL loop checks live in `tests/dq/` across the
three layers and cover extraction health, columns, keys, loaded at shape,
lineage, and reconciliation. Severity policy from §11 stands: critical
blocks the next layer and the watermark, warn quarantines and continues.

### 3.8 Observability

Mostly missing. Structured run logging exists in code: every runner records
its stage through `src/utils/tracking.py` into `ops.pipeline_run_log`, every
procedure writes `*_etl_logs`, loggers come from `src/utils/logger.py`, and
`sql/scripts/02_ops_run_log.sql` adds the run log plus the
`ops.record_layer_snapshot` procedure. Nothing else exists. The
`monitoring/prometheus/`, `monitoring/alertmanager/`, and
`monitoring/grafana/` directories are empty. No SLO panels, no alert rules,
no runbook. Gmail SMTP alerting is designed in §13.5 and templated in
`.env.example` but not wired to anything running.

### 3.9 Serving

Missing. No `dashboard/` Streamlit app and no `report/` LaTeX sources. The
consumer contract is defined (gold views plus the Redis live state) but the
views and both consumers still have to be written. Compose `serve` and `obs`
profiles do not exist yet.

## 4. Foundation state

* `compose.yml` provides the `core` profile (postgres, airflow metadata DB,
  pgbouncer, redis broker, airflow init plus webserver plus scheduler, spark
  runner) and the `stream` profile (KRaft kafka, redis cache). Every core
  service has a healthcheck. Local 8GB budget from §15.2 applies: one profile
  at a time.
* Migrations are the 8 ordered files in `sql/scripts/` (`00` init schema
  through `07` stream tables), applied once each by the single
  `scripts/run_migrate.py`, tracked in `ops.schema_migrations` with an
  insert guard, so reruns record nothing twice. `make migrate` and the CI
  migrate job both call this one runner. `00_init_schema.sql` creates roles
  only when absent from `pg_roles`, so raw reruns are safe too. The runner
  connects through the admin role when `POSTGRES_SUPERUSER_PASSWORD` is set
  and falls back to the app role otherwise, because the app role owns no
  CREATE privilege. Proven 2026-10-06 against local Postgres: first run
  applied 8 and skipped 0, second run applied 0 and skipped 8.
* CI (`.github/workflows/ci.yml`) runs unit tests plus a migrate job on
  Postgres 16 that asserts all 6 layer schemas exist and the registry row
  count equals the script count. Pre commit carries ruff, gitleaks, and a
  sqlfluff hook.
* Shared Python helpers in `src/utils/`: postgres plus mongo plus Databricks
  connections from environment, engine and session setup, structured logger,
  and run tracking with snapshot support.

## 5. Repository inventory

| Path | Contents | State |
|---|---|---|
| `ARCHITECTURE.md` | Design authority, §§1-24, layout in §19 matches the built tree | Current |
| `PROJECT_STATUS.md` | This file | New |
| `AGENTS.md` | Agent and contributor rules | Current |
| `Makefile` | Verbs only, `PROFILE ?= core` | `up`, `down`, `ps`, `logs`, `pull`, `lint`, `eda`, `migrate` real; `test`, `run-batch`, `dq`, `report` still stubs |
| `compose.yml` | `core` and `stream` profiles | Current, `serve` and `obs` missing |
| `.env.example` | Full template including SMTP and alert mail | Current, no secrets inside |
| `pyproject.toml`, `uv.lock` | uv managed deps, committed together | Current |
| `contracts/` | 12 per table YAML files, 9 databricks plus 3 mongo | Complete |
| `data/` | Sample dirs, gitignored | Local only |
| `notebooks/` | 2 EDA notebooks, plots and stats gitignored | Complete |
| `docs/` | `FINDINGS.md` only | Dictionary, runbook, capacity, ADRs missing |
| `airflow/dags/` | 7 DAGs | Written, tested, never run |
| `airflow/include/` | `ops.py`, `alerts.py` | Complete |
| `airflow/configs/` | Empty | Reserved |
| `src/jobs/extract/` | 2 extractors | Built, never run |
| `src/jobs/transform/` | 14 silver procedures | Built, never executed |
| `src/jobs/load/` | 14 gold procedures | Built, never executed |
| `src/utils/` | connection, engine, logger, session, tracking | Complete |
| `scripts/` | 16 `run_*` entry points plus `_report.py` and `_submit.py` | Complete except `run-batch`, `dq`, `report` wiring |
| `sql/scripts/` | 8 ordered migration scripts | Complete and idempotent |
| `sql/metadata/` | `sequence.sql` helper | Complete |
| `gx/` | 38 suites, 37 validation definitions, 3 checkpoints | Complete |
| `streaming/` | producer, flink, sinks, schemas, topics, message | Code complete, never run |
| `monitoring/` | Empty prometheus, alertmanager, grafana dirs | Missing |
| `tests/unit/` | 13 modules, 106 passed, 7 skipped | Green |
| `tests/dq/` | 42 SQL loop checks | Green in CI by inspection, need a live DB for full proof |
| `tests/smoke/` | Placeholder only | Missing |
| `tests/streaming/` | 4 modules | Green |
| `jars/` | Spark connector jars | Local only |
| `main.py`, `DATABASE_SCHEMA.txt` | Legacy notes at root | Unchanged |

## 6. Entry points and which ones are real

| Command | Action | State |
|---|---|---|
| `make up`, `down`, `ps`, `logs`, `pull` | Compose lifecycle | Real |
| `make lint` | Compile check over `src` and `scripts` | Real |
| `make eda` | `uv run scripts/run_notebooks.py` | Real |
| `make migrate` | `uv run scripts/run_migrate.py` | Real |
| `make test` | Lint plus a placeholder echo | Stub, wire to `uv run pytest` |
| `make run-batch` | Placeholder echo | Stub, wire to `airflow dags trigger lh_daily_batch` |
| `make dq` | Placeholder echo | Stub, wire to the GX runners plus `tests/dq` |
| `make report` | Placeholder echo | Stub, needs M6 |
| `uv run scripts/run_mongo_job.py` | Mongo extract | Built, never run |
| `uv run scripts/run_databricks_job.py` | Databricks extract | Built, never run |
| `uv run scripts/run_gx_bronze.py` | Bronze gate | Built, never run |
| `uv run scripts/run_silver_mongo.py` | Mongo silver master | Built, never run |
| `uv run scripts/run_silver_databricks.py` | Databricks silver master | Built, never run |
| `uv run scripts/run_gx_silver.py` | Silver gate | Built, never run |
| `uv run scripts/run_gold_all.py` | Gold master | Built, never run |
| `uv run scripts/run_gx_gold.py` | Gold gate | Built, never run |

## 7. What is proven versus what is claimed

Proven without a database: unit suite green (106 passed, 7 skipped),
migration file conventions, DAG integrity (import, no cycles, tags, owners),
GX suite definitions, streaming message and logic tests, CI workflow shape.

Not proven yet: a single extract against live Mongo or Databricks, a single
bronze load, a single silver or gold procedure execution, a full DAG run,
the double run idempotency check, reconcile counts against sources, any SLO
number, backup restore, 10x volume behavior, and the fresh clone `make up`
path from M10.

## 8. Known gaps and risks

1. Nothing has run against live data except the Step 1 migrate proof.
   Integration risk sits in the
   extractors, the silver type casts (especially the int `purchase_date`),
   and the 2 percent null vehicle joins.
2. The dev database behind `make migrate` is a local Postgres 17 on the
   host, while compose ships Postgres 16 with no published port. Decide one
   before Step 2: either publish the container port and retire the local
   server, or record local Postgres as the dev default. The container
   database currently holds a partial migration state with no registry rows,
   so treat it as untrusted until it is rebuilt by the runner.
2. `loaded_at` carries one value per table, so the first real incremental
   run must use business date filters or refresh `loaded_at` per batch.
3. `monitoring/` is empty and `docs/runbook.md` does not exist, so the first
   live failure will have nowhere to page and no runbook to follow.
4. `tests/smoke/` is a placeholder and `tests/sql/` plus `tests/load/` do
   not exist, so M9 has no harness.
5. `README.md` is one line and the ADRs were never written beyond the table
   in §21, so M10 is far off.
6. No `release.yml` workflow exists, images are unpublished, and tags do not
   exist yet.
7. Every GX runner rewrites all committed suite JSONs with fresh UUIDs on
   each run (49 files of churn observed 2026-10-06, restored). Either make
   the runners leave committed suites alone or accept the noise in a
   dedicated commit. Never mix it into feature commits.
8. Two databases exist side by side: the local Postgres 17 that `make`
   targets use, and the compose Postgres 16 container with an older partial
   state. Always verify which server a command talks to before trusting
   counts.

## 9. Next steps in order

1. Wire `run-batch` to `airflow dags trigger lh_daily_batch` and bring up
   `core`, then `make migrate`.
2. Trigger one `lh_daily_batch` run and drive it green through all three DQ
   gates, reconcile, and watermark advance.
3. Rerun the same batch and confirm identical silver and gold counts and
   checksums (the §12 idempotency proof), then wire `make test` and
   `make dq` to the real suites.
4. Build M6 serving on gold views only, then M7 observability with one
   runbook entry per alert, then the M8 live run with lag and drift proof.
5. Finish with M9 load plus restore drills and the M10 README, ADRs, and
   fresh clone check.
