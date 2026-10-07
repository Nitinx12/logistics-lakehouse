# NEXT_STEPS.md: Lakehouse Logistics Data Platform

Snapshot date: 2026-10-06. Design authority: `ARCHITECTURE.md`. Current state: `PROJECT_STATUS.md`.

## 1. Where you really are

M0 to M5 are built and unit tested, but no layer has processed real data in this environment. Until one full run goes green, treat the project as roughly 35 to 40 percent done, not 55. The goal of this guide is to turn "built" into "proven", then ship a visible v0.1.

## 2. Rules for this phase

1. Prove each layer by script before Airflow touches it. Airflow only calls what already works.
2. Smallest table first. Debug on 170 rows, not 170,000.
3. One profile at a time on the 8GB host. Never run a Spark extract and the Airflow stack together.
4. Fix bugs where they surface, add a unit test for each, and commit small.
5. Do not start M6 to M9 work until Step 8 passes.

## 3. Phase A: prove the batch core

### Step 1. Live migrate proof (closes M1)

Bring up only the database services, not the full `core` profile.

```bash
docker compose --profile core config --services
docker compose --profile core up -d postgres pgbouncer
make migrate
make migrate
```

Verify:

```sql
SELECT count(*) FROM ops.schema_migrations;   -- expect 8
SELECT schema_name FROM information_schema.schemata
WHERE schema_name IN ('bronze','silver','gold','ops','dq','analytics');   -- expect 6 rows
```

Done when: the second `make migrate` records nothing new and the counts match.

If it breaks: fix the migration script, never edit a merged one. Add a new script instead (ADR rule in section 10.5).

### Step 2. Mongo path, smallest table first

Run in this order and check after each command.

```bash
uv run scripts/run_mongo_job.py
uv run scripts/run_gx_bronze.py
uv run scripts/run_silver_mongo.py
uv run scripts/run_gx_silver.py
```

Do it in two passes. Pass one with `safety_incidents` only (170 rows), pass two with `maintenance_records` (2,920) and then `delivery_events` (170,820). Check the script arguments for table selection before running.

Verify after each run:

```sql
SELECT * FROM bronze.etl_logs ORDER BY 1 DESC LIMIT 5;
SELECT * FROM silver.etl_logs ORDER BY 1 DESC LIMIT 5;
SELECT count(*) FROM bronze.safety_incidents;   -- expect 170
SELECT count(*) FROM silver.safety_incidents;   -- expect 170 minus quarantined rows
SELECT count(*) FROM dq.quarantine_safety_incidents;
```

Done when: row counts match the EDA numbers in `docs/FINDINGS.md` (quarantine counted) and both GX gates pass.

Watch for: type cast failures, the row with null `truck_id` and `driver_id` in safety_incidents, and Spark memory on `delivery_events` (use `local[2]`, driver 2g, executor 1g, shuffle partitions 8).

### Step 3. Settle the `loaded_at` problem

EDA found one distinct `loaded_at` value per table, so it cannot drive incrementals. Decide and record it in `docs/FINDINGS.md` and as ADR 0001.

Recommended decision:

1. Extract incrementally on business dates (`load_date`, `dispatch_date`, `purchase_date`, event datetime), as the load mode table already assumes.
2. Make the silver "latest wins" guard use `_ingested_at` or batch order, because source `loaded_at` ties on every row.
3. Prove incremental behavior by loading in date slices: first half of the data in run 1, the rest in run 2. A static dataset otherwise hides incremental bugs.

Done when: a two slice load gives the same silver counts as a single full load.

### Step 4. Databricks path, smallest table first

```bash
uv run scripts/run_databricks_job.py
uv run scripts/run_gx_bronze.py
uv run scripts/run_silver_databricks.py
uv run scripts/run_gx_silver.py
```

Order: `customers` (200), `drivers` (150), `trucks`, `trailers`, `facilities` and `routes` (full reload via staging swap), then `loads`, `trips`, `fuel_purchases`.

Known traps from EDA:

1. `purchase_date` arrives as an int in YYYYMMDD form and needs a format cast.
2. About 2 percent of trips and fuel rows have null vehicle assignments. Keep joins null tolerant and map misses to the unknown member.
3. 85,410 loads must equal 85,410 trips, and delivery events must equal exactly 2 per trip.

Done when: silver counts reconcile to the EDA table in `PROJECT_STATUS.md` section 2.

### Step 5. Gold layer and gate

```bash
uv run scripts/run_gold_all.py
uv run scripts/run_gx_gold.py
```

Verify:

```sql
SELECT * FROM gold.etl_logs ORDER BY 1 DESC LIMIT 20;
-- every fact FK resolves to a dim row or -1
-- grain unique on each fact
```

Done when: all 7 dims and 6 facts load, the 18 SQL loop checks in `tests/dq/gold/` pass, and fact counts reconcile to silver.

### Step 6. Idempotency proof (section 12)

Run the whole chain twice on the same input and compare. Save this as the first real test in `tests/smoke/`.

```sql
SELECT 'gold.fact_trips' AS table_name,
       count(*) AS row_count,
       md5(string_agg(t::text, '|' ORDER BY t::text)) AS checksum
FROM gold.fact_trips AS t;
```

Repeat for every silver table and every gold dim and fact. Store the first run output, rerun the pipeline, and diff.

Done when: counts and checksums are identical across both runs for all silver and gold tables.

### Step 7. Make the stub targets real

| Target | Wire to |
|---|---|
| `make test` | `uv run pytest tests/unit tests/smoke` plus lint |
| `make dq` | the three GX runners plus the `tests/dq` SQL checks |
| `make run-batch` | `airflow dags trigger lh_daily_batch` |

Done when: `make test` and `make dq` fail on a deliberately broken input and pass on clean data.

### Step 8. First live Airflow run

Stop everything else, then bring up the `core` profile.

```bash
make up PROFILE=core
make migrate
make run-batch
```

Drive one `lh_daily_batch` run green through preflight, extracts, bronze gate, silver loads, silver gate, gold, gold gate, reconcile, freshness, watermark advance, and snapshot. Then trigger a second run.

Done when: two consecutive runs succeed, the watermark advanced only after the gold gate passed, and counts are unchanged on the second run. Merge `feat/dag-alerts` after this.

## 4. Phase B: ship a showable v0.1

Keep each item minimal. The aim is a visible, honest portfolio piece.

### Step 9. M6 minimal serving

1. Create gold views as the consumer contract (`analyst_ro` and `dashboard_ro` read only these).
2. Build a Streamlit app in `dashboard/` with 2 or 3 tabs: fleet and fuel, load performance, safety.
3. Add a `report/` LaTeX skeleton with 2 or 3 figures from gold views, and wire `make report`.
4. Add the compose `serve` profile.

### Step 10. README and v0.1

1. Replace the one line `README.md` with: purpose, architecture diagram, quick start (`make up`, `make migrate`, `make run-batch`), screenshots, and a plain statement of what is proven.
2. Write ADRs for the decisions that changed (migrations runner, `loaded_at` strategy) in `docs/adr/`.
3. Tag `v0.1`.

### Step 11. M7 trimmed observability

1. Prometheus and Grafana with one pipeline dashboard (run success, duration, freshness, DQ pass rate).
2. One working Gmail alert for pipeline failure, using an App Password from `.env`.
3. `docs/runbook.md` with one entry per alert that exists.
4. Add the compose `obs` profile.

### Step 12. M8 streaming (optional, last)

Only start this after v0.1 is tagged. Run order: `ensure_topics.py`, replay producer, trip status job, Postgres sink, Redis sink. Prove consumer lag under 60 seconds at p95 and stream versus batch drift under 0.5 percent. The batch platform works without it.

### Step 13. M9 and M10 as time allows

1. Load test with synthetic 10x data and record numbers in `docs/capacity.md`.
2. One backup and restore drill, documented.
3. Trivy and gitleaks clean in CI.
4. Fresh clone check: `make up` works from a clean checkout.

## 5. Doc fixes to do alongside

1. ADR 15 and the section 20 table in `ARCHITECTURE.md` still say Flyway. Update both to `scripts/run_migrate.py` and `ops.schema_migrations`.
2. Update `PROJECT_STATUS.md` after each step so "never run" labels turn into dated proof.
3. Add `docs/data_dictionary.md` once gold is stable.

## 6. Progress checklist

- [x] Step 1 live migrate proof (2026-10-06: 8 applied then 8 skipped, registry 8, 6 schemas)
- [x] Step 2 Mongo path green (2026-10-06: safety 170/170, maintenance 2920/2920, delivery 170820/170820, both GX gates green; reruns absorbed by upserts)
- [x] Step 3 `loaded_at` decision recorded (2026-10-06: ADR 0001, small tables full load, large on business dates; two slice proof deferred to Step 6 for want of extractor window args)
- [x] Step 4 Databricks path green (2026-10-06: all 9 tables extract SUCCESS, silver master 0 rejected, both gates green, loads 85410 equals trips, delivery 170820)
- [x] Step 5 gold layer and gate green (2026-10-06: 7 dims with -1 members, 6 facts reconciled, GX gate green, 18/18 tests/dq/gold checks pass)
- [x] Step 6 idempotency proof in `tests/smoke/` (2026-10-06: full chain rerun, 22/25 tables byte identical, 3 differ only on batch housekeeping columns, business hashes stable; `test_idempotency.py` green live)
- [x] Step 7 `make test`, `make dq`, `make run-batch` real (2026-10-06: test runs lint plus unit plus smoke, dq runs 3 GX gates plus 42 SQL checks via `run_dq_checks.py`, exit 1 proven on broken input)
- [ ] Step 9 dashboard and report from gold views
- [ ] Step 10 README, ADRs, tag `v0.1`
- [ ] Step 11 minimal observability and runbook
- [ ] Step 12 streaming live proof (optional)
- [ ] Step 13 hardening and fresh clone check

## 7. Definition of done for v0.1

1. A fresh clone runs `make up`, `make migrate`, `make run-batch` and produces gold tables.
2. Running the pipeline twice gives identical silver and gold checksums.
3. The dashboard reads gold views only.
4. The README states exactly what is proven and what is not.
