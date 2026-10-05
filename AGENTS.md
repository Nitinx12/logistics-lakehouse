# AGENTS.md

Instructions for AI coding agents working in the lakehouse repository. Read this file and `ARCHITECTURE.md` before changing anything. Human contributors should follow the same rules.

## Project Summary

lakehouse is a batch first ELT platform for logistics data.

* Sources: MongoDB `fleet_operations` and Databricks `logistics_operations.default`.
* Extraction: PySpark with `loaded_at` watermarks.
* Warehouse: PostgreSQL with layered PL/pgSQL procedures (bronze, silver, gold).
* Orchestration: Airflow. Streaming side branch: Kafka and Flink for `delivery_events`.
* Serving: Streamlit and a LaTeX report. Monitoring: Prometheus and Grafana. Alerts: Gmail SMTP.

## Setup and Commands

Use these commands. Do not invent alternatives.

| Task | Command |
|---|---|
| Install dependencies | `uv sync` |
| Add a dependency | `uv add <package>` |
| Run Python | `uv run python <file>` |
| Start the stack | `make up` |
| Apply migrations | `make migrate` |
| Run all checks | `make test` |
| Lint Python | `uv run ruff check . && uv run ruff format --check .` |
| Lint SQL | `uv run sqlfluff lint sql/` |
| Lint shell | `shellcheck scripts/bash/*.sh` |
| Run DQ suites | `make dq` |

Rules:

* Python dependencies are managed with `uv` only. Never use `pip install`, `poetry`, or a hand edited `requirements.txt`.
* Commit `pyproject.toml` and `uv.lock` together.
* Copy `.env.example` to `.env` for local runs. Never commit `.env`.

## Repository Layout

```
airflow/      DAGs and shared helpers
extract/      PySpark extractors and per table YAML config
sql/          migrations, bronze, silver, gold, ops, dq
streaming/    producer, Flink job, sinks, JSON schemas
dashboard/    Streamlit app
report/       LaTeX sources
monitoring/   Prometheus, Alertmanager, Grafana provisioning
scripts/      bash and powershell wrappers
tests/        unit, sql, smoke, streaming, load
docs/         data dictionary, runbook, ADRs
```

## Comment Rules (all languages)

Agents write as few comments as possible.

1. Every source file gets one short header comment at the very top. One line is preferred, two lines at most. It states what the file does.
2. Never write a paragraph comment.
3. Never write comments between lines of code. No inline comments and no section divider comments. If code needs explaining, rename the variable or extract a well named function instead.
4. Never add author names, dates, change history, or ticket logs to comments. Git holds that.
5. Allowed exceptions, kept minimal:
   * Tool directives such as `# noqa: E501`, `# type: ignore`, `# shellcheck disable=SC2034`, `-- noqa: LT05`, and `# syntax=docker/dockerfile:1`.
   * Makefile `##` help text on a target, which feeds `make help`.
   * One line docstrings on public Python functions and classes, only when the name does not already say what it does.

Header comment examples:

```python
# Extracts incremental rows from Databricks into the bronze schema.
```

```sql
-- Loads silver.trips from bronze.trips with a latest row wins upsert.
```

```bash
# Starts the compose stack for the selected profile.
```

## SQL Standards

Target dialect is PostgreSQL 16. Lint with SQLFluff using the repo config.

Format:

* SQL keywords and function names in UPPERCASE. Identifiers in lowercase `snake_case`.
* 4 space indentation. No tabs.
* One column per line in `SELECT`. Trailing commas, never leading commas.
* Each clause (`FROM`, `WHERE`, `GROUP BY`, `ORDER BY`) starts on its own line.
* End every statement with a semicolon.
* Maximum line length 100 characters.

Example of the expected style:

```sql
SELECT
    customer_id,
    customer_name,
    created_at
FROM customers
WHERE status = 'active';
```

Practices:

* Never use `SELECT *` outside throwaway exploration. Always list columns.
* Always list columns in `INSERT INTO table (col_a, col_b)`.
* Qualify objects with the schema, for example `silver.trips`.
* Use explicit `INNER JOIN`, `LEFT JOIN` with `ON`. Never use comma joins.
* Use meaningful table aliases, not single letters, once a query has more than two tables.
* Use `AS` for column and table aliases.
* Prefer CTEs with descriptive names over deeply nested subqueries.
* Cast with the `::VARCHAR` style, for example `trip_id::VARCHAR`.
* Use `COALESCE`, `NULLIF`, and `CASE` explicitly. Never rely on implicit casts.
* Keep set based logic. No row by row loops over data.
* Use `ON CONFLICT ... DO UPDATE` or `MERGE` for upserts, guarded by `loaded_at`.
* Every `CREATE` uses `IF NOT EXISTS` or `CREATE OR REPLACE` so scripts rerun safely.
* Every table has a primary key or a documented unique grain constraint.

PL/pgSQL:

* Procedures are named `<layer>.load_<table>` and take `p_batch_id TEXT`.
* Parameters start with `p_`, local variables with `v_`.
* Wrap each procedure body in an exception block that logs to `ops.etl_step_log` and re raises.
* Build dynamic SQL only with `format()` using `%I` for identifiers and `%L` for literals.
* Procedures must be idempotent. Rerunning a batch never changes the final state.

Migrations:

* Files live in `sql/migrations/` and are named `V<number>__<description>.sql`.
* Never edit a migration that is merged. Add a new one.
* Prefer backward compatible changes first, remove old columns in a later migration.

## Python Standards

* Python version comes from `pyproject.toml`. Format and lint with `ruff`. Follow PEP 8.
* Type hints on all function signatures.
* Use `pathlib` for paths, f strings for formatting, and context managers for connections and files.
* Use the shared logger from `extract/common/`. Never use `print` for operational output. Rich is allowed for console progress bars.
* Read configuration and secrets from environment variables. Never hardcode credentials, hosts, or paths.
* Catch specific exceptions. Never use a bare `except:`.
* Keep functions small and single purpose. No logic at import time; use `if __name__ == "__main__":`.
* Pin nothing by hand. Let `uv.lock` own versions.
* PySpark: select only needed columns early, push filters to the source, avoid `collect()` on large data, and suppress Spark and py4j log noise.
* Tests use `pytest`. Name files `test_<module>.py`. One behaviour per test.

## Shell Standards

Bash (`scripts/bash/*.sh`):

* First line `#!/usr/bin/env bash`, then the header comment, then `set -euo pipefail`.
* Quote every variable expansion. Use `[[ ... ]]` for tests.
* Constants in `UPPER_CASE`, locals in `lower_case`. Use `local` inside functions.
* Use functions and a `main` entry point. Use `trap` for cleanup.
* Check that required commands exist before using them.
* Must pass `shellcheck` and `shfmt`.

PowerShell (`scripts/powershell/*.ps1`):

* Start with the header comment, then `Set-StrictMode -Version Latest` and `$ErrorActionPreference = 'Stop'`.
* Use approved `Verb-Noun` names, a `param()` block, and full cmdlet names. No aliases.
* Must pass `PSScriptAnalyzer`.

Batch files (`*.bat`, `*.cmd`):

* Use only when PowerShell is not an option. Start with `@echo off`, then a `REM` header line, then `setlocal EnableExtensions`.
* Reference the script folder with `%~dp0`. Quote all paths.
* Check `%ERRORLEVEL%` after each critical command. Finish with `exit /b <code>`.

## Makefile Standards

* Start with the header comment, then `SHELL := bash`, `.SHELLFLAGS := -eu -o pipefail -c`, and `.DEFAULT_GOAL := help`.
* Declare every non file target in `.PHONY`.
* Recipes are indented with a real tab.
* Use `?=` for overridable variables, for example `PROFILE ?= core`.
* Provide a `help` target that lists targets from their `##` text.
* Targets are verbs in lowercase: `up`, `down`, `migrate`, `test`, `dq`, `report`.
* Targets call scripts or tools. Keep logic out of the Makefile.
* No comments inside recipes.

## Docker and YAML

* Pin image tags. Never use `latest`. Use multi stage builds and run as a non root user.
* Every compose service has a healthcheck, resource limits, and a restart policy.
* Every compose service sets `mem_limit` from the 8GB budget in ARCHITECTURE.md §15.2.
* Local runs start one compose profile at a time and stop it when done.
* Keep a `.dockerignore`. Never copy `.env` into an image.
* YAML uses 2 space indentation and passes `yamllint`.

## Git and GitHub Practices

* Branch from `main` using `<type>/<short_topic>`, for example `feat/watermark_table` or `fix/silver_trips_dedupe`.
* Commit messages follow Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, `ci:`. Subject in imperative mood, 72 characters or fewer.
* One logical change per commit. Keep pull requests small and focused.
* Stage files by name. Do not run `git add .` or `git add -A` without reviewing `git status`.
* Do not commit or push unless the user asks.
* Never force push to `main`. Never rewrite published history. Never use `--no-verify` to skip hooks.
* Never commit secrets, `.env`, credentials, tokens, generated data, or large binaries. Run `gitleaks` before committing.
* Every pull request needs a description, passing CI, and at least one review. Squash merge into `main`.
* Tag releases with semantic versions such as `v1.2.0`. Never deploy `latest`.
* Keep `CODEOWNERS`, the pull request template, and Dependabot config current.

## Documentation Style

* Plain, professional prose. No hyphens in prose. Hyphens are fine inside code, commands, file names, and tool names.
* No emojis. No marketing language.
* Update `docs/data_dictionary.md` when a gold column changes and `ARCHITECTURE.md` when a design decision changes.

## Data Pipeline Rules

* Data flows only bronze, then silver, then gold. No layer reads from a layer above it.
* Every procedure and job is idempotent. Retries and backfills must be safe.
* Watermarks advance only after the gold quality gate passes.
* Timestamps are stored in UTC.
* Every fact has a declared grain with a unique constraint. Fact foreign keys resolve to a dimension row or the unknown member `-1`.
* The stream never writes to silver or gold.
* Dashboards and reports read only gold views.
* Never use `TRUNCATE`, `DROP`, or `DELETE` without a `WHERE` on shared data unless the table is a documented full load staging swap.

## Testing and Definition of Done

A change is done only when all of these hold:

1. `make test` passes locally.
2. New or changed procedures have a SQL test with fixtures.
3. New tables have entries in `dq.test_catalog`.
4. The idempotency test still passes (a double run gives the same result).
5. Linters report no errors: `ruff`, `sqlfluff`, `shellcheck`, `yamllint`.
6. Docs are updated and no secrets are present in the diff.

## Boundaries

Always:

* Read existing code and match its patterns before writing new code.
* Run the relevant linter and tests before finishing.
* Keep changes minimal and scoped to the request.

Ask first:

* Adding a new dependency or service.
* Changing a table grain, a load mode, or a migration that is already merged.
* Deleting files, dropping objects, or changing CI and deploy configuration.

Never:

* Hardcode secrets or commit `.env`.
* Edit generated files, lock files by hand, or merged migrations.
* Bypass quality gates, tests, or hooks.
* Run destructive commands against shared or production data.
