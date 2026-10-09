# Runs lakehouse compose and pipeline entry points.
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
PROFILE ?= core
SVC ?= airflow-scheduler
COMPOSE ?= docker compose -f compose.yml --profile $(PROFILE)

.PHONY: help up down ps logs pull lint test eda migrate run-batch dq report

help: ## Show this help
	@uv run python -c "import re,pathlib; text=pathlib.Path('Makefile').read_text(); rows=sorted(re.findall(r'^([a-z-]+):.*?## (.*)', text, re.M)); print('\n'.join(f'{n:<10} {d}' for n,d in rows))"

up: ## Start stack for PROFILE (core|stream|all)
	$(COMPOSE) up -d --wait

down: ## Stop stack
	$(COMPOSE) down

ps: ## Show stack status
	$(COMPOSE) ps

logs: ## Follow one service logs, SVC=name
	$(COMPOSE) logs -f $(SVC)

pull: ## Pull compose images
	$(COMPOSE) pull

lint: ## Lint Python and SQL
	uv run ruff check .
	uv run ruff format --check .
	uv run sqlfluff lint sql/

test: lint ## Run lint plus unit, smoke, streaming tests
	uv run pytest tests/unit tests/smoke tests/streaming -q

eda: ## Run profiling notebooks
	uv run python scripts/run_notebooks.py

migrate: ## Apply pending migrations and install procedures
	uv run python scripts/run_migrate.py
	uv run python scripts/run_procedures.py

run-batch: ## Trigger the daily batch DAG once
	$(COMPOSE) exec airflow-scheduler airflow dags trigger lh_daily_batch

dq: ## Run DQ suites
	uv run python scripts/run_gx_bronze.py
	uv run python scripts/run_gx_silver.py
	uv run python scripts/run_gx_gold.py
	uv run python scripts/run_dq_checks.py

report: ## Build the LaTeX report
	@echo "LaTeX report build lands in M6"
	@exit 1
