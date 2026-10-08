# lakehouse make targets (ARCHITECTURE.md §15.5). Tabs required below.
#   make up              core stack (PROFILE=core|stream|all)
#   make logs SVC=kafka  follow one service

PROFILE ?= core
COMPOSE = docker compose -f compose.yml --profile $(PROFILE)

.PHONY: help up down ps logs pull lint test eda migrate run-batch dq report

help:
	@echo "up | down | ps | logs | pull | lint | test | eda | migrate | run-batch | dq | report"

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f $(SVC)

pull:
	$(COMPOSE) pull

lint:
	uv run python -m compileall -q src scripts

test: lint
	uv run pytest tests/unit tests/smoke -q

eda:
	uv run scripts/run_notebooks.py

migrate: ## Apply pending sql/scripts once each, tracked in ops.schema_migrations
	uv run scripts/run_migrate.py && uv run scripts/run_procedures.py

run-batch:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger lh_daily_batch

dq:
	uv run scripts/run_gx_bronze.py && uv run scripts/run_gx_silver.py && uv run scripts/run_gx_gold.py && uv run scripts/run_dq_checks.py

report:
	@echo "LaTeX report build lands in M6"
	@exit 1
