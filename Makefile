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
	@echo "pytest suites land with the first DAGs and procedures (M2/M5)"

eda:
	uv run scripts/run_notebooks.py

migrate:
	@echo "versioned migrations land in M1 (sql/migrations + Flyway)"
	@exit 1

run-batch:
	@echo "daily DAG trigger lands in M5 (airflow dags trigger lh_daily_batch)"
	@exit 1

dq:
	@echo "DQ suites land in M3 (catalog + GX)"
	@exit 1

report:
	@echo "LaTeX report build lands in M6"
	@exit 1
