.DEFAULT_GOAL := help
.NOTPARALLEL:
.PHONY: help prepare build up load deps seed run test evaluate docs serve verify check all down
ifeq ($(IN_CONTAINER),1)
RUN :=
prepare:
	@true
else
RUN := docker compose run --rm dbt
prepare: build up
endif

help:
	@echo "make all       - load, dependencies, seeds, models, tests, evaluator, docs, verification"
	@echo "make check     - loader tests and aggregate reconciliation"
	@echo "make serve     - open dbt Docs at http://localhost:8080"
	@echo "make down      - stop services, preserve data and client IDs"
	@echo "Windows: docker compose build dbt; docker compose run --rm dbt make all IN_CONTAINER=1"

build:
	docker compose build dbt
up:
	docker compose up -d clickhouse
load: prepare
	$(RUN) python -m scripts.load_source
deps: prepare
	$(RUN) dbt deps --project-dir dbt --profiles-dir dbt
	$(RUN) python -m scripts.prepare_packages
seed: prepare
	$(RUN) dbt seed --project-dir dbt --profiles-dir dbt
run: prepare
	$(RUN) dbt run --select package:transaction_analytics --project-dir dbt --profiles-dir dbt
test: prepare
	$(RUN) python -m scripts.run_dbt test --select package:transaction_analytics --log reports/dbt_test.log
evaluate: prepare
	$(RUN) dbt run --select package:dbt_project_evaluator --project-dir dbt --profiles-dir dbt
	$(RUN) python -m scripts.run_dbt test --select package:dbt_project_evaluator --log reports/project_evaluator_test.log
docs: prepare
	$(RUN) dbt docs generate --project-dir dbt --profiles-dir dbt
	$(RUN) python -m scripts.export_lineage
serve:
	docker compose --profile docs up -d dbt-docs
verify: prepare
	$(RUN) python -m scripts.verify_results
check: prepare
	$(RUN) python -m scripts.run_checks
	$(RUN) python -m scripts.verify_results
all: load deps seed run test evaluate docs check
down:
	docker compose --profile docs down
