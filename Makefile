.DEFAULT_GOAL := help
.NOTPARALLEL:
.PHONY: help build up down logs test verify retrain stream-start stream-stop demo-invalid

help:
	@echo "make up / down / logs / test / verify / retrain"
	@echo "make stream-start / stream-stop / demo-invalid"
	@echo "PowerShell equivalents are in README."

build:
	docker compose build scoring-service airflow-init
up: build
	docker compose up -d --no-build
down:
	docker compose --profile ui down
logs:
	docker compose logs --tail=100 scoring-service scores-sink transaction-simulator grafana
test: build
	docker compose run --rm --no-deps tools python -m unittest discover -s tests -v
verify:
	python -m scripts.verify_monitoring
retrain:
	docker compose exec airflow-scheduler airflow dags trigger retrain_model
stream-start:
	docker compose up -d --no-deps transaction-simulator
stream-stop:
	docker compose stop transaction-simulator
demo-invalid:
	docker compose run --rm --no-deps transaction-simulator python -m transaction_simulator.app --rate 10 --limit 900 --invalid-ratio 0.1
