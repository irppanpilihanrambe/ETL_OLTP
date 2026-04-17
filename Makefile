.PHONY: help up down schema load quality dbt-run dbt-test train ci

help:
	@echo ""
	@echo "ETL OLTP — Supply Chain Data Platform"
	@echo "======================================="
	@echo "  make up          Start all services (PostgreSQL, Airflow, MLflow)"
	@echo "  make down        Stop all services"
	@echo "  make schema      Apply DDL (dimensions + facts)"
	@echo "  make load        Bulk load all CSVs"
	@echo "  make quality     Run data quality checks"
	@echo "  make dbt-run     Run all dbt models"
	@echo "  make dbt-test    Run dbt data tests"
	@echo "  make train       Train all ML models"
	@echo "  make ci          Full pipeline: schema → load → quality → dbt → train"
	@echo ""

up:
	docker-compose up -d
	@echo "Services up. Airflow: http://localhost:8080 | MLflow: http://localhost:5000"

down:
	docker-compose down

schema:
	psql $$DATABASE_URL -f de/schema/01_dimensions.sql
	psql $$DATABASE_URL -f de/schema/02_facts.sql
	@echo "Schema applied."

load:
	python de/ingestion/bulk_load.py --table all

quality:
	python de/quality/run_checks.py --fail-fast

dbt-run:
	cd dbt && dbt run --select staging && dbt run --select marts && dbt run --select analysis

dbt-test:
	cd dbt && dbt test

train:
	python ds/features/feature_engineering.py --model all
	python ds/models/train_forecasting.py
	python ds/models/train_churn.py
	python ds/models/train_stockout.py

ci: schema load quality dbt-run dbt-test train
	@echo "Full pipeline complete."
