# ETL OLTP — Supply Chain Data Platform

> End-to-end data project: **71.9 million records** | PostgreSQL · Airflow · dbt · Python

[![DE Pipeline](https://img.shields.io/badge/DE-PostgreSQL%20%2B%20Airflow-blue)](./de)
[![DA Models](https://img.shields.io/badge/DA-dbt%20%2B%20SQL-green)](./da)
[![DS Models](https://img.shields.io/badge/DS-Python%20%2B%20MLflow-purple)](./ds)

---

## Overview

This monorepo contains the complete Supply Chain Data Platform project — from raw CSV ingestion to production ML models — built by three data roles working in parallel.

| Table | Type | Rows |
|---|---|---|
| `order_items` | Fact | 50,046,909 |
| `orders` | Fact | 19,961,967 |
| `daily_production` | Fact | 1,037,791 |
| `customers` | Dimension | 200,000 |

---

## Project Structure

```
ETL_OLTP/
├── de/                     # Data Engineering
│   ├── schema/             # DDL — Snowflake Schema
│   ├── ingestion/          # Bulk COPY pipeline scripts
│   ├── quality/            # Data quality checks
│   └── dags/               # Airflow DAGs (ingestion)
│
├── da/                     # Data Analysis
│   ├── models/             # Materialized view definitions
│   ├── queries/            # KPI SQL queries
│   └── dashboards/         # Dashboard config (Superset/Metabase)
│
├── ds/                     # Data Science
│   ├── features/           # Feature engineering
│   ├── models/             # Training scripts (Prophet, XGBoost, RF)
│   └── serving/            # Batch inference + Airflow DAGs
│
├── dbt/                    # dbt project (shared transform layer)
│   ├── models/
│   │   ├── staging/        # Raw → cleaned
│   │   ├── marts/          # Business-level aggregations
│   │   └── analysis/       # KPI models for DA
│   ├── tests/              # dbt data tests
│   └── macros/             # Reusable SQL macros
│
├── infra/                  # Docker, docker-compose
├── docs/                   # Architecture diagrams, data dictionary
├── scripts/                # Helper scripts
└── .github/workflows/      # CI/CD pipelines
```

---

## Quick Start

### 1. Clone & setup environment

```bash
git clone https://github.com/<your-username>/ETL_OLTP.git
cd ETL_OLTP
cp .env.example .env          # fill in your credentials
```

### 2. Spin up all services

```bash
docker-compose up -d
# Services: PostgreSQL (5432) · Airflow (8080) · dbt runner
```

### 3. Run DE pipeline (schema + ingestion)

```bash
# Apply Snowflake Schema DDL
psql $DATABASE_URL -f de/schema/01_dimensions.sql
psql $DATABASE_URL -f de/schema/02_facts.sql

# Bulk load CSVs (71.9M rows)
python de/ingestion/bulk_load.py --table all

# Validate data quality
python de/quality/run_checks.py
```

### 4. Run dbt transforms

```bash
cd dbt
dbt deps
dbt run --select staging
dbt run --select marts
dbt test
```

### 5. Run DS training pipeline

```bash
python ds/models/train_forecasting.py
python ds/models/train_churn.py
python ds/models/train_stockout.py
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Database | PostgreSQL 15 |
| Orchestration | Apache Airflow 2.8 |
| Transformation | dbt-core 1.7 |
| ML | scikit-learn, Prophet, XGBoost |
| Experiment Tracking | MLflow |
| Containerization | Docker + docker-compose |
| CI/CD | GitHub Actions |

---

## Team Roles

| Role | Owns |
|---|---|
| **Data Engineer** | `de/`, `infra/`, Airflow ingestion DAGs |
| **Data Analyst** | `da/`, `dbt/models/marts/`, `dbt/models/analysis/` |
| **Data Scientist** | `ds/`, Airflow serving DAGs |

---

## License

MIT
