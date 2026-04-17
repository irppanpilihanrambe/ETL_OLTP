# Architecture — Supply Chain Data Platform

## Data Flow

```
Raw CSVs (71.9M rows)
        │
        ▼
┌──────────────────────┐
│  DE: Bulk COPY       │  PostgreSQL COPY command
│  via Airflow DAG     │  Loading sequence: dim → fact
└────────┬─────────────┘
         │
         ▼
┌──────────────────────┐
│  PostgreSQL          │  Snowflake Schema
│  Snowflake Schema    │  FK constraints enforced
└────────┬─────────────┘
         │
         ▼
┌──────────────────────┐
│  DE: Quality Checks  │  Row count, orphan FK,
│  (run_checks.py)     │  NULL, duplicate checks
└────────┬─────────────┘
         │
         ▼
┌──────────────────────┐
│  dbt Transform Layer │  staging → marts → analysis
│  (Airflow triggered) │  Materialized views
└────────┬─────────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐  ┌──────────────────┐
│  BI    │  │  DS Feature      │
│  Tool  │  │  Engineering     │
│(Superset│  │  (chunked SQL)   │
│/Meta)  │  └────────┬─────────┘
└────────┘           │
                      ▼
             ┌──────────────────┐
             │  ML Training     │
             │  Prophet / XGB   │
             │  RandomForest    │
             │  → MLflow logs   │
             └────────┬─────────┘
                      │
                      ▼
             ┌──────────────────┐
             │  Batch Serving   │
             │  (Airflow DAG)   │
             │  → predictions_* │
             │  tables in PG    │
             └──────────────────┘
```

## Schema Diagram

```
regions ──────── branches ──────── customers
   │                                    │
   │                                 orders
   │                                    │
suppliers ── daily_production      order_items
                    │                   │
               products ────────────────┘
```

## Services & Ports

| Service | Port | URL |
|---|---|---|
| PostgreSQL | 5432 | `psql $DATABASE_URL` |
| Airflow Webserver | 8080 | http://localhost:8080 |
| MLflow UI | 5000 | http://localhost:5000 |
