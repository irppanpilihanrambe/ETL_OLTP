"""
dag_ingestion.py
----------------
Airflow DAG: Daily ingestion pipeline
Schedule: @daily (or trigger manually for initial load)

Flow:
  apply_schema → bulk_load → quality_checks → dbt_run
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner":            "data-engineer",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="supply_chain_ingestion",
    description="Bulk load supply chain CSVs into PostgreSQL + dbt transform",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["DE", "ingestion", "supply-chain"],
) as dag:

    apply_schema = BashOperator(
        task_id="apply_schema",
        bash_command="""
            psql $DATABASE_URL -f /opt/airflow/scripts/de/schema/01_dimensions.sql &&
            psql $DATABASE_URL -f /opt/airflow/scripts/de/schema/02_facts.sql
        """,
        doc_md="Apply Snowflake Schema DDL (idempotent — uses IF NOT EXISTS)",
    )

    bulk_load = BashOperator(
        task_id="bulk_load_csvs",
        bash_command="python /opt/airflow/scripts/de/ingestion/bulk_load.py --table all",
        doc_md="Bulk COPY all CSVs into PostgreSQL in dependency order",
        execution_timeout=timedelta(hours=3),
    )

    quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command="python /opt/airflow/scripts/de/quality/run_checks.py --fail-fast",
        doc_md="Validate row counts, FK integrity, NULLs, and duplicates",
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="cd /opt/airflow/scripts/dbt && dbt run --select staging --target prod",
        doc_md="Run dbt staging models (raw → cleaned)",
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command="cd /opt/airflow/scripts/dbt && dbt run --select marts --target prod",
        doc_md="Run dbt mart models (cleaned → business aggregations)",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/scripts/dbt && dbt test --target prod",
        doc_md="Run all dbt data tests",
    )

    apply_indexes = BashOperator(
        task_id="apply_indexes",
        bash_command="psql $DATABASE_URL -f /opt/airflow/scripts/de/schema/03_indexes.sql",
        doc_md="Create B-tree indexes after load (Phase 4 optimization)",
    )

    # DAG dependency chain
    apply_schema >> bulk_load >> quality_checks >> dbt_staging >> dbt_marts >> dbt_test >> apply_indexes
