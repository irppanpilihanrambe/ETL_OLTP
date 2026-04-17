"""
dag_model_serving.py
--------------------
Airflow DAG: Weekly batch inference for all 3 ML models.
Results written back to PostgreSQL for dashboard consumption.

Schedule: every Sunday at 02:00
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner":            "data-scientist",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="supply_chain_model_serving",
    description="Weekly batch inference: demand forecast, churn, stockout risk",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * 0",   # Every Sunday at 02:00
    catchup=False,
    tags=["DS", "serving", "supply-chain"],
) as dag:

    build_features = BashOperator(
        task_id="build_features",
        bash_command="python /opt/airflow/scripts/ds/features/feature_engineering.py --model all",
        doc_md="Extract and engineer features from PostgreSQL for all models",
        execution_timeout=timedelta(hours=2),
    )

    run_demand_forecast = BashOperator(
        task_id="batch_demand_forecast",
        bash_command="python /opt/airflow/scripts/ds/serving/batch_inference.py --model demand",
        doc_md="30-day demand forecast per region → postgres table: predictions_demand",
    )

    run_churn_scoring = BashOperator(
        task_id="batch_churn_scoring",
        bash_command="python /opt/airflow/scripts/ds/serving/batch_inference.py --model churn",
        doc_md="Churn probability per customer → postgres table: predictions_churn",
    )

    run_stockout_risk = BashOperator(
        task_id="batch_stockout_risk",
        bash_command="python /opt/airflow/scripts/ds/serving/batch_inference.py --model stockout",
        doc_md="Stockout risk score per branch/product → postgres table: predictions_stockout",
    )

    notify_done = BashOperator(
        task_id="notify_completion",
        bash_command="""
            echo "All model predictions written to PostgreSQL at $(date)"
        """,
        doc_md="Log completion — extend with Slack/email notification as needed",
    )

    # Parallel model inference after feature build
    build_features >> [run_demand_forecast, run_churn_scoring, run_stockout_risk] >> notify_done
