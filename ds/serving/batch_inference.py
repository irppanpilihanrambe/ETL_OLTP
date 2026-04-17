"""
batch_inference.py
------------------
Load latest MLflow model and run batch predictions.
Results written to PostgreSQL prediction tables.

Usage:
    python ds/serving/batch_inference.py --model churn
    python ds/serving/batch_inference.py --model stockout
    python ds/serving/batch_inference.py --model demand
"""

import os
import click
import mlflow
import pandas as pd
from pathlib import Path
from datetime import date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

FEATURES_DIR = Path("ds/features/output")

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))


def get_engine():
    return create_engine(os.environ["DATABASE_URL"])


def load_latest_model(experiment_name: str):
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["start_time DESC"],
        max_results=1,
    )
    if not runs:
        raise ValueError(f"No runs found for experiment: {experiment_name}")
    run_id = runs[0].info.run_id
    logger.info(f"  Loading model from run: {run_id}")
    return mlflow.pyfunc.load_model(f"runs:/{run_id}/model")


def run_churn(engine):
    logger.info("Running churn batch inference ...")
    df = pd.read_parquet(FEATURES_DIR / "churn_features.parquet")
    feature_cols = ["recency_days", "frequency", "monetary",
                    "avg_basket_size", "basket_variance", "active_months"]
    df = df.dropna(subset=feature_cols)

    model = load_latest_model("customer_churn")
    df["churn_probability"] = model.predict(df[feature_cols])
    df["prediction_date"]   = date.today()
    df["label"] = (df["churn_probability"] >= 0.5).astype(int)

    out = df[["customer_id", "churn_probability", "label", "prediction_date"]]
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE IF EXISTS predictions_churn;"))
    out.to_sql("predictions_churn", engine, if_exists="append", index=False)
    logger.success(f"  Written {len(out):,} churn scores to predictions_churn")


def run_stockout(engine):
    logger.info("Running stockout risk batch inference ...")
    df = pd.read_parquet(FEATURES_DIR / "stockout_features.parquet")
    feature_cols = ["produced", "demanded", "gap",
                    "fill_rate_lag1", "fill_rate_rolling", "demand_trend"]
    df = df.dropna(subset=feature_cols)

    model = load_latest_model("stockout_risk")
    df["stockout_probability"] = model.predict(df[feature_cols])
    df["prediction_date"]      = date.today()
    df["risk_label"] = (df["stockout_probability"] >= 0.5).astype(int)

    out = df[["branch_id", "product_id", "week", "stockout_probability", "risk_label", "prediction_date"]]
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE IF EXISTS predictions_stockout;"))
    out.to_sql("predictions_stockout", engine, if_exists="append", index=False)
    logger.success(f"  Written {len(out):,} stockout scores to predictions_stockout")


def run_demand(engine):
    logger.info("Running demand forecast batch inference ...")
    df = pd.read_parquet(FEATURES_DIR / "demand_features.parquet")
    df["ds"] = pd.to_datetime(df["ds"])

    model = load_latest_model("demand_forecasting")

    results = []
    for region_id in df["region_id"].unique():
        region_df = df[df["region_id"] == region_id].copy()
        future = model.make_future_dataframe(periods=30)
        forecast = model.predict(future)
        forecast["region_id"]       = region_id
        forecast["prediction_date"] = date.today()
        results.append(forecast[["ds", "region_id", "yhat", "yhat_lower", "yhat_upper", "prediction_date"]])

    out = pd.concat(results, ignore_index=True)
    out = out.rename(columns={"ds": "forecast_date", "yhat": "predicted_orders"})
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE IF EXISTS predictions_demand;"))
    out.to_sql("predictions_demand", engine, if_exists="append", index=False)
    logger.success(f"  Written {len(out):,} demand forecast rows to predictions_demand")


@click.command()
@click.option("--model", required=True,
              type=click.Choice(["churn", "stockout", "demand"]),
              help="Which model to run batch inference for")
def main(model):
    engine = get_engine()
    if model == "churn":
        run_churn(engine)
    elif model == "stockout":
        run_stockout(engine)
    elif model == "demand":
        run_demand(engine)
    logger.success(f"Batch inference [{model}] complete.")


if __name__ == "__main__":
    main()
