"""
train_forecasting.py
--------------------
Demand forecasting per region using Meta Prophet.
Trains one model per region, logs to MLflow.

Usage:
    python ds/models/train_forecasting.py
    python ds/models/train_forecasting.py --region-id 3
"""

import os
import click
import mlflow
import mlflow.pyfunc
import pandas as pd
import numpy as np
from pathlib import Path
from prophet import Prophet
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

FEATURES_PATH  = Path("ds/features/output/demand_features.parquet")
CUTOFF_TRAIN   = "2023-06-30"
CUTOFF_VAL     = "2023-09-30"
FORECAST_DAYS  = 30

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
mlflow.set_experiment("demand_forecasting")


def split_data(df: pd.DataFrame):
    train = df[df["ds"] <= CUTOFF_TRAIN]
    val   = df[(df["ds"] > CUTOFF_TRAIN) & (df["ds"] <= CUTOFF_VAL)]
    test  = df[df["ds"] > CUTOFF_VAL]
    return train, val, test


def train_prophet(train_df: pd.DataFrame, region_name: str) -> Prophet:
    model = Prophet(
        changepoint_prior_scale=0.05,
        seasonality_mode="multiplicative",
        weekly_seasonality=True,
        yearly_seasonality=True,
        daily_seasonality=False,
    )
    model.add_regressor("avg_unit_price")
    model.add_regressor("rolling_7d_avg")

    prophet_df = train_df.rename(columns={"y": "y"})[
        ["ds", "y", "avg_unit_price", "rolling_7d_avg"]
    ].dropna()

    model.fit(prophet_df)
    return model


def evaluate(model: Prophet, val_df: pd.DataFrame) -> dict:
    future = val_df[["ds", "avg_unit_price", "rolling_7d_avg"]].copy()
    forecast = model.predict(future)
    y_true = val_df["y"].values
    y_pred = forecast["yhat"].clip(lower=0).values

    mape = mean_absolute_percentage_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    return {"mape": round(mape, 4), "rmse": round(rmse, 2)}


@click.command()
@click.option("--region-id", default=None, type=int,
              help="Train for a specific region_id (default: all regions)")
def main(region_id):
    logger.info("Loading demand features ...")
    df = pd.read_parquet(FEATURES_PATH)
    df["ds"] = pd.to_datetime(df["ds"])

    regions = [region_id] if region_id else df["region_id"].unique().tolist()
    logger.info(f"Training Prophet for {len(regions)} region(s) ...")

    for rid in regions:
        region_df   = df[df["region_id"] == rid].copy()
        region_name = region_df["region_name"].iloc[0]

        train, val, test = split_data(region_df)

        with mlflow.start_run(run_name=f"prophet_region_{rid}"):
            mlflow.log_params({
                "region_id":              rid,
                "region_name":            region_name,
                "train_rows":             len(train),
                "val_rows":               len(val),
                "changepoint_prior":      0.05,
                "seasonality_mode":       "multiplicative",
            })

            model   = train_prophet(train, region_name)
            metrics = evaluate(model, val)

            mlflow.log_metrics(metrics)
            mlflow.pyfunc.log_model("model", python_model=model)

            status = "PASS" if metrics["mape"] < 0.10 else "WARN"
            logger.info(
                f"  Region {rid} ({region_name}) | "
                f"MAPE={metrics['mape']:.2%} | RMSE={metrics['rmse']:,.0f} | {status}"
            )

    logger.success("Demand forecasting training complete.")


if __name__ == "__main__":
    main()
