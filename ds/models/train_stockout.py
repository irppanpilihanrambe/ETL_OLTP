"""
train_stockout.py
-----------------
Stockout risk classification per branch/product using XGBoost.
Uses scale_pos_weight to handle class imbalance.

Usage:
    python ds/models/train_stockout.py
"""

import os
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score, f1_score, classification_report
)
from xgboost import XGBClassifier
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

FEATURES_PATH = Path("ds/features/output/stockout_features.parquet")
FEATURE_COLS  = [
    "produced", "demanded", "gap",
    "fill_rate_lag1", "fill_rate_rolling", "demand_trend"
]
TARGET_COL   = "is_stockout"
RANDOM_STATE = 42

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
mlflow.set_experiment("stockout_risk")


def main():
    logger.info("Loading stockout features ...")
    df = pd.read_parquet(FEATURES_PATH)
    df = df.dropna(subset=FEATURE_COLS + [TARGET_COL])

    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    stockout_rate = y.mean()
    scale_pos_weight = round((1 - stockout_rate) / stockout_rate, 2)
    logger.info(f"  Dataset: {len(df):,} rows | stockout rate: {stockout_rate:.2%} | scale_pos_weight: {scale_pos_weight}")

    # Time-aware split (don't shuffle — preserve temporal order)
    split_idx = int(len(df) * 0.70)
    val_idx   = int(len(df) * 0.85)

    X_train, y_train = X.iloc[:split_idx],  y.iloc[:split_idx]
    X_val,   y_val   = X.iloc[split_idx:val_idx], y.iloc[split_idx:val_idx]
    X_test,  y_test  = X.iloc[val_idx:],    y.iloc[val_idx:]

    logger.info(f"  Train: {len(X_train):,} | Val: {len(X_val):,} | Test: {len(X_test):,}")

    with mlflow.start_run(run_name="stockout_xgboost"):
        params = {
            "n_estimators":    500,
            "max_depth":       6,
            "learning_rate":   0.05,
            "subsample":       0.8,
            "colsample_bytree":0.8,
            "scale_pos_weight":scale_pos_weight,
            "eval_metric":     "auc",
            "random_state":    RANDOM_STATE,
            "n_jobs":          -1,
        }
        mlflow.log_params(params)

        model = XGBClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            early_stopping_rounds=30,
            verbose=False,
        )

        # Evaluate
        for split_name, X_s, y_s in [("val", X_val, y_val), ("test", X_test, y_test)]:
            y_pred  = model.predict(X_s)
            y_proba = model.predict_proba(X_s)[:, 1]
            metrics = {
                f"{split_name}_auc": round(roc_auc_score(y_s, y_proba), 4),
                f"{split_name}_f1":  round(f1_score(y_s, y_pred, average="weighted"), 4),
            }
            mlflow.log_metrics(metrics)
            logger.info(f"  {split_name.upper()} — AUC: {metrics[f'{split_name}_auc']} | F1: {metrics[f'{split_name}_f1']}")

        # Feature importance
        fi = pd.Series(model.feature_importances_, index=FEATURE_COLS).sort_values(ascending=False)
        logger.info(f"  Feature importances:\n{fi.to_string()}")
        mlflow.log_dict(fi.to_dict(), "feature_importance.json")

        report = classification_report(y_test, model.predict(X_test))
        logger.info(f"\n{report}")

        mlflow.sklearn.log_model(model, "model")

    logger.success("Stockout risk training complete.")


if __name__ == "__main__":
    main()
