"""
train_churn.py
--------------
Customer churn prediction using Logistic Regression (baseline) + Random Forest.
Handles class imbalance via class_weight='balanced'.
Logs all experiments to MLflow.

Usage:
    python ds/models/train_churn.py
"""

import os
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model  import LogisticRegression
from sklearn.ensemble      import RandomForestClassifier
from sklearn.pipeline      import Pipeline
from sklearn.metrics       import (
    roc_auc_score, f1_score, classification_report,
    precision_score, recall_score
)
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

FEATURES_PATH = Path("ds/features/output/churn_features.parquet")
FEATURE_COLS  = [
    "recency_days", "frequency", "monetary",
    "avg_basket_size", "basket_variance", "active_months"
]
TARGET_COL    = "churned"
TEST_SIZE     = 0.15
VAL_SIZE      = 0.15
RANDOM_STATE  = 42

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
mlflow.set_experiment("customer_churn")


def load_features() -> tuple:
    logger.info("Loading churn features ...")
    df = pd.read_parquet(FEATURES_PATH)
    df = df.dropna(subset=FEATURE_COLS + [TARGET_COL])

    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    logger.info(f"  Dataset: {len(df):,} rows | churn rate: {y.mean():.2%}")

    X_temp, X_test, y_temp, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, stratify=y, random_state=RANDOM_STATE
    )
    X_train, X_val, y_train, y_val = train_test_split(
        X_temp, y_temp, test_size=VAL_SIZE / (1 - TEST_SIZE),
        stratify=y_temp, random_state=RANDOM_STATE
    )
    logger.info(f"  Train: {len(X_train):,} | Val: {len(X_val):,} | Test: {len(X_test):,}")
    return X_train, X_val, X_test, y_train, y_val, y_test


def evaluate_model(model, X, y, split_name: str) -> dict:
    y_pred  = model.predict(X)
    y_proba = model.predict_proba(X)[:, 1]
    return {
        f"{split_name}_auc":       round(roc_auc_score(y, y_proba), 4),
        f"{split_name}_f1":        round(f1_score(y, y_pred, average="weighted"), 4),
        f"{split_name}_precision": round(precision_score(y, y_pred, zero_division=0), 4),
        f"{split_name}_recall":    round(recall_score(y, y_pred, zero_division=0), 4),
    }


def train_logistic(X_train, y_train) -> Pipeline:
    pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(
            class_weight="balanced",
            max_iter=1000,
            random_state=RANDOM_STATE
        ))
    ])
    pipe.fit(X_train, y_train)
    return pipe


def train_random_forest(X_train, y_train) -> Pipeline:
    pipe = Pipeline([
        ("clf", RandomForestClassifier(
            n_estimators=300,
            max_depth=12,
            class_weight="balanced",
            n_jobs=-1,
            random_state=RANDOM_STATE
        ))
    ])
    pipe.fit(X_train, y_train)
    return pipe


def log_feature_importance(model, feature_names):
    clf = model.named_steps["clf"]
    if hasattr(clf, "feature_importances_"):
        fi = pd.Series(clf.feature_importances_, index=feature_names)
        fi = fi.sort_values(ascending=False)
        logger.info("  Feature importances:\n" + fi.to_string())
        mlflow.log_dict(fi.to_dict(), "feature_importance.json")


def main():
    X_train, X_val, X_test, y_train, y_val, y_test = load_features()

    # ── Logistic Regression baseline ──
    logger.info("Training Logistic Regression baseline ...")
    with mlflow.start_run(run_name="churn_logistic_regression"):
        mlflow.log_params({
            "model":         "LogisticRegression",
            "class_weight":  "balanced",
            "features":      FEATURE_COLS,
            "train_samples": len(X_train),
        })
        lr = train_logistic(X_train, y_train)
        metrics = {**evaluate_model(lr, X_val, y_val, "val"),
                   **evaluate_model(lr, X_test, y_test, "test")}
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(lr, "model")
        logger.info(f"  LR  val_auc={metrics['val_auc']} | test_auc={metrics['test_auc']}")

    # ── Random Forest ──
    logger.info("Training Random Forest ...")
    with mlflow.start_run(run_name="churn_random_forest"):
        mlflow.log_params({
            "model":         "RandomForestClassifier",
            "n_estimators":  300,
            "max_depth":     12,
            "class_weight":  "balanced",
            "train_samples": len(X_train),
        })
        rf = train_random_forest(X_train, y_train)
        metrics = {**evaluate_model(rf, X_val, y_val, "val"),
                   **evaluate_model(rf, X_test, y_test, "test")}
        mlflow.log_metrics(metrics)
        log_feature_importance(rf, FEATURE_COLS)
        mlflow.sklearn.log_model(rf, "model")
        logger.info(f"  RF  val_auc={metrics['val_auc']} | test_auc={metrics['test_auc']}")

        # Detailed report on test set
        report = classification_report(y_test, rf.predict(X_test))
        logger.info(f"\n{report}")

    logger.success("Churn training complete. Check MLflow UI for results.")


if __name__ == "__main__":
    main()
