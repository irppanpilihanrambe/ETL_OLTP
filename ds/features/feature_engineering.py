"""
feature_engineering.py
-----------------------
Extract and engineer features from PostgreSQL for all 3 ML models.
Uses chunked reads to handle 50M+ rows without OOM.

Usage:
    python ds/features/feature_engineering.py --model all
    python ds/features/feature_engineering.py --model churn
"""

import os
import click
import numpy as np
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

OUTPUT_DIR = Path("ds/features/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 500_000


def get_engine():
    return create_engine(os.environ["DATABASE_URL"])


# ─────────────────────────────────────────────
# DEMAND FORECASTING FEATURES
# ─────────────────────────────────────────────

DEMAND_SQL = """
SELECT
    DATE_TRUNC('day', o.order_date)::DATE          AS ds,
    b.region_id,
    r.region_name,
    COUNT(DISTINCT o.order_id)                      AS y,
    SUM(oi.quantity)                                AS total_units,
    AVG(oi.unit_price)                              AS avg_unit_price,
    EXTRACT(DOW   FROM o.order_date)                AS day_of_week,
    EXTRACT(MONTH FROM o.order_date)                AS month,
    EXTRACT(YEAR  FROM o.order_date)                AS year
FROM orders o
JOIN order_items oi ON o.order_id    = oi.order_id
JOIN customers   c  ON o.customer_id = c.customer_id
JOIN branches    b  ON c.branch_id   = b.branch_id
JOIN regions     r  ON b.region_id   = r.region_id
GROUP BY ds, b.region_id, r.region_name, day_of_week, month, year
ORDER BY ds, b.region_id
"""


def build_demand_features(engine) -> pd.DataFrame:
    logger.info("Building demand forecasting features ...")
    df = pd.read_sql(DEMAND_SQL, engine)
    df["ds"] = pd.to_datetime(df["ds"])
    df = df.sort_values(["region_id", "ds"])

    for region_id, grp in df.groupby("region_id"):
        idx = grp.index
        for lag in [7, 14, 30]:
            df.loc[idx, f"lag_{lag}d"] = grp["y"].shift(lag).values
        df.loc[idx, "rolling_7d_avg"]  = grp["y"].shift(1).rolling(7).mean().values
        df.loc[idx, "rolling_30d_avg"] = grp["y"].shift(1).rolling(30).mean().values

    df = df.dropna(subset=["lag_7d", "lag_14d", "lag_30d"])
    logger.success(f"  Demand features: {len(df):,} rows x {df.shape[1]} cols")
    return df


# ─────────────────────────────────────────────
# CUSTOMER CHURN FEATURES
# ─────────────────────────────────────────────

CHURN_SQL = """
SELECT
    o.customer_id,
    MAX(o.order_date)                                     AS last_order_date,
    COUNT(DISTINCT o.order_id)                            AS frequency,
    SUM(oi.quantity * oi.unit_price)                      AS monetary,
    AVG(oi.quantity * oi.unit_price)                      AS avg_basket_size,
    STDDEV(oi.quantity * oi.unit_price)                   AS basket_variance,
    CURRENT_DATE - MAX(o.order_date)                      AS recency_days,
    COUNT(DISTINCT DATE_TRUNC('month', o.order_date))     AS active_months,
    CASE
        WHEN CURRENT_DATE - MAX(o.order_date) > 180 THEN 1 ELSE 0
    END AS churned
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.customer_id
"""


def build_churn_features(engine) -> pd.DataFrame:
    logger.info("Building churn prediction features ...")
    chunks = []
    for chunk in pd.read_sql(CHURN_SQL, engine, chunksize=CHUNK_SIZE):
        chunk["basket_variance"] = chunk["basket_variance"].fillna(0)
        chunk["monetary"]        = chunk["monetary"].clip(lower=0)
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    logger.success(f"  Churn features: {len(df):,} rows | churn rate: {df['churned'].mean():.2%}")
    return df


# ─────────────────────────────────────────────
# STOCKOUT RISK FEATURES
# ─────────────────────────────────────────────

STOCKOUT_SQL = """
WITH prod AS (
    SELECT
        dp.product_id,
        b.branch_id,
        DATE_TRUNC('week', dp.prod_date)::DATE AS week,
        SUM(dp.qty_produced)                    AS produced
    FROM daily_production dp
    JOIN suppliers s ON dp.supplier_id = s.supplier_id
    JOIN branches  b ON s.region_id    = b.region_id
    GROUP BY dp.product_id, b.branch_id, week
),
dem AS (
    SELECT
        oi.product_id,
        c.branch_id,
        DATE_TRUNC('week', o.order_date)::DATE AS week,
        SUM(oi.quantity)                        AS demanded
    FROM order_items oi
    JOIN orders    o ON oi.order_id    = o.order_id
    JOIN customers c ON o.customer_id  = c.customer_id
    GROUP BY oi.product_id, c.branch_id, week
)
SELECT
    COALESCE(p.product_id, d.product_id)   AS product_id,
    COALESCE(p.branch_id,  d.branch_id)    AS branch_id,
    COALESCE(p.week,       d.week)         AS week,
    COALESCE(p.produced, 0)                AS produced,
    COALESCE(d.demanded, 0)                AS demanded,
    COALESCE(p.produced, 0) - COALESCE(d.demanded, 0) AS gap,
    CASE WHEN COALESCE(p.produced, 0) < COALESCE(d.demanded, 0)
         THEN 1 ELSE 0 END                AS is_stockout
FROM prod p
FULL OUTER JOIN dem d
    ON  p.product_id = d.product_id
    AND p.branch_id  = d.branch_id
    AND p.week       = d.week
"""


def build_stockout_features(engine) -> pd.DataFrame:
    logger.info("Building stockout risk features ...")
    df = pd.read_sql(STOCKOUT_SQL, engine)
    df["week"] = pd.to_datetime(df["week"])
    df = df.sort_values(["branch_id", "product_id", "week"])

    grp = df.groupby(["branch_id", "product_id"])
    df["fill_rate_lag1"]    = grp["gap"].shift(1)
    df["fill_rate_rolling"] = grp["gap"].shift(1).rolling(4).mean().reset_index(level=[0,1], drop=True)
    df["demand_trend"]      = grp["demanded"].shift(1).rolling(4).apply(
        lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) == 4 else np.nan,
        raw=True
    ).reset_index(level=[0,1], drop=True)

    df = df.dropna(subset=["fill_rate_lag1"])
    logger.success(f"  Stockout features: {len(df):,} rows | stockout rate: {df['is_stockout'].mean():.2%}")
    return df


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

@click.command()
@click.option("--model", default="all",
              type=click.Choice(["all", "demand", "churn", "stockout"]),
              help="Which feature set to build")
def main(model):
    engine = get_engine()

    if model in ("all", "demand"):
        df = build_demand_features(engine)
        df.to_parquet(OUTPUT_DIR / "demand_features.parquet", index=False)
        logger.info(f"  Saved → {OUTPUT_DIR}/demand_features.parquet")

    if model in ("all", "churn"):
        df = build_churn_features(engine)
        df.to_parquet(OUTPUT_DIR / "churn_features.parquet", index=False)
        logger.info(f"  Saved → {OUTPUT_DIR}/churn_features.parquet")

    if model in ("all", "stockout"):
        df = build_stockout_features(engine)
        df.to_parquet(OUTPUT_DIR / "stockout_features.parquet", index=False)
        logger.info(f"  Saved → {OUTPUT_DIR}/stockout_features.parquet")

    logger.success("Feature engineering complete.")


if __name__ == "__main__":
    main()
