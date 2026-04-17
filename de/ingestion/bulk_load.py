"""
bulk_load.py
------------
Bulk load CSVs into PostgreSQL using COPY command.
Handles 71.9M rows across 4 core tables.

Usage:
    python de/ingestion/bulk_load.py --table all
    python de/ingestion/bulk_load.py --table orders
    python de/ingestion/bulk_load.py --table order_items --data-dir /data/raw
"""

import os
import time
import click
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# Load order matters — dimensions before facts
LOAD_SEQUENCE = [
    {
        "table":   "regions",
        "file":    "regions.csv",
        "columns": "region_name, country",
    },
    {
        "table":   "suppliers",
        "file":    "suppliers.csv",
        "columns": "supplier_name, contact_email, region_id",
    },
    {
        "table":   "branches",
        "file":    "branches.csv",
        "columns": "branch_name, region_id, address",
    },
    {
        "table":   "products",
        "file":    "products.csv",
        "columns": "product_name, category, unit_cost",
    },
    {
        "table":   "customers",
        "file":    "customers.csv",
        "columns": "customer_name, email, branch_id, joined_at",
    },
    {
        "table":   "orders",
        "file":    "orders.csv",
        "columns": "customer_id, order_date, status, total_amount",
    },
    {
        "table":   "order_items",
        "file":    "order_items.csv",
        "columns": "order_id, product_id, quantity, unit_price",
    },
    {
        "table":   "daily_production",
        "file":    "daily_production.csv",
        "columns": "supplier_id, product_id, prod_date, qty_produced",
    },
]


def get_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def drop_redundant_columns(conn, table: str):
    """Drop system/redundant columns like column1 that appear in some CSVs."""
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND column_name = 'column1';
        """, (table,))
        if cur.fetchone():
            cur.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS column1;")
            conn.commit()
            logger.info(f"  Dropped redundant 'column1' from {table}")


def get_row_count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        return cur.fetchone()[0]


def bulk_copy(conn, table: str, columns: str, filepath: Path):
    sql = f"""
        COPY {table} ({columns})
        FROM STDIN
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ',',
            NULL '\\N',
            ENCODING 'UTF8'
        );
    """
    start = time.time()
    with conn.cursor() as cur:
        with open(filepath, "r", encoding="utf-8") as f:
            cur.copy_expert(sql, f)
    conn.commit()
    elapsed = time.time() - start
    return elapsed


def load_table(entry: dict, data_dir: Path):
    table    = entry["table"]
    columns  = entry["columns"]
    filepath = data_dir / entry["file"]

    if not filepath.exists():
        logger.warning(f"  File not found: {filepath} — skipping {table}")
        return

    logger.info(f"Loading {table} from {filepath.name} ...")
    conn = get_connection()
    try:
        elapsed = bulk_copy(conn, table, columns, filepath)
        drop_redundant_columns(conn, table)
        count = get_row_count(conn, table)
        logger.success(f"  {table}: {count:,} rows loaded in {elapsed:.1f}s")
    except Exception as e:
        conn.rollback()
        logger.error(f"  FAILED {table}: {e}")
        raise
    finally:
        conn.close()


@click.command()
@click.option("--table",    default="all", help="Table name or 'all'")
@click.option("--data-dir", default=os.getenv("RAW_DATA_DIR", "./data/raw"),
              help="Directory containing CSV files")
def main(table: str, data_dir: str):
    data_path = Path(data_dir)
    logger.info(f"Starting bulk load | data_dir={data_path} | target={table}")

    sequence = LOAD_SEQUENCE if table == "all" else [
        e for e in LOAD_SEQUENCE if e["table"] == table
    ]

    if not sequence:
        logger.error(f"Unknown table: {table}")
        return

    total_start = time.time()
    for entry in sequence:
        load_table(entry, data_path)

    logger.success(f"All done in {time.time() - total_start:.1f}s")


if __name__ == "__main__":
    main()
