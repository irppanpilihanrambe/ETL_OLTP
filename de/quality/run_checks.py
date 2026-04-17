"""
run_checks.py
-------------
Post-ingestion data quality validation.
Checks: row counts, orphan FKs, NULLs, duplicates.

Usage:
    python de/quality/run_checks.py
    python de/quality/run_checks.py --fail-fast
"""

import os
import sys
import click
import psycopg2
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

EXPECTED_MINIMUMS = {
    "regions":          1,
    "suppliers":        1,
    "branches":         1,
    "products":         1,
    "customers":        200_000,
    "orders":           19_000_000,
    "order_items":      50_000_000,
    "daily_production": 1_000_000,
}

CHECKS = [
    {
        "name": "Row count — orders",
        "sql":  "SELECT COUNT(*) FROM orders;",
        "expect": lambda n: n >= EXPECTED_MINIMUMS["orders"],
        "msg":  f"Expected >= {EXPECTED_MINIMUMS['orders']:,}",
    },
    {
        "name": "Row count — order_items",
        "sql":  "SELECT COUNT(*) FROM order_items;",
        "expect": lambda n: n >= EXPECTED_MINIMUMS["order_items"],
        "msg":  f"Expected >= {EXPECTED_MINIMUMS['order_items']:,}",
    },
    {
        "name": "Row count — customers",
        "sql":  "SELECT COUNT(*) FROM customers;",
        "expect": lambda n: n >= EXPECTED_MINIMUMS["customers"],
        "msg":  f"Expected >= {EXPECTED_MINIMUMS['customers']:,}",
    },
    {
        "name": "Row count — daily_production",
        "sql":  "SELECT COUNT(*) FROM daily_production;",
        "expect": lambda n: n >= EXPECTED_MINIMUMS["daily_production"],
        "msg":  f"Expected >= {EXPECTED_MINIMUMS['daily_production']:,}",
    },
    {
        "name": "Orphan FK — order_items → orders",
        "sql":  """
            SELECT COUNT(*) FROM order_items oi
            LEFT JOIN orders o ON oi.order_id = o.order_id
            WHERE o.order_id IS NULL;
        """,
        "expect": lambda n: n == 0,
        "msg":  "Expected 0 orphan order_items",
    },
    {
        "name": "Orphan FK — orders → customers",
        "sql":  """
            SELECT COUNT(*) FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL;
        """,
        "expect": lambda n: n == 0,
        "msg":  "Expected 0 orphan orders",
    },
    {
        "name": "Orphan FK — daily_production → suppliers",
        "sql":  """
            SELECT COUNT(*) FROM daily_production dp
            LEFT JOIN suppliers s ON dp.supplier_id = s.supplier_id
            WHERE s.supplier_id IS NULL;
        """,
        "expect": lambda n: n == 0,
        "msg":  "Expected 0 orphan production rows",
    },
    {
        "name": "NULL check — orders.order_date",
        "sql":  "SELECT COUNT(*) FROM orders WHERE order_date IS NULL;",
        "expect": lambda n: n == 0,
        "msg":  "order_date must not be NULL",
    },
    {
        "name": "NULL check — order_items.quantity",
        "sql":  "SELECT COUNT(*) FROM order_items WHERE quantity IS NULL OR quantity <= 0;",
        "expect": lambda n: n == 0,
        "msg":  "quantity must be > 0",
    },
    {
        "name": "Duplicate PK — orders",
        "sql":  """
            SELECT COUNT(*) FROM (
                SELECT order_id, COUNT(*) c FROM orders
                GROUP BY order_id HAVING COUNT(*) > 1
            ) t;
        """,
        "expect": lambda n: n == 0,
        "msg":  "Duplicate order_id detected",
    },
]


def run_checks(fail_fast: bool = False):
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    passed = 0
    failed = 0

    logger.info(f"Running {len(CHECKS)} data quality checks...")

    for check in CHECKS:
        try:
            with conn.cursor() as cur:
                cur.execute(check["sql"])
                result = cur.fetchone()[0]

            ok = check["expect"](result)
            if ok:
                logger.success(f"  PASS  {check['name']} → {result:,}")
                passed += 1
            else:
                logger.error(f"  FAIL  {check['name']} → {result:,}  ({check['msg']})")
                failed += 1
                if fail_fast:
                    break
        except Exception as e:
            logger.error(f"  ERROR {check['name']}: {e}")
            failed += 1
            if fail_fast:
                break

    conn.close()
    logger.info(f"\nResults: {passed} passed, {failed} failed out of {len(CHECKS)} checks")
    return failed == 0


@click.command()
@click.option("--fail-fast", is_flag=True, default=False,
              help="Stop on first failure")
def main(fail_fast):
    success = run_checks(fail_fast)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
