# ================================================================
# AIRFLOW DAG — Orchestrates the batch pipeline every night
# Why Airflow: Runs tasks automatically on a schedule, retries
#              on failure, and gives a visual monitoring dashboard
# ================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import psycopg2
import os
import logging

log = logging.getLogger(__name__)

# ─── Default settings for all tasks ─────────────────────────────
# Why retries=2? If PostgreSQL is temporarily busy, retry
# before marking the job as failed
default_args = {
    "owner":            "data_engineer",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ─── Database connection ─────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host="postgres",          # Docker service name, not localhost
        port=5432,
        database=os.getenv("POSTGRES_DB",       "ecommerce_dw"),
        user=os.getenv("POSTGRES_USER",         "pipeline_user"),
        password=os.getenv("POSTGRES_PASSWORD", "pipeline_pass123"),
    )


# ─── TASK 1: Check data quality ──────────────────────────────────
# Why: Never aggregate bad data. Check first, aggregate second.
def check_data_quality(**context):
    conn = get_conn()
    cur  = conn.cursor()

    # Check 1: No null order UUIDs
    cur.execute("SELECT COUNT(*) FROM fact_orders WHERE order_uuid IS NULL")
    null_count = cur.fetchone()[0]
    if null_count > 0:
        raise ValueError(f"Data quality fail: {null_count} null order UUIDs!")

    # Check 2: No negative amounts
    cur.execute("SELECT COUNT(*) FROM fact_orders WHERE final_amount < 0")
    neg_count = cur.fetchone()[0]
    if neg_count > 0:
        raise ValueError(f"Data quality fail: {neg_count} negative amounts!")

    # Check 3: Orders exist
    cur.execute("SELECT COUNT(*) FROM fact_orders")
    total = cur.fetchone()[0]
    if total == 0:
        raise ValueError("Data quality fail: fact_orders is empty!")

    log.info(f"✓ Data quality passed — {total} orders look clean")
    cur.close()
    conn.close()


# ─── TASK 2: Aggregate daily sales ───────────────────────────────
# Why: Power BI reads from agg_daily_sales for fast dashboard loading
#      Instead of scanning millions of rows, it reads pre-computed totals
def aggregate_daily_sales(**context):
    conn = get_conn()
    cur  = conn.cursor()

    # Delete today's aggregates first (idempotent — safe to re-run)
    cur.execute("""
        DELETE FROM agg_daily_sales
        WHERE sale_date = CURRENT_DATE - INTERVAL '1 day'
    """)

    # Re-compute yesterday's aggregates from fact_orders
    cur.execute("""
        INSERT INTO agg_daily_sales (
            sale_date, category, region,
            total_orders, total_revenue, total_profit,
            avg_order_value, cancelled_orders, returned_orders
        )
        SELECT
            dt.full_date                        AS sale_date,
            dp.category                         AS category,
            dl.region                           AS region,
            COUNT(fo.order_id)                  AS total_orders,
            SUM(fo.final_amount)                AS total_revenue,
            SUM(fo.profit)                      AS total_profit,
            AVG(fo.final_amount)                AS avg_order_value,
            SUM(CASE WHEN fo.order_status = 'CANCELLED' THEN 1 ELSE 0 END)
                                                AS cancelled_orders,
            SUM(CASE WHEN fo.is_returned = TRUE THEN 1 ELSE 0 END)
                                                AS returned_orders
        FROM fact_orders fo
        JOIN dim_time      dt ON fo.time_id     = dt.time_id
        JOIN dim_products  dp ON fo.product_id  = dp.product_id
        JOIN dim_locations dl ON fo.location_id = dl.location_id
        WHERE dt.full_date = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY dt.full_date, dp.category, dl.region
    """)

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM agg_daily_sales")
    count = cur.fetchone()[0]
    log.info(f"✓ Daily aggregation complete — {count} rows in agg_daily_sales")
    cur.close()
    conn.close()


# ─── TASK 3: Compute revenue summary ─────────────────────────────
def compute_revenue_summary(**context):
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        SELECT
            dp.category,
            COUNT(fo.order_id)   AS total_orders,
            SUM(fo.final_amount) AS total_revenue,
            SUM(fo.profit)       AS total_profit,
            ROUND(AVG(fo.final_amount)::numeric, 2) AS avg_order_value
        FROM fact_orders fo
        JOIN dim_products dp ON fo.product_id = dp.product_id
        GROUP BY dp.category
        ORDER BY total_revenue DESC
    """)

    rows = cur.fetchall()
    log.info("=== Revenue by Category ===")
    for row in rows:
        log.info(
            f"  {row[0]:<15} orders={row[1]:>5} "
            f"revenue=${row[2]:>10.2f} "
            f"profit=${row[3]:>10.2f}"
        )

    cur.close()
    conn.close()


# ─── TASK 4: Log pipeline health ─────────────────────────────────
def log_pipeline_health(**context):
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*)                                    AS total_orders,
            SUM(final_amount)                           AS total_revenue,
            ROUND(AVG(final_amount)::numeric, 2)        AS avg_order_value,
            SUM(CASE WHEN order_status='DELIVERED'
                THEN 1 ELSE 0 END)                      AS delivered,
            SUM(CASE WHEN order_status='CANCELLED'
                THEN 1 ELSE 0 END)                      AS cancelled,
            SUM(CASE WHEN is_returned=TRUE
                THEN 1 ELSE 0 END)                      AS returned
        FROM fact_orders
    """)

    row = cur.fetchone()
    log.info("=== Pipeline Health Report ===")
    log.info(f"  Total orders   : {row[0]}")
    log.info(f"  Total revenue  : ${row[1]:,.2f}")
    log.info(f"  Avg order value: ${row[2]}")
    log.info(f"  Delivered      : {row[3]}")
    log.info(f"  Cancelled      : {row[4]}")
    log.info(f"  Returned       : {row[5]}")

    cur.close()
    conn.close()


# ─── Define the DAG ──────────────────────────────────────────────
with DAG(
    dag_id="ecommerce_batch_pipeline",
    description="Nightly batch aggregation for ecommerce data warehouse",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 0 * * *",   # every night at midnight
    catchup=False,
    tags=["ecommerce", "batch", "warehouse"],
) as dag:

    # Task 1
    t1_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )

    # Task 2
    t2_aggregate = PythonOperator(
        task_id="aggregate_daily_sales",
        python_callable=aggregate_daily_sales,
    )

    # Task 3
    t3_revenue = PythonOperator(
        task_id="compute_revenue_summary",
        python_callable=compute_revenue_summary,
    )

    # Task 4
    t4_health = PythonOperator(
        task_id="log_pipeline_health",
        python_callable=log_pipeline_health,
    )

    # ─── Task order (the "directed" part of DAG) ─────────────────
    # Quality check MUST pass before aggregation runs
    # Then revenue and health run in parallel after aggregation
    t1_quality >> t2_aggregate >> [t3_revenue, t4_health]