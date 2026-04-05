# ================================================================
# KAFKA CONSUMER — Reads events from Kafka → writes to PostgreSQL
# Why: Kafka holds raw events. This script transforms and loads
#      them into our star schema tables in real time.
# ================================================================

import json
import logging
import psycopg2
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

# ─── Database connection ─────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

# ─── Upsert functions (insert or skip if already exists) ─────────

def upsert_customer(cur, data: dict):
    cur.execute("""
        INSERT INTO dim_customers (
            customer_uuid, full_name, email, age, gender,
            membership_tier, city, country
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (customer_uuid) DO NOTHING
    """, (
        data["customer_uuid"], data["full_name"], data["email"],
        data["age"], data["gender"], data["membership_tier"],
        data["city"], data["country"],
    ))


def upsert_product(cur, data: dict):
    cur.execute("""
        INSERT INTO dim_products (
            product_uuid, product_name, category, subcategory,
            brand, unit_price, cost_price
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (product_uuid) DO NOTHING
    """, (
        data["product_uuid"], data["product_name"], data["category"],
        data["subcategory"], data["brand"],
        data["unit_price"], data["cost_price"],
    ))


def upsert_location(cur, data: dict) -> int:
    """Insert location and return its ID"""
    cur.execute("""
        INSERT INTO dim_locations (city, state, country, region, postal_code)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING location_id
    """, (
        data.get("city"), None,
        data.get("country"), data.get("region"), None,
    ))
    return cur.fetchone()[0]


def get_time_id(cur, date_str: str) -> int:
    """Look up time_id from dim_time for a given date string"""
    if not date_str:
        return None
    date = datetime.fromisoformat(date_str).date()
    cur.execute(
        "SELECT time_id FROM dim_time WHERE full_date = %s", (date,)
    )
    row = cur.fetchone()
    return row[0] if row else None


def upsert_order(cur, data: dict):
    # Look up foreign keys
    cur.execute(
        "SELECT customer_id FROM dim_customers WHERE customer_uuid = %s",
        (data["customer_uuid"],)
    )
    customer_row = cur.fetchone()
    if not customer_row:
        log.warning(f"Customer not found: {data['customer_uuid']}")
        return

    cur.execute(
        "SELECT product_id FROM dim_products WHERE product_uuid = %s",
        (data["product_uuid"],)
    )
    product_row = cur.fetchone()
    if not product_row:
        log.warning(f"Product not found: {data['product_uuid']}")
        return

    customer_id = customer_row[0]
    product_id  = product_row[0]
    time_id     = get_time_id(cur, data.get("ordered_at"))
    location_id = upsert_location(cur, data)

    cur.execute("""
        INSERT INTO fact_orders (
            order_uuid, customer_id, product_id, time_id, location_id,
            quantity, unit_price, total_amount, discount_pct, final_amount,
            profit, order_status, payment_method, delivery_days,
            is_returned, ordered_at, shipped_at, delivered_at
        ) VALUES (
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s
        )
        ON CONFLICT (order_uuid) DO NOTHING
    """, (
        data["order_uuid"], customer_id, product_id, time_id, location_id,
        data["quantity"], data["unit_price"], data["total_amount"],
        data["discount_pct"], data["final_amount"],
        data["profit"], data["order_status"], data["payment_method"],
        data["delivery_days"], data["is_returned"],
        data.get("ordered_at"), data.get("shipped_at"), data.get("delivered_at"),
    ))


# ─── Topic handlers ──────────────────────────────────────────────

HANDLERS = {
    "ecom.customers": ("customer", upsert_customer),
    "ecom.products":  ("product",  upsert_product),
    "ecom.orders":    ("order",    upsert_order),
    "ecom.deliveries":("delivery", None),  # deliveries update order status
}

# ─── Main consumer loop ──────────────────────────────────────────

def main():
    log.info("Connecting to Kafka...")
    consumer = KafkaConsumer(
        "ecom.customers", "ecom.products",
        "ecom.orders",    "ecom.deliveries",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",   # read from beginning
        group_id="ecom-consumer-group",
    )
    log.info("Connected! Listening for events...")

    conn = get_db_connection()
    conn.autocommit = False

    for message in consumer:
        topic = message.topic
        data  = message.value

        try:
            cur = conn.cursor()

            if topic == "ecom.customers":
                upsert_customer(cur, data)
            elif topic == "ecom.products":
                upsert_product(cur, data)
            elif topic == "ecom.orders":
                upsert_order(cur, data)
            elif topic == "ecom.deliveries":
                pass  # handled via order status

            conn.commit()
            log.info(f"✓ [{topic}] processed")
            cur.close()

        except Exception as e:
            conn.rollback()
            log.error(f"✗ [{topic}] Error: {e}")


if __name__ == "__main__":
    main()