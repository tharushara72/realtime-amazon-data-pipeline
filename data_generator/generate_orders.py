# ================================================================
# DATA GENERATOR — Simulates Amazon-style order events
# Sends events to Kafka topics every few seconds
# Why: We need realistic streaming data to test our pipeline
# ================================================================

import json
import time
import uuid
import random
import logging
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

fake = Faker()
random.seed(42)

# ─── Configuration ──────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPICS = {
    "orders":    "ecom.orders",
    "customers": "ecom.customers",
    "products":  "ecom.products",
    "deliveries":"ecom.deliveries",
}

# ─── Static reference data (like a real catalogue) ──────────────
CATEGORIES = {
    "Electronics": ["iPhone 15", "Samsung TV", "Sony Headphones",
                    "iPad Pro", "MacBook Air", "Gaming Mouse"],
    "Clothing":    ["Nike Shoes", "Levi Jeans", "Zara Jacket",
                    "Adidas T-Shirt", "H&M Dress", "Puma Shorts"],
    "Books":       ["Atomic Habits", "Clean Code", "The Lean Startup",
                    "Deep Work", "Zero to One", "Rich Dad Poor Dad"],
    "Home":        ["Coffee Maker", "Air Purifier", "Blender",
                    "Vacuum Cleaner", "Desk Lamp", "Pillow Set"],
    "Food":        ["Protein Powder", "Green Tea", "Olive Oil",
                    "Granola Bars", "Vitamin C", "Almond Milk"],
}

BRANDS = {
    "Electronics": ["Apple", "Samsung", "Sony", "Dell", "Logitech"],
    "Clothing":    ["Nike", "Adidas", "Zara", "H&M", "Puma"],
    "Books":       ["Penguin", "HarperCollins", "OReilly", "Wiley"],
    "Home":        ["Philips", "Dyson", "Bosch", "Panasonic"],
    "Food":        ["Optimum Nutrition", "Lipton", "Bertolli", "Quaker"],
}

MEMBERSHIP_TIERS = ["FREE", "PRIME", "PREMIUM"]
ORDER_STATUSES   = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
PAYMENT_METHODS  = ["CARD", "UPI", "WALLET", "COD"]
REGIONS          = ["North", "South", "East", "West", "Central"]


# ─── Generator functions ─────────────────────────────────────────

def make_customer() -> dict:
    """Generate one realistic customer profile"""
    return {
        "customer_uuid":   str(uuid.uuid4()),
        "full_name":       fake.name(),
        "email":           fake.email(),
        "age":             random.randint(18, 65),
        "gender":          random.choice(["Male", "Female", "Other"]),
        "membership_tier": random.choice(MEMBERSHIP_TIERS),
        "city":            fake.city(),
        "country":         fake.country(),
        "created_at":      datetime.now().isoformat(),
    }


def make_product() -> dict:
    """Generate one realistic product"""
    category = random.choice(list(CATEGORIES.keys()))
    name     = random.choice(CATEGORIES[category])
    brand    = random.choice(BRANDS[category])

    # Realistic price ranges per category
    price_ranges = {
        "Electronics": (50,  1500),
        "Clothing":    (15,  200),
        "Books":       (8,   60),
        "Home":        (20,  500),
        "Food":        (5,   80),
    }
    lo, hi    = price_ranges[category]
    unit_price = round(random.uniform(lo, hi), 2)
    cost_price = round(unit_price * random.uniform(0.4, 0.7), 2)

    return {
        "product_uuid": str(uuid.uuid4()),
        "product_name": f"{brand} {name}",
        "category":     category,
        "subcategory":  name,
        "brand":        brand,
        "unit_price":   unit_price,
        "cost_price":   cost_price,
        "created_at":   datetime.now().isoformat(),
    }


def make_order(customer: dict, product: dict) -> dict:
    """Generate one order event linking customer + product"""
    quantity     = random.randint(1, 5)
    discount_pct = random.choice([0, 5, 10, 15, 20, 25])
    unit_price   = product["unit_price"]
    total_amount = round(unit_price * quantity, 2)
    final_amount = round(total_amount * (1 - discount_pct / 100), 2)
    profit       = round(final_amount - (product["cost_price"] * quantity), 2)
    status       = random.choices(
        ORDER_STATUSES,
        weights=[20, 25, 45, 10]   # DELIVERED is most common
    )[0]

    ordered_at  = datetime.now() - timedelta(days=random.randint(0, 30))
    shipped_at  = ordered_at + timedelta(days=1) if status != "PLACED" else None
    delivered_at = shipped_at + timedelta(days=random.randint(2, 7)) \
                   if status == "DELIVERED" else None

    return {
        "order_uuid":      str(uuid.uuid4()),
        "customer_uuid":   customer["customer_uuid"],
        "product_uuid":    product["product_uuid"],
        "quantity":        quantity,
        "unit_price":      unit_price,
        "total_amount":    total_amount,
        "discount_pct":    discount_pct,
        "final_amount":    final_amount,
        "profit":          profit,
        "order_status":    status,
        "payment_method":  random.choice(PAYMENT_METHODS),
        "delivery_days":   random.randint(2, 10),
        "is_returned":     random.random() < 0.05,   # 5% return rate
        "region":          random.choice(REGIONS),
        "city":            customer["city"],
        "country":         customer["country"],
        "ordered_at":      ordered_at.isoformat(),
        "shipped_at":      shipped_at.isoformat() if shipped_at else None,
        "delivered_at":    delivered_at.isoformat() if delivered_at else None,
    }


def make_delivery(order: dict) -> dict:
    """Generate a delivery status update event"""
    return {
        "delivery_uuid": str(uuid.uuid4()),
        "order_uuid":    order["order_uuid"],
        "status":        order["order_status"],
        "carrier":       random.choice(["FedEx", "DHL", "UPS", "BlueDart"]),
        "tracking_no":   fake.bothify(text="??########").upper(),
        "updated_at":    datetime.now().isoformat(),
    }


# ─── Kafka Producer ──────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    """
    Why JSON serializer?
    Kafka sends raw bytes. We convert Python dicts → JSON strings → bytes
    so the consumer can deserialize them back easily.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def send(producer: KafkaProducer, topic: str, key: str, data: dict):
    producer.send(topic, key=key, value=data)
    log.info(f"→ [{topic}] {key[:8]}… | {list(data.items())[:3]}")


# ─── Main loop ───────────────────────────────────────────────────

def main():
    log.info("Starting data generator — connecting to Kafka...")
    producer = create_producer()
    log.info("Connected! Generating events every 2 seconds...")

    event_count = 0
    while True:
        # Generate linked entities
        customer = make_customer()
        product  = make_product()
        order    = make_order(customer, product)
        delivery = make_delivery(order)

        # Send to Kafka topics
        send(producer, TOPICS["customers"], customer["customer_uuid"], customer)
        send(producer, TOPICS["products"],  product["product_uuid"],   product)
        send(producer, TOPICS["orders"],    order["order_uuid"],        order)
        send(producer, TOPICS["deliveries"],delivery["delivery_uuid"],  delivery)

        event_count += 1
        log.info(f"✓ Event batch #{event_count} sent\n")
        time.sleep(2)   # 1 batch every 2 seconds = real-time feel


if __name__ == "__main__":
    main()