-- ================================================================
-- ECOMMERCE DATA WAREHOUSE — STAR SCHEMA
-- This file runs automatically when PostgreSQL container starts
-- ================================================================

-- ─── DIMENSION: Customers ───────────────────────────────────────
-- WHO placed the order
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id     SERIAL PRIMARY KEY,
    customer_uuid   VARCHAR(50) UNIQUE NOT NULL,
    full_name       VARCHAR(100),
    email           VARCHAR(100),
    age             INTEGER,
    gender          VARCHAR(20),
    membership_tier VARCHAR(20),  -- FREE, PRIME, PREMIUM
    city            VARCHAR(100),
    country         VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ─── DIMENSION: Products ────────────────────────────────────────
-- WHAT was ordered
CREATE TABLE IF NOT EXISTS dim_products (
    product_id      SERIAL PRIMARY KEY,
    product_uuid    VARCHAR(50) UNIQUE NOT NULL,
    product_name    VARCHAR(200),
    category        VARCHAR(100),  -- Electronics, Clothing, Books etc
    subcategory     VARCHAR(100),
    brand           VARCHAR(100),
    unit_price      DECIMAL(10,2),
    cost_price      DECIMAL(10,2),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ─── DIMENSION: Time ────────────────────────────────────────────
-- WHEN the order happened (pre-computed for fast querying)
CREATE TABLE IF NOT EXISTS dim_time (
    time_id         SERIAL PRIMARY KEY,
    full_date       DATE UNIQUE NOT NULL,
    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    month_name      VARCHAR(20),
    week            INTEGER,
    day_of_month    INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE
);

-- ─── DIMENSION: Locations ───────────────────────────────────────
-- WHERE the order was delivered
CREATE TABLE IF NOT EXISTS dim_locations (
    location_id     SERIAL PRIMARY KEY,
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100),
    region          VARCHAR(50),   -- North, South, East, West
    postal_code     VARCHAR(20)
);

-- ─── FACT TABLE: Orders ─────────────────────────────────────────
-- The core event — one row per order item
-- References all dimension tables (the "star" shape)
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id            SERIAL PRIMARY KEY,
    order_uuid          VARCHAR(50) UNIQUE NOT NULL,

    -- Foreign keys to dimensions
    customer_id         INTEGER REFERENCES dim_customers(customer_id),
    product_id          INTEGER REFERENCES dim_products(product_id),
    time_id             INTEGER REFERENCES dim_time(time_id),
    location_id         INTEGER REFERENCES dim_locations(location_id),

    -- Measures (the numbers we analyze)
    quantity            INTEGER,
    unit_price          DECIMAL(10,2),
    total_amount        DECIMAL(10,2),
    discount_pct        DECIMAL(5,2),
    final_amount        DECIMAL(10,2),
    profit              DECIMAL(10,2),

    -- Order details
    order_status        VARCHAR(30),  -- PLACED, SHIPPED, DELIVERED, CANCELLED
    payment_method      VARCHAR(30),  -- CARD, UPI, WALLET, COD
    delivery_days       INTEGER,
    is_returned         BOOLEAN DEFAULT FALSE,

    -- Timestamps
    ordered_at          TIMESTAMP,
    shipped_at          TIMESTAMP,
    delivered_at        TIMESTAMP,
    ingested_at         TIMESTAMP DEFAULT NOW()
);

-- ─── AGGREGATE TABLE: Daily Sales ───────────────────────────────
-- Pre-computed by Airflow batch job every night
-- Power BI reads THIS for fast dashboard loading
CREATE TABLE IF NOT EXISTS agg_daily_sales (
    agg_id              SERIAL PRIMARY KEY,
    sale_date           DATE,
    category            VARCHAR(100),
    region              VARCHAR(50),
    total_orders        INTEGER,
    total_revenue       DECIMAL(12,2),
    total_profit        DECIMAL(12,2),
    avg_order_value     DECIMAL(10,2),
    cancelled_orders    INTEGER,
    returned_orders     INTEGER,
    computed_at         TIMESTAMP DEFAULT NOW()
);

-- ─── INDEXES for fast querying ──────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer
    ON fact_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product
    ON fact_orders(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_time
    ON fact_orders(time_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status
    ON fact_orders(order_status);
CREATE INDEX IF NOT EXISTS idx_agg_daily_sales_date
    ON agg_daily_sales(sale_date);

-- ─── Seed time dimension (2024-2026) ────────────────────────────
INSERT INTO dim_time (
    full_date, year, quarter, month, month_name,
    week, day_of_month, day_of_week, day_name, is_weekend
)
SELECT
    d::DATE,
    EXTRACT(YEAR FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH FROM d),
    TO_CHAR(d, 'Month'),
    EXTRACT(WEEK FROM d),
    EXTRACT(DAY FROM d),
    EXTRACT(DOW FROM d),
    TO_CHAR(d, 'Day'),
    EXTRACT(DOW FROM d) IN (0, 6)
FROM generate_series(
    '2024-01-01'::DATE,
    '2026-12-31'::DATE,
    '1 day'::INTERVAL
) AS d
ON CONFLICT (full_date) DO NOTHING;