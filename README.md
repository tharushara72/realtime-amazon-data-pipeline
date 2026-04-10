
# E-Commerce Data Engineering Pipeline 🚀

A production-grade end-to-end data engineering project simulating an Amazon-style order processing system.

## Tech Stack
| Tool | Purpose |
|------|---------|
| Apache Kafka | Real-time event streaming |
| Apache Airflow | Pipeline orchestration |
| PostgreSQL | Star schema data warehouse |
| Docker | Containerization |
| Power BI | Analytics dashboard |
| Python + uv | Data generation & processing |

## Architecture
**Orders API → Kafka Topics → Spark Consumer → PostgreSQL → Airflow DAG → Power BI**

## Data Model (Star Schema)
- `fact_orders` — core order events
- `dim_customers` — customer profiles
- `dim_products` — product catalogue
- `dim_locations` — delivery locations
- `dim_time` — time dimension
- `agg_daily_sales` — pre-aggregated for Power BI

## How to Run
```bash
# Start all infrastructure
docker-compose up -d

# Generate streaming data
uv run python data_generator/generate_orders.py

# Consume and load to PostgreSQL
uv run python kafka/consumer.py
```

## Airflow DAG
Runs nightly at midnight with 4 tasks:
1. `check_data_quality` — validates data before processing
2. `aggregate_daily_sales` — computes daily KPIs
3. `compute_revenue_summary` — category breakdown
4. `log_pipeline_health` — monitors pipeline status

## Power BI Dashboard
3 report pages showing:
- Revenue and profit by country, brand, category
- Order status breakdown
- Customer segmentation by membership tier
- Regional performance analysis
EOF

# Make sure .gitignore is correct
cat > .gitignore << 'EOF'
.env
.venv/
__pycache__/
*.pyc
*.pyo
airflow/logs/
power_bi_exports/
.DS_Store
uv.lock
EOF

# Stage everything
git add .
git status
