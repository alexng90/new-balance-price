# JD Sports New Balance Price Tracker — Apache Airflow

Scrapes all New Balance product prices from JD Sports Singapore
every **12 hours** and stores them in SQLite + CSV.

---

## Quick Start (Docker)

```bash
# 1. Clone / place files
cd jdsports_airflow

# 2. Init & start
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up -d

# 3. Open UI → http://localhost:8080  (admin / admin)
# 4. Enable the DAG: jdsports_newbalance_price_tracker
```

---

## Manual Setup (pip)

```bash
pip install apache-airflow==2.9.0 requests beautifulsoup4 lxml

export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
airflow users create -u admin -p admin -f Admin -l User -r Admin -e admin@example.com

# Copy dag
cp dags/jdsports_price_tracker.py $AIRFLOW_HOME/dags/

airflow scheduler &
airflow webserver &
```

---

## Pipeline Tasks

```
fetch_products
     │   Scrapes all search result pages for "new balance"
     │   Parses: name, price (SGD), original price, discount %, URL, image
     ▼
store_to_db
     │   Persists snapshot to SQLite  →  data/jdsports/prices.db
     ▼
  ┌──┴──────────────┐
  ▼                 ▼
export_csv      check_price_drops
  │               │
  CSV export      Logs items with ≥10% price drop vs previous run
  data/jdsports/prices_export.csv
```

**Schedule**: `0 */12 * * *`  → runs at 00:00 UTC and 12:00 UTC daily

---

## Output Files

| File | Description |
|------|-------------|
| `data/jdsports/prices.db` | SQLite — full history |
| `data/jdsports/prices_export.csv` | Latest snapshot CSV |

### Query examples

```sql
-- Latest prices, sorted cheapest first
SELECT name, price_sgd, discount_pct, scraped_at
FROM products
WHERE scraped_at = (SELECT MAX(scraped_at) FROM products)
ORDER BY price_sgd;

-- Price history for a specific product
SELECT scraped_at, price_sgd
FROM products
WHERE name LIKE '%574%'
ORDER BY scraped_at;

-- Biggest discounts right now
SELECT name, price_sgd, orig_price, discount_pct
FROM products
WHERE scraped_at = (SELECT MAX(scraped_at) FROM products)
  AND discount_pct IS NOT NULL
ORDER BY discount_pct DESC
LIMIT 20;
```

---

## Extending

- **Email alerts**: set `email_on_failure: True` in `DEFAULT_ARGS` and configure SMTP in `airflow.cfg`
- **Slack alerts**: add a `SlackWebhookOperator` after `check_price_drops`
- **PostgreSQL**: swap SQLite for Postgres by updating `DB_PATH` and connection string
- **More brands**: change `SEARCH_URL` query param or duplicate the DAG for Adidas, Nike, etc.
