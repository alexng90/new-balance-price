"""
JD Sports Singapore - New Balance Price Tracker DAG
Scrapes product prices every 12 hours and stores results in CSV/SQLite.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://www.jdsports.com.sg"
SEARCH_URL = f"{BASE_URL}/search?q=new+balance"
DATA_DIR = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "data" / "jdsports"
DB_PATH = DATA_DIR / "prices.db"
CSV_PATH = DATA_DIR / "prices_export.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-SG,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": BASE_URL,
}

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,   # set True + configure SMTP to enable alerts
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

log = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_db() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at  TEXT    NOT NULL,
            product_id  TEXT,
            name        TEXT    NOT NULL,
            brand       TEXT,
            price_sgd   REAL,
            orig_price  REAL,
            discount_pct REAL,
            url         TEXT,
            image_url   TEXT,
            in_stock    INTEGER DEFAULT 1
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scrape_runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at  TEXT NOT NULL,
            finished_at TEXT,
            total_pages INTEGER,
            total_items INTEGER,
            status      TEXT DEFAULT 'running'
        )
    """)
    conn.commit()
    return conn


# ── Task 1: Fetch product listings ────────────────────────────────────────────

def fetch_products(**context) -> None:
    """
    Scrape all pages of the New Balance search results on JD Sports SG.
    Pushes raw product list to XCom for downstream tasks.
    """
    ensure_dirs()
    session = requests.Session()
    session.headers.update(HEADERS)

    conn = get_db()
    run_id = conn.execute(
        "INSERT INTO scrape_runs (started_at) VALUES (?)",
        (datetime.utcnow().isoformat(),)
    ).lastrowid
    conn.commit()

    all_products: list[dict] = []
    page = 1
    max_pages = 50  # safety cap

    while page <= max_pages:
        url = f"{SEARCH_URL}&start={(page - 1) * 36}&sz=36"
        log.info("Fetching page %d → %s", page, url)

        try:
            resp = session.get(url, timeout=20)
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.warning("Page %d failed: %s", page, exc)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        products = _parse_product_cards(soup)

        if not products:
            log.info("No products on page %d — stopping.", page)
            break

        all_products.extend(products)
        log.info("Page %d: %d products (total so far: %d)", page, len(products), len(all_products))

        # check if there's a next page
        next_btn = soup.select_one("li.page-next:not(.disabled) a")
        if not next_btn:
            break

        page += 1
        time.sleep(1.5)  # polite crawl delay

    # Update run record
    conn.execute(
        "UPDATE scrape_runs SET total_pages=?, total_items=? WHERE id=?",
        (page, len(all_products), run_id)
    )
    conn.commit()
    conn.close()

    log.info("Scraped %d products across %d pages.", len(all_products), page)

    # Push to XCom
    context["ti"].xcom_push(key="products", value=all_products)
    context["ti"].xcom_push(key="run_id", value=run_id)


def _parse_product_cards(soup: BeautifulSoup) -> list[dict]:
    """Extract product data from a search results page."""
    products = []

    # JD Sports uses data-gtm-* attributes on product tiles
    for card in soup.select(".product-tile, [data-pid], article.product"):
        try:
            name_el = card.select_one(".product-name, .pdp-link a, h3.tile-body__title")
            price_el = card.select_one(".price .value, .sales .value, [data-price]")
            orig_el  = card.select_one(".price del .value, .strike-through .value")
            link_el  = card.select_one("a.thumb-link, a.product-tile__image-link, a[href*='/product/']")
            img_el   = card.select_one("img.product-tile__primary-image, img[data-src]")
            pid      = card.get("data-pid", card.get("data-product-id", ""))

            name = (name_el.get_text(strip=True) if name_el else "").strip()
            if not name:
                continue

            price_raw = price_el.get("content") or price_el.get_text(strip=True) if price_el else ""
            price_sgd = _parse_price(price_raw)

            orig_raw = orig_el.get_text(strip=True) if orig_el else ""
            orig_price = _parse_price(orig_raw)

            discount_pct = None
            if orig_price and price_sgd and orig_price > price_sgd:
                discount_pct = round((orig_price - price_sgd) / orig_price * 100, 1)

            href = link_el.get("href", "") if link_el else ""
            url = f"{BASE_URL}{href}" if href.startswith("/") else href

            img_src = ""
            if img_el:
                img_src = img_el.get("data-src") or img_el.get("src", "")

            products.append({
                "product_id":   pid,
                "name":         name,
                "brand":        "New Balance",
                "price_sgd":    price_sgd,
                "orig_price":   orig_price,
                "discount_pct": discount_pct,
                "url":          url,
                "image_url":    img_src,
                "in_stock":     1,
            })
        except Exception as exc:
            log.debug("Card parse error: %s", exc)
            continue

    return products


def _parse_price(raw: str) -> float | None:
    """Convert '$123.90' → 123.90"""
    if not raw:
        return None
    cleaned = "".join(c for c in raw if c.isdigit() or c == ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


# ── Task 2: Store to database ─────────────────────────────────────────────────

def store_to_db(**context) -> None:
    """Persist scraped products into SQLite."""
    ti = context["ti"]
    products: list[dict] = ti.xcom_pull(key="products", task_ids="fetch_products")
    run_id: int          = ti.xcom_pull(key="run_id",   task_ids="fetch_products")

    if not products:
        log.warning("No products received — skipping DB write.")
        return

    scraped_at = datetime.utcnow().isoformat()
    conn = get_db()

    rows = [
        (
            scraped_at,
            p["product_id"],
            p["name"],
            p["brand"],
            p["price_sgd"],
            p["orig_price"],
            p["discount_pct"],
            p["url"],
            p["image_url"],
            p["in_stock"],
        )
        for p in products
    ]

    conn.executemany(
        """INSERT INTO products
           (scraped_at, product_id, name, brand, price_sgd, orig_price,
            discount_pct, url, image_url, in_stock)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.execute(
        "UPDATE scrape_runs SET finished_at=?, status='success' WHERE id=?",
        (datetime.utcnow().isoformat(), run_id),
    )
    conn.commit()
    conn.close()
    log.info("Stored %d products (run_id=%s).", len(products), run_id)


# ── Task 3: Export to CSV ─────────────────────────────────────────────────────

def export_csv(**context) -> None:
    """Export the latest scrape snapshot to a CSV file."""
    conn = get_db()
    scraped_at = conn.execute(
        "SELECT MAX(scraped_at) FROM products"
    ).fetchone()[0]

    if not scraped_at:
        log.warning("No data to export.")
        conn.close()
        return

    rows = conn.execute(
        "SELECT * FROM products WHERE scraped_at = ? ORDER BY price_sgd ASC",
        (scraped_at,)
    ).fetchall()
    conn.close()

    ensure_dirs()
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id", "scraped_at", "product_id", "name", "brand",
            "price_sgd", "orig_price", "discount_pct",
            "url", "image_url", "in_stock"
        ])
        writer.writerows(rows)

    log.info("Exported %d rows → %s", len(rows), CSV_PATH)


# ── Task 4: Price alert check ─────────────────────────────────────────────────

def check_price_drops(**context) -> None:
    """
    Compare latest vs previous scrape; log items with ≥10% price drop.
    Extend with EmailOperator / Slack webhook for real alerts.
    """
    conn = get_db()
    snapshots = conn.execute(
        "SELECT DISTINCT scraped_at FROM products ORDER BY scraped_at DESC LIMIT 2"
    ).fetchall()

    if len(snapshots) < 2:
        log.info("Need at least 2 snapshots for comparison — skipping.")
        conn.close()
        return

    latest, prev = snapshots[0][0], snapshots[1][0]

    query = """
        SELECT
            a.name,
            b.price_sgd AS old_price,
            a.price_sgd AS new_price,
            ROUND((b.price_sgd - a.price_sgd) / b.price_sgd * 100, 1) AS drop_pct,
            a.url
        FROM products a
        JOIN products b ON a.product_id = b.product_id
        WHERE a.scraped_at = ?
          AND b.scraped_at = ?
          AND a.price_sgd < b.price_sgd
          AND (b.price_sgd - a.price_sgd) / b.price_sgd >= 0.10
        ORDER BY drop_pct DESC
    """
    drops = conn.execute(query, (latest, prev)).fetchall()
    conn.close()

    if drops:
        log.info("🔥 %d price drops detected:", len(drops))
        for d in drops:
            log.info("  %-60s SGD %.2f → %.2f  (-%.1f%%)", d["name"], d["old_price"], d["new_price"], d["drop_pct"])
    else:
        log.info("No significant price drops since last run.")

    # Push summary to XCom for downstream email / Slack tasks
    context["ti"].xcom_push(key="price_drops", value=[dict(d) for d in drops])


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="jdsports_newbalance_price_tracker",
    description="Track New Balance prices on JD Sports Singapore every 12 hours",
    schedule_interval="0 */12 * * *",   # 00:00 and 12:00 UTC daily
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["e-commerce", "pricing", "jdsports", "new-balance"],
    doc_md=__doc__,
) as dag:

    t_fetch = PythonOperator(
        task_id="fetch_products",
        python_callable=fetch_products,
        doc_md="Scrape all New Balance product pages from JD Sports SG.",
    )

    t_store = PythonOperator(
        task_id="store_to_db",
        python_callable=store_to_db,
        doc_md="Persist scraped products to SQLite database.",
    )

    t_csv = PythonOperator(
        task_id="export_csv",
        python_callable=export_csv,
        doc_md="Export latest snapshot to CSV for downstream use.",
    )

    t_alerts = PythonOperator(
        task_id="check_price_drops",
        python_callable=check_price_drops,
        doc_md="Detect ≥10% price drops vs previous run and log/alert.",
    )

    # Pipeline: fetch → store → [csv, alerts] in parallel
    t_fetch >> t_store >> [t_csv, t_alerts]
