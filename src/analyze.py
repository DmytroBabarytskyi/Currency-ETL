import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, date
from pathlib import Path
from dotenv import load_dotenv
from decimal import Decimal

load_dotenv()

# Connection to Postgres (from .env or fallback)
DB_URL = os.getenv("DATABASE_URL") or "postgresql://airflow:airflow@postgres:5432/airflow"
REPORTS_DIR = Path("/opt/airflow/data/reports")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

engine = create_engine(DB_URL)

# Define analysis queries
queries = {
    # average rate all time
    "avg_usd_rate": "SELECT AVG(rate) AS avg_usd FROM exchange_rates WHERE cc='USD';",
    "avg_eur_rate": "SELECT AVG(rate) AS avg_eur FROM exchange_rates WHERE cc='EUR';",

    # last available rate
    "last_usd_rate": """
        SELECT rate AS last_usd, exchangedate
        FROM exchange_rates
        WHERE cc='USD'
        ORDER BY exchangedate DESC
        LIMIT 1;
    """,
    "last_eur_rate": """
        SELECT rate AS last_eur, exchangedate
        FROM exchange_rates
        WHERE cc='EUR'
        ORDER BY exchangedate DESC
        LIMIT 1;
    """,

    # min / max for last year
    "usd_range_year": """
        SELECT MIN(rate) AS min_usd, MAX(rate) AS max_usd
        FROM exchange_rates
        WHERE cc='USD' AND exchangedate >= NOW() - INTERVAL '365 days';
    """,
    "eur_range_year": """
        SELECT MIN(rate) AS min_eur, MAX(rate) AS max_eur
        FROM exchange_rates
        WHERE cc='EUR' AND exchangedate >= NOW() - INTERVAL '365 days';
    """,

    # number of available days
    "usd_days": """
        SELECT COUNT(*) AS days_usd, MIN(exchangedate) AS first_usd_date, MAX(exchangedate) AS last_usd_date
        FROM exchange_rates WHERE cc='USD';
    """,
    "eur_days": """
        SELECT COUNT(*) AS days_eur, MIN(exchangedate) AS first_eur_date, MAX(exchangedate) AS last_eur_date
        FROM exchange_rates WHERE cc='EUR';
    """,

    # difference between last day and ~30 days ago
    "usd_change_month": """
        WITH days_info AS (
            SELECT COUNT(*) AS cnt FROM exchange_rates WHERE cc='USD'
        ),
        ordered AS (
            SELECT rate,
                   ROW_NUMBER() OVER (ORDER BY exchangedate DESC) AS rn
            FROM exchange_rates
            WHERE cc='USD'
        ),
        last AS (
            SELECT rate FROM ordered WHERE rn = 1
        ),
        ago AS (
            SELECT o.rate
            FROM ordered o, days_info d
            WHERE o.rn = LEAST(d.cnt, 31)  -- 30 днів тому або останній доступний
        )
        SELECT last.rate - ago.rate AS diff_usd
        FROM last, ago;
    """,
    "eur_change_month": """
        WITH days_info AS (
            SELECT COUNT(*) AS cnt FROM exchange_rates WHERE cc='EUR'
        ),
        ordered AS (
            SELECT rate,
                   ROW_NUMBER() OVER (ORDER BY exchangedate DESC) AS rn
            FROM exchange_rates
            WHERE cc='EUR'
        ),
        last AS (
            SELECT rate FROM ordered WHERE rn = 1
        ),
        ago AS (
            SELECT o.rate
            FROM ordered o, days_info d
            WHERE o.rn = LEAST(d.cnt, 31)
        )
        SELECT last.rate - ago.rate AS diff_eur
        FROM last, ago;
    """,

    # count distinct currencies
    "unique_currencies": "SELECT COUNT(DISTINCT cc) AS num_currencies FROM exchange_rates;"
}

def serialize_value(v):
    # Convert DB values into JSON-safe types
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    try:
        json.dumps(v)
        return v
    except Exception:
        return str(v)

# Run queries and collect results
results = {}
with engine.connect() as conn:
    for name, q in queries.items():
        res = conn.execute(text(q)).fetchall()
        rows = []
        for r in res:
            m = dict(r._mapping)
            m_serial = {k: serialize_value(v) for k, v in m.items()}
            rows.append(m_serial)
        results[name] = rows

def safe_get(results, key, field, default=None):
    # Helper to safely extract field from query result
    if results.get(key) and len(results[key]) > 0:
        return results[key][0].get(field, default)
    return default

# Structure data into nested JSON
structured = {
    "usd": {
        "last": safe_get(results, "last_usd_rate", "last_usd"),
        "change_month": safe_get(results, "usd_change_month", "diff_usd", 0.0),
        "range_year": results["usd_range_year"][0] if results["usd_range_year"] else {},
        "avg_all_time": safe_get(results, "avg_usd_rate", "avg_usd"),
        "days": safe_get(results, "usd_days", "days_usd", 0)
    },
    "eur": {
        "last": safe_get(results, "last_eur_rate", "last_eur"),
        "change_month": safe_get(results, "eur_change_month", "diff_eur", 0.0),
        "range_year": results["eur_range_year"][0] if results["eur_range_year"] else {},
        "avg_all_time": safe_get(results, "avg_eur_rate", "avg_eur"),
        "days": safe_get(results, "eur_days", "days_eur", 0)
    },
    "general": {
        "num_currencies": safe_get(results, "unique_currencies", "num_currencies", 0)
    }
}

today = datetime.utcnow().strftime("%Y-%m-%d")

# Save reports in multiple formats
# --- JSON ---
with open(REPORTS_DIR / f"report_{today}.json", "w", encoding="utf-8") as f:
    json.dump(structured, f, ensure_ascii=False, indent=2)

# --- CSV ---
pd.DataFrame([structured["usd"]]).to_csv(REPORTS_DIR / f"usd_report_{today}.csv", index=False)
pd.DataFrame([structured["eur"]]).to_csv(REPORTS_DIR / f"eur_report_{today}.csv", index=False)
pd.DataFrame([structured["general"]]).to_csv(REPORTS_DIR / f"general_report_{today}.csv", index=False)

# Format human-readable text report
def format_change(label, value, days, full_period):
    actual_days = min(days, full_period)
    return f"📈 {label} change in {actual_days} days: {value:+.2f} UAH"

def format_range(label, rng, days, full_period):
    if not rng:
        return f"📊 No data for {label} yet"
    min_v = rng.get("min_" + label.lower())
    max_v = rng.get("max_" + label.lower())
    if days < full_period:
        return f"📊 {label} in {days} days fluctuated from {min_v:.2f} to {max_v:.2f} UAH"
    return f"📊 {label} per year fluctuated from {min_v:.2f} to {max_v:.2f} UAH"

usd = structured["usd"]
eur = structured["eur"]

text_report = [
    f"💵 Current USD rate: {usd['last']:.2f} UAH",
    f"💶 Current EUR rate: {eur['last']:.2f} UAH",
    format_change("USD", usd["change_month"], usd["days"], 30),
    format_change("EUR", eur["change_month"], eur["days"], 30),
    format_range("USD", usd["range_year"], usd["days"], 365),
    format_range("EUR", eur["range_year"], eur["days"], 365),
    f"💱 The database tracks  {structured['general']['num_currencies']} currencies"
]

report_text_path = REPORTS_DIR / f"report_{today}.txt"
with open(report_text_path, "w", encoding="utf-8") as f:
    f.write("\n".join(text_report))

print("+ Text report saved to", report_text_path)
