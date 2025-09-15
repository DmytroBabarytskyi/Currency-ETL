import os
from sqlalchemy import create_engine, text
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Prefer DATABASE_URL (for flexibility in deployment)
DB_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
)

PROCESSED_DIR = Path("/opt/airflow/data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def latest_processed_file():
    # Get latest processed parquet file
    files = list(PROCESSED_DIR.glob("*/data.parquet"))
    if not files:
        return None
    files.sort(key=os.path.getmtime)
    return files[-1]


def load_to_db(df, engine):
    # Ensure exchangedate is datetime
    df['exchangedate'] = pd.to_datetime(df['exchangedate'])

    # Upsert rows into exchange_rates table
    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = text("""
            INSERT INTO exchange_rates (cc, txt, rate, rate_per_100, exchangedate)
            VALUES (:cc, :txt, :rate, :rate_per_100, :exchangedate)
            ON CONFLICT (cc, exchangedate) DO UPDATE
              SET rate = EXCLUDED.rate,
                  rate_per_100 = EXCLUDED.rate_per_100,
                  txt = EXCLUDED.txt;
            """)
            conn.execute(stmt, {
                "cc": row['cc'],
                "txt": row['txt'],
                "rate": float(row['rate']),
                "rate_per_100": float(row['rate_per_100']),
                # формат YYYY-MM-DD (Postgres DATE)
                "exchangedate": row['exchangedate'].strftime("%Y-%m-%d")
            })
    print("+ Loaded to DB")


if __name__ == "__main__":
    latest = latest_processed_file()
    if not latest:
        raise SystemExit("# No processed file found. Run transform first.")
    df = pd.read_parquet(latest)
    if not DB_URL:
        raise SystemExit("# Set DATABASE_URL in .env")

    engine = create_engine(DB_URL)
    # Create tables if not exists
    sql = Path("/opt/airflow/sql/create_tables.sql").read_text()
    with engine.begin() as conn:
        conn.execute(text(sql))

    load_to_db(df, engine)
