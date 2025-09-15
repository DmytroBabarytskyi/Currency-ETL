import os
from pathlib import Path
import glob
import json
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL") or "postgresql://airflow:airflow@postgres:5432/airflow"

RAW_DIR = Path("/opt/airflow/data/raw")
CLEAN_DIR = Path("/opt/airflow/data/processed")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)


def transform_file(raw_path):
    """Прочитати JSON, трансформувати у DataFrame і повернути"""
    with open(raw_path, encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    df = df[['cc', 'rate', 'txt', 'exchangedate']]
    df['exchangedate'] = pd.to_datetime(df['exchangedate'], format='%d.%m.%Y')
    df['rate_per_100'] = df['rate'] * 100
    df = df[df['cc'].isin(['USD', 'EUR'])]
    return df


def load_to_db(df, engine):
    """Завантажити DataFrame у DB з ON CONFLICT DO UPDATE"""
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
                "exchangedate": row['exchangedate'].date() if hasattr(row['exchangedate'], 'date') else row[
                    'exchangedate']
            })


def process_all_files():
    engine = create_engine(DB_URL)

    # Переконатися, що таблиці створені
    sql_path = Path("/opt/airflow/sql/create_tables.sql")
    if sql_path.exists():
        with engine.begin() as conn:
            conn.execute(text(sql_path.read_text()))

    files = sorted(glob.glob(str(RAW_DIR / "exchange_*.json")))
    if not files:
        print("❌ Raw files not found!")
        return

    for raw_f in files:
        df = transform_file(raw_f)

        # Зберегти processed CSV для контролю
        out_fname = CLEAN_DIR / f"clean_{Path(raw_f).stem}.csv"
        df.to_csv(out_fname, index=False)
        print("Clean saved:", out_fname)

        # Завантажити в БД
        load_to_db(df, engine)
        print("Loaded to DB:", raw_f)


if __name__ == "__main__":
    process_all_files()
