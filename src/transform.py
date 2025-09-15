import pandas as pd
from pathlib import Path
from datetime import datetime
import glob
import json

RAW_DIR = Path("/opt/airflow/data/raw")
PROCESSED_DIR = Path("/opt/airflow/data/processed")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def latest_raw_file():
    # Get latest raw file (sorted by folder name)
    files = sorted(glob.glob(str(RAW_DIR / "*/response.json")))
    return files[-1] if files else None

def transform(raw_path):
    # Load raw JSON and convert to DataFrame
    with open(raw_path, encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    # Keep only useful columns
    df = df[['cc', 'rate', 'txt', 'exchangedate']]
    # Convert date from dd.MM.yyyy -> datetime
    df['exchangedate'] = pd.to_datetime(df['exchangedate'], format='%d.%m.%Y')
    # Add derived column
    df['rate_per_100'] = df['rate'] * 100
    # Keep only USD and EUR
    df = df[df['cc'].isin(['USD', 'EUR'])]
    return df

if __name__ == "__main__":
    raw_f = latest_raw_file()
    if not raw_f:
        raise SystemExit("No raw file found. Run extract first.")

    df = transform(raw_f)

    today = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = PROCESSED_DIR / today
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "data.parquet", index=False)
    print("+ Processed data saved to", out_dir / "data.parquet")
