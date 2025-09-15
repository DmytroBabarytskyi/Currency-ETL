import requests
import json
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

RAW_DIR = Path("/opt/airflow/data/raw")

RAW_DIR.mkdir(parents=True, exist_ok=True)

def fetch_nbu():
    # Fetch exchange rates from NBU public API
    url = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

def save_raw(data):
    # Save raw API response to dated folder
    today = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = RAW_DIR / today
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = out_dir / "response.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("+ Saved raw to", fname)


if __name__ == "__main__":
    data = fetch_nbu()
    save_raw(data)