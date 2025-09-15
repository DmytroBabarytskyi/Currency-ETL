from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# Add src/ folder to Python path for imports
sys.path.append(os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "src"))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def forecast():
    # Simple moving-average forecast for USD/EUR
    import pandas as pd
    from sqlalchemy import create_engine
    import matplotlib.pyplot as plt
    from datetime import timedelta

    DB_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres:5432/airflow")
    engine = create_engine(DB_URL)
    df = pd.read_sql("SELECT * FROM exchange_rates", engine)

    out_dir = Path("/opt/airflow/dags/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    for currency in ["USD", "EUR"]:
        df_cur = df[df["cc"] == currency].sort_values("exchangedate").copy()
        if df_cur.empty:
            print(f"# No data for {currency}, skipping forecast")
            continue

        # rolling 7-day average
        df_cur["avg_rate"] = df_cur["rate"].rolling(window=7, min_periods=1).mean()

        last_date = pd.to_datetime(df_cur["exchangedate"].max())
        # fallback to last rate if avg_rate is NaN
        if df_cur["avg_rate"].dropna().shape[0] > 0:
            last_avg = float(df_cur["avg_rate"].dropna().iloc[-1])
        else:
            last_avg = float(df_cur["rate"].iloc[-1])

        # forecast next 5 days (flat line at last_avg)
        future_dates = [last_date + timedelta(days=i) for i in range(1, 6)]
        future_rates = [last_avg for _ in range(5)]

        # plot and save
        plt.figure(figsize=(10,5))
        plt.plot(pd.to_datetime(df_cur["exchangedate"]), df_cur["rate"], label="Rate")
        plt.plot(pd.to_datetime(df_cur["exchangedate"]), df_cur["avg_rate"], label="7-day rolling avg")
        plt.plot(future_dates, future_rates, linestyle="--", label="Forecast")
        plt.title(f"{currency} Exchange Rate")
        plt.xlabel("Date")
        plt.ylabel("Rate")
        plt.legend()
        plt.grid(True)

        out_path = out_dir / f"forecast_{currency}.png"
        plt.savefig(out_path)
        plt.close()
        print(f"Saved forecast for {currency} to {out_path}")

def send_forecast():
    # Send forecast charts and analysis text to Telegram users
    from sqlalchemy import create_engine, text
    import os, requests
    from pathlib import Path
    from datetime import datetime

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        print("# TELEGRAM_BOT_TOKEN not set — skipping send_forecast")
        return

    DB_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres:5432/airflow")
    engine = create_engine(DB_URL)

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT chat_id FROM telegram_users")).fetchall()

    out_dir = Path("/opt/airflow/dags/output")
    reports_dir = Path("/opt/airflow/data/reports")

    # 1) Send forecast images
    url_photo = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    for currency in ["USD", "EUR"]:
        photo_path = out_dir / f"forecast_{currency}.png"
        if not photo_path.exists():
            continue
        for (chat_id,) in rows:
            with open(photo_path, "rb") as photo:
                requests.post(url_photo, data={"chat_id": chat_id}, files={"photo": photo})

    # 2) Send text analysis report
    today = datetime.utcnow().strftime("%Y-%m-%d")
    text_path = reports_dir / f"report_{today}.txt"
    if text_path.exists():
        with open(text_path, "r", encoding="utf-8") as f:
            message = f.read()

        url_msg = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        for (chat_id,) in rows:
            requests.post(url_msg, data={"chat_id": chat_id, "text": message})

# Define Airflow DAG
with DAG(
    "currency_etl",
    default_args=default_args,
    description="ETL for NBU currency with forecast",
    schedule_interval="0 16 * * *",
    start_date=datetime(2025, 9, 9),
    catchup=False,
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="python /opt/airflow/src/extract.py"
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="python /opt/airflow/src/transform.py"
    )

    load = BashOperator(
        task_id="load",
        bash_command="python /opt/airflow/src/load.py"
    )

    analyze = BashOperator(
        task_id="analyze",
        bash_command="python /opt/airflow/src/analyze.py"
    )

    forecast_task = PythonOperator(
        task_id="forecast",
        python_callable=forecast
    )

    send_telegram = PythonOperator(
        task_id="send_forecast",
        python_callable=send_forecast
    )

    # DAG pipeline
    extract >> transform >> load >> analyze >> forecast_task >> send_telegram
