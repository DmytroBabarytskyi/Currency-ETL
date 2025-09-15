# Currency ETL Pipeline (NBU API)

## Overview
This project implements a mini ETL pipeline that loads exchange rates from the **National Bank of Ukraine (NBU API)**, processes and stores them, loads the data into a PostgreSQL database, and generates reports with basic analytics and simple forecasting.  

### Main steps:
1. **Extract** – fetch raw data from the API and save it as JSON.  
2. **Transform** – clean data, select relevant fields, create derived columns, and save it as Parquet.  
3. **Load** – insert processed data into PostgreSQL.  
4. **Analyze** – run SQL queries for basic analytics and export results to CSV/JSON/TXT.  
### Extra:
5. **Forecast** – simple moving average–based forecast.  
6. **Send to Telegram** – optional: send reports and charts to users via a Telegram bot.  

---

## API Used
- **NBU Exchange Rates API**  
  [https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json](https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json)

---

## Fields Selected & Processed
From the original JSON, the following fields are extracted and processed:  

- `cc` – currency code (e.g., "USD", "EUR")  
- `txt` – currency full name  
- `rate` – official exchange rate  
- `exchangedate` – exchange date (`dd.MM.yyyy` → converted to ISO `YYYY-MM-DD`)  
- `rate_per_100` – derived field (exchange rate per 100 units)  

**Filter applied:** only **USD** and **EUR** are stored in the database.  

---

## How to Run

### 1. Local Run (without Docker)
```bash
# 1. Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run ETL steps one by one
make extract
make transform
make load
make analyze
```

### 2. Run with Docker Compose
```bash
# Start Postgres + PgAdmin
docker-compose up -d

# Start Airflow
docker-compose -f docker-compose.airflow.yml up -d --build
```

- Airflow: [http://localhost:8080](http://localhost:8080)

---

## SQL Queries Implemented
Queries implemented in `analyze.py`:

- Average USD and EUR rates across all available data  
- Latest USD and EUR rates  
- Minimum and maximum rates for the last year  
- Rate changes over the last 30 days  
- Number of available data days  
- Number of unique currencies in the database  

---

##  Reports
Three types of reports are generated:

- **JSON** → `data/reports/report_YYYY-MM-DD.json`  
- **CSV** → `usd_report_*.csv`, `eur_report_*.csv`, `general_report_*.csv`  
- **TXT** → text summary with key metrics (`report_YYYY-MM-DD.txt`)  

---

## Bonus
- **Airflow DAG** (`currency_etl.py`)  
- **Docker Compose** config for Postgres + Airflow  
- **Makefile** for simplified local execution  

---

## Extra

- **Currency Forecasting**  
  Added a short-term (5-day) forecast for USD and EUR using the 7-day **rolling average** method.  
  This approach is simple, interpretable, but provides only a **basic trend estimation** (low accuracy for long-term predictions).

- **Forecast Visualization**  
  For USD and EUR, graphs are generated showing historical data, rolling averages, and forecasted points.  
  Images are saved in `dags/output/forecast_*.png`.  

- **Telegram Bot Integration**  
  When Airflow is running, forecast images and text reports are automatically sent daily at **16:00 UTC (19:00 Kyiv time)** via the Telegram bot **`@CurrencyForecastDBTestBot`**.  

- **Running Without Telegram**  
  If Telegram integration is not required:  
  - simply do not set the `TELEGRAM_BOT_TOKEN` variable (the sending step will be skipped automatically), or  
  - remove/comment out the `send_forecast` task in `currency_etl.py`.  

  In this case, the process ends after data analysis and saves all reports locally.