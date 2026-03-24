import os
import time
import calendar
import zipfile
import pandas as pd
import requests
import logging
from datetime import datetime, timedelta
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

BUCKET_NAME = "stock-market-lake-kanad-001"
PROJECT_ID = "terraform-demo-482016"

# Current Nifty 50 constituents for filtering
NIFTY_50 =[
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", 
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BEL", "BPCL", 
    "BHARTIARTL", "BRITANNIA", "CIPLA", "COALINDIA", "DRREDDY", 
    "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", 
    "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", 
    "INDUSINDBK", "INFY", "JSWSTEEL", "KOTAKBANK", "LT", 
    "M&M", "MARUTI", "NTPC", "NESTLEIND", "ONGC", 
    "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SUNPHARMA", 
    "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM", 
    "TITAN", "ULTRACEMCO", "TRENT", "WIPRO", "ZOMATO"
]

def get_report_url(date_obj):
    """Targets the Security-wise Price Volume report archive."""
    ds_str = date_obj.strftime("%d%m%Y") # DDMMYYYY
    # The URL pattern for the 'Security-wise Price Volume' archive files
    # Note: These are usually released in the evening after market close.
    return f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{ds_str}.csv"

def download_and_filter(**context):
    conf = context["params"]
    start_date = datetime.strptime(conf["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(conf["end_date"], "%Y-%m-%d")

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://www.nseindia.com/"})

    current = start_date
    while current <= end_date:
        if current.isoweekday() in (6, 7):  # Saturday=6, Sunday=7
            logger.info(f"⏭️ Skipping {current.date()} (Weekend)")
            current += timedelta(days=1)
            continue

        url = get_report_url(current)
        try:
            # 1. Warm up session
            session.get("https://www.nseindia.com", timeout=10)
            
            # 2. Try fetching the full CSV report
            resp = session.get(url, timeout=15)
            
            if resp.status_code == 200:
                # Load the full market report (approx 2MB)
                from io import StringIO
                df = pd.read_csv(StringIO(resp.text))
                
                # Standardize Column Names (NSE often adds leading/trailing spaces)
                df.columns = [c.strip().upper() for c in df.columns]
                
                # Filter for Nifty 50 and Equity series
                df_filtered = df[df['SYMBOL'].isin(NIFTY_50)]
                
                if not df_filtered.empty:
                    blob_path = f"raw/nse_delivery/year={current.year}/month={current.month:02d}/data_{current.strftime('%Y%m%d')}.csv"
                    bucket.blob(blob_path).upload_from_string(df_filtered.to_csv(index=False), 'text/csv')
                    logger.info(f"✅ Success: {current.date()}")
            else:
                logger.warning(f"⚠️ {current.date()}: HTTP {resp.status_code} - File might not be archived yet.")

        except Exception as e:
            logger.error(f"❌ Error {current.date()}: {e}")

        time.sleep(1.5)
        current += timedelta(days=1)

with DAG(
    dag_id="nse_security_wise_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    PythonOperator(task_id="ingest", python_callable=download_and_filter)