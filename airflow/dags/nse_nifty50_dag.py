import os
import time
import calendar
import zipfile
import pandas as pd
import requests
import logging
from io import StringIO
from datetime import datetime, timedelta
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

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
    return f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{ds_str}.csv"

def download_and_filter(**context):
    # Determine the date range
    # If triggered manually via 'Trigger DAG w/ config' or CLI
    conf = context.get("params", {})
    
    # Check if this is a manual backfill or a scheduled daily run
    if context['dag_run'].external_trigger and "start_date" in conf and "end_date" in conf:
        start_date = datetime.strptime(conf["start_date"], "%Y-%m-%d")
        end_date = datetime.strptime(conf["end_date"], "%Y-%m-%d")
        logger.info(f"🚀 Running MANUAL BACKFILL: {start_date.date()} to {end_date.date()}")
    else:
        # Scheduled run: use the 'ds' (logical date of the run)
        execution_date = datetime.strptime(context['ds'], "%Y-%m-%d")
        start_date = execution_date
        end_date = execution_date
        logger.info(f"📅 Running SCHEDULED DAILY for: {execution_date.date()}")

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://www.nseindia.com/"})

    current = start_date
    while current <= end_date:
        # ISO: Mon=1, Tue=2, Wed=3, Thu=4, Fri=5, Sat=6, Sun=7
        if current.isoweekday() in (6, 7):
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
                df = pd.read_csv(StringIO(resp.text))
                
                # Standardize Column Names
                df.columns = [c.strip().upper() for c in df.columns]
                
                # Filter for Nifty 50
                df_filtered = df[df['SYMBOL'].isin(NIFTY_50)]
                
                if not df_filtered.empty:
                    blob_path = f"raw/nse_delivery/year={current.year}/month={current.month:02d}/data_{current.strftime('%Y%m%d')}.csv"
                    bucket.blob(blob_path).upload_from_string(df_filtered.to_csv(index=False), 'text/csv')
                    logger.info(f"✅ Success: {current.date()}")
                else:
                    logger.warning(f"Empty data after filtering for {current.date()}")
            else:
                logger.warning(f"⚠️ {current.date()}: HTTP {resp.status_code} - Likely archive not yet updated.")

        except Exception as e:
            logger.error(f"❌ Error {current.date()}: {e}")

        # Only sleep if we are looping through multiple days
        if start_date != end_date:
            time.sleep(1.5)
            
        current += timedelta(days=1)

with DAG(
    dag_id="nse_security_wise_unified",
    start_date=datetime(2024, 1, 1),
    # Scheduled for 14:30 UTC (8:00 PM IST) to ensure NSE archive is ready
    schedule_interval="30 14 * * *", 
    catchup=False,
    params={
        "start_date": Param(str(datetime.now().date() - timedelta(days=7)), type="string", format="date"),
        "end_date": Param(str(datetime.now().date()), type="string", format="date"),
    },
    tags=["nse", "nifty50", "batch"]
) as dag:

    PythonOperator(
        task_id="ingest_security_data",
        python_callable=download_and_filter,
        provide_context=True
    )