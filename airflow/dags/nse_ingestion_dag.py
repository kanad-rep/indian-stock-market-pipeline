import logging
from datetime import datetime, timedelta
import requests
import zipfile
import os
import time
from google.cloud import storage

# ✅ ADD THESE
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

BUCKET_NAME = "stock-market-lake-kanad-001"


def download_nse_range(**context):

    # -----------------------------
    # Params
    # -----------------------------
    conf = context["dag_run"].conf or {}

    if "start_date" not in conf or "end_date" not in conf:
        raise ValueError("start_date and end_date must be provided")

    start_date = datetime.strptime(conf["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(conf["end_date"], "%Y-%m-%d")

    logger.info(f"Starting backfill from {start_date} to {end_date}")

    # -----------------------------
    # Setup
    # -----------------------------
    base_url = "https://archives.nseindia.com/content/historical/EQUITIES"
    os.makedirs("/tmp/nse", exist_ok=True)

    client = storage.Client(project="terraform-demo-482016")
    bucket = client.bucket(BUCKET_NAME)

    headers = {"User-Agent": "Mozilla/5.0"}

    current = start_date
    success_count = 0
    skip_count = 0
    error_count = 0

    # -----------------------------
    # Loop
    # -----------------------------
    while current <= end_date:

        if current.weekday() >= 5:
            logger.info(f"Skipping weekend: {current.date()}")
            current += timedelta(days=1)
            continue

        date_str = current.strftime("%d%b%Y").upper()
        year = current.strftime("%Y")
        month = current.strftime("%b").upper()

        url = f"{base_url}/{year}/{month}/cm{date_str}bhav.csv.zip"

        logger.info(f"Processing {date_str}")

        try:
            response = requests.get(url, headers=headers, timeout=20)

            if response.status_code != 200:
                logger.warning(f"No data for {date_str} (status {response.status_code})")
                skip_count += 1
                current += timedelta(days=1)
                continue

            zip_path = f"/tmp/nse/{date_str}.zip"

            with open(zip_path, "wb") as f:
                f.write(response.content)

            # Extract
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall("/tmp/nse")

            # Upload
            for file in os.listdir("/tmp/nse"):
                if file.endswith(".csv"):

                    local_file = os.path.join("/tmp/nse", file)

                    blob_path = f"raw/nse/year={year}/month={month}/{file}"
                    blob = bucket.blob(blob_path)

                    blob.upload_from_filename(local_file)

                    logger.info(f"Uploaded → {blob_path}")

                    os.remove(local_file)

            os.remove(zip_path)

            success_count += 1

        except Exception as e:
            logger.error(f"Error on {date_str}: {str(e)}")
            error_count += 1

        time.sleep(0.2)
        current += timedelta(days=1)

    # -----------------------------
    # Summary
    # -----------------------------
    logger.info("----- BACKFILL SUMMARY -----")
    logger.info(f"Successful days: {success_count}")
    logger.info(f"Skipped days: {skip_count}")
    logger.info(f"Errors: {error_count}")
    logger.info("----------------------------")

# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id="nse_backfill_param",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["nse", "backfill"],
) as dag:

    run_backfill = PythonOperator(
        task_id="download_nse_range",
        python_callable=download_nse_range
    )