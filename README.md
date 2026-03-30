# 📊 Indian Stock Market Data Pipeline
### 🧭 Introduction

The `Indian Stock Market Data Pipeline` is an end-to-end data engineering project designed to ingest, process, and store stock market data from the Indian equity markets (primarily NSE). The project demonstrates how modern data engineering tools can be combined to build a scalable, cloud-native pipeline.

The pipeline automates the process of:

* Downloading daily stock market datasets (bhavcopy)
* Storing raw data in cloud storage
* Transforming and loading data into a data warehouse
* Enabling analytics and querying for downstream use cases

This project is built as a practical implementation of real-world data engineering concepts including orchestration, cloud storage, and data warehousing.

### ❗ Problem Statement

Stock market data is:

* Highly dynamic (updated daily)
* Large in volume
* Unstructured or semi-structured when downloaded
* Difficult to manage manually over time

For analysts, traders, and data scientists, there is a need for:

1. Automated data ingestion
    * Manual downloads are inefficient and error-prone
2. Reliable storage system
    * Historical data must be preserved and easily accessible
3. Efficient querying
    * Raw CSV files are not suitable for analytics at scale
4. Scalable architecture
    * The system should handle growing data without redesign

#### Objective

To build a fully automated, scalable data pipeline that:

* Extracts stock market data daily
* Stores it reliably in cloud storage
* Transforms it into structured formats
* Loads it into a data warehouse for analysis
### 📦 Dataset

#### 📍 Source

The dataset used in this project is the **NSE Bhavcopy dataset**, which contains daily trading information for all listed securities. We are using the data for Top 50 stocks on NSE, in the last 5 years for our pipeline.

Typical source:

* [National Stock Exchange of India](https://www.nseindia.com/) official website
* Example format (bhavcopy ZIP/CSV files)

### 📊 Dataset Description

Each file represents one trading day and contains records for all traded stocks.

| Field             | Definition                                             |
| ----------------- | ------------------------------------------------------ |
| **SYMBOL**        | Stock ticker symbol of the company                     |
| **SERIES**        | Security type (e.g., EQ = Equity, BE = Trade-to-trade) |
| **DATE1**         | Trading date of the record                             |
| **PREV_CLOSE**    | Closing price from the previous trading day            |
| **OPEN_PRICE**    | Price at which the stock opened for the day            |
| **HIGH_PRICE**    | Highest price reached during the trading session       |
| **LOW_PRICE**     | Lowest price reached during the trading session        |
| **LAST_PRICE**    | Last traded price before market close                  |
| **CLOSE_PRICE**   | Official closing price of the stock                    |
| **AVG_PRICE**     | Volume-weighted average price (VWAP) of the day        |
| **TTL_TRD_QNTY**  | Total quantity of shares traded during the day         |
| **TURNOVER_LACS** | Total traded value in lakhs (1 lakh = 100,000)         |
| **NO_OF_TRADES**  | Total number of executed trades                        |
| **DELIV_QTY**     | Quantity of shares delivered (not intraday)            |
| **DELIV_PER**     | Percentage of traded quantity that was delivered       |


## Tech Stack
- GCP (GCS, BigQuery)
- Terraform
- Airflow
- Spark
- dbt
- Streamlit

## Project Structure
(To be added)

## Usability
### Step 1: Environment & Cloud Setup
Before running any code, you need a Google Cloud Platform (GCP) project and the necessary credentials.
1. **Create a GCP Project**: Go to the GCP Console and create a project (e.g., nse-pipeline-2026).
2. **Enable APIs**: Enable the BigQuery and Cloud Storage APIs.
3. **Create a Service Account**:
    - Go to **IAM & Admin > Service Accounts**.
    - Create an account named `dbt-runner`.
    - Grant these roles: `BigQuery Admin`, `Storage Object Admin`, `Storage Insights Viewer`.
    - **Create a JSON Key**: Actions > Manage Keys > Add Key > Create New Key (JSON). Save this as `creds.json`.

4. Install Tools: Ensure you have the Google Cloud SDK, Terraform, and Python 3.9+ installed.
    #### 1. Google Cloud SDK (gcloud CLI)
    The Google Cloud SDK is required to authenticate with GCP and manage cloud resources from the terminal.
    ```bash
    # Update package list and install dependencies
    sudo apt-get update
    sudo apt-get install apt-transport-https ca-certificates gnupg curl -y

    # Import the Google Cloud public key
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg

    # Add the gcloud SDK repo to your system
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

    # Install the SDK
    sudo apt-get update && sudo apt-get install google-cloud-cli -y

    # Verify installation
    gcloud --version
    ```