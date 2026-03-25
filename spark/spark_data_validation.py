from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim

# ----------------------------
# CONFIG
# ----------------------------
BUCKET = "stock-market-lake-kanad-001"
PROJECT_ID = "terraform-demo-482016"

RAW_PATH = f"gs://{BUCKET}/raw/nse_delivery/year=2021/month=01/data_20210104.csv"
# OUTPUT_PATH = f"gs://{BUCKET}/processed/nse/"

# ----------------------------
# SPARK SESSION
# ----------------------------
spark = SparkSession.builder \
    .appName("NSE Processing Job") \
    .getOrCreate()

# ----------------------------
# READ RAW DATA
# ----------------------------
print("Trying to read from:", RAW_PATH)
df = spark.read.option("header", True).csv(RAW_PATH)

print("Schema:")
df.printSchema()

count = df.count()
print(f"Total rows read: {count}")

print("Raw Data:")
df.show(5)

df = df.withColumn("date", to_date(trim(col("DATE1")), "dd-MMM-yyyy"))

print("Updated Data:")
df.show(5)