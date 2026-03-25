from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim

# ----------------------------
# CONFIG
# ----------------------------
BUCKET = "stock-market-lake-kanad-001"
PROJECT_ID = "terraform-demo-482016"

RAW_PATH = f"gs://{BUCKET}/raw/nse_delivery/year=*/month=*/*.csv"
OUTPUT_PATH = f"gs://{BUCKET}/processed/nse/"

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

if count == 0:
    raise Exception("❌ No data read from GCS — check path!")

print("Raw Data:")
df.show(5)

# ----------------------------
# LOAD SECTOR MAPPING
# ----------------------------
sector_df = spark.read.csv("data/sector_mapping.csv", header=True)

# ----------------------------
# JOIN
# ----------------------------
df_joined = df.join(sector_df, on = "SYMBOL", how = "left")

# ----------------------------
# CLEAN
# ----------------------------
df_clean = df_joined.select(
    col("SYMBOL"),
    col("SERIES"),
    col("DATE1"),
    col("PREV_CLOSE").cast("double"),
    col("OPEN_PRICE").cast("double"),
    col("HIGH_PRICE").cast("double"),
    col("LOW_PRICE").cast("double"),
    col("LAST_PRICE").cast("double"),
    col("CLOSE_PRICE").cast("double"),
    col("AVG_PRICE").cast("double"),
    col("TTL_TRD_QNTY").cast("long"),
    col("TURNOVER_LACS").cast("double"),
    col("NO_OF_TRADES").cast("long"),
    col("DELIV_QTY").cast("long"),
    col("DELIV_PER").cast("double"),
    col("SECTOR")
)

# Convert date
df_clean = df_clean.withColumn("date", to_date(trim(col("DATE1")), "dd-MMM-yyyy"))


#See Sector Distribution
print("Sector Distribution:")
df_clean.groupBy("SECTOR").count().show()
# ----------------------------
# WRITE PARQUET
# ----------------------------
df_clean.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet(OUTPUT_PATH)

print("✅ Data written to GCS (Parquet)")

spark.stop()