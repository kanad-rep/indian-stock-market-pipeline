from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NSE Test Job") \
    .getOrCreate()

df = spark.read.csv("data/sector_mapping.csv", header=True)

df.show(10)

spark.stop()