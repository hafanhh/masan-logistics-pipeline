# File debug_quality.py để điều tra fail: "lat trong khoảng Brazil (-35 đến 6)"
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.appName("debug").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

base_dir = "/home/vmo/masan_palantir"
df = spark.read.parquet(os.path.join(base_dir, "bronze_layer", "orders_enriched"))

print("=== Điều tra lat ngoài biên giới Brazil ===")
bad_lat = df.filter(
    F.col("lat").isNotNull() &
    ((F.col("lat") < -35) | (F.col("lat") > 6))
)
print(f"Số dòng lat ngoài range: {bad_lat.count():,}")
bad_lat.select("customer_state", "lat", "lng", "city").show(20, truncate=False)

spark.stop()