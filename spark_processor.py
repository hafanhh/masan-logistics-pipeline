# spark_processor.py
# Mô phỏng: Palantir Pipeline Transform — Bronze Layer
# Nhiệm vụ: Join 3 nguồn, xử lý schema, lưu ra Bronze Layer

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
import logging
import os

logger = logging.getLogger(__name__)


# -------------------------------------------------------
# Định nghĩa Schema cứng cho PySpark
# Trong Palantir: đây là "Foundry Schema" — bắt buộc phải
# khai báo trước, không được để Spark tự đoán (inferSchema)
# -------------------------------------------------------
SPARK_SCHEMAS = {
    "orders": StructType([
        StructField("order_id",                       StringType(),    False),
        StructField("customer_id",                    StringType(),    False),
        StructField("order_status",                   StringType(),    True),
        StructField("order_purchase_timestamp",       StringType(),    True),
        StructField("order_approved_at",              StringType(),    True),
        StructField("order_delivered_carrier_date",   StringType(),    True),
        StructField("order_delivered_customer_date",  StringType(),    True),
        StructField("order_estimated_delivery_date",  StringType(),    True),
    ]),
    "customers": StructType([
        StructField("customer_id",               StringType(),    False),
        StructField("customer_unique_id",        StringType(),    True),
        StructField("customer_zip_code_prefix",  IntegerType(),   True),
        StructField("customer_city",             StringType(),    True),
        StructField("customer_state",            StringType(),    True),
    ]),
    "geolocation": StructType([
        StructField("geolocation_zip_code_prefix", IntegerType(),  False),
        StructField("geolocation_lat",             DoubleType(),   True),
        StructField("geolocation_lng",             DoubleType(),   True),
        StructField("geolocation_city",            StringType(),   True),
        StructField("geolocation_state",           StringType(),   True),
    ])
}


def create_spark_session(app_name: str = "Masan_DataIngestion") -> SparkSession:
    """
    Tạo SparkSession — tương đương 'khởi động engine' của Palantir Foundry.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Cho phép ghi đè file output nếu chạy lại
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # Log level thấp để terminal không bị spam
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ SparkSession khởi động: '{app_name}'")
    print(f"  Spark version: {spark.version}")
    return spark


def read_with_spark(
    spark: SparkSession,
    source_name: str,
    file_path: str
) -> "pyspark.sql.DataFrame":
    """
    Đọc CSV bằng PySpark với schema đã định nghĩa sẵn.
    KHÔNG dùng inferSchema=True — đây là nguyên tắc Palantir.
    """
    schema = SPARK_SCHEMAS.get(source_name)
    if schema is None:
        raise ValueError(f"Chưa định nghĩa Spark schema cho: {source_name}")

    print(f"\n--- Đọc '{source_name}' bằng PySpark ---")

    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        # Nếu có dòng lỗi → đưa vào cột _corrupt_record thay vì crash
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(file_path)
    )

    count = df.count()
    print(f"  Rows: {count:,} | Columns: {len(df.columns)}")

    # Kiểm tra dòng bị lỗi (corrupt)
    if "_corrupt_record" in df.columns:
        bad_rows = df.filter(F.col("_corrupt_record").isNotNull()).count()
        if bad_rows > 0:
            print(f"Dòng lỗi (corrupt): {bad_rows:,}")
        df = df.drop("_corrupt_record")

    return df


def deduplicate_geolocation(
    df_geo: "pyspark.sql.DataFrame"
) -> "pyspark.sql.DataFrame":
    """
    Geolocation có 1M+ dòng vì mỗi zip code có nhiều tọa độ.
    Palantir mindset: lấy 1 tọa độ đại diện mỗi zip (trung bình).
    Đây là bước 'Deduplication Transform'.
    """
    print("\n--- Dedup Geolocation (1M → unique zip codes) ---")

    df_deduped = (
        df_geo
        .groupBy("geolocation_zip_code_prefix", "geolocation_state")
        .agg(
            F.avg("geolocation_lat").alias("lat"),
            F.avg("geolocation_lng").alias("lng"),
            # Lấy city phổ biến nhất của mỗi zip
            F.first("geolocation_city").alias("city")
        )
    )

    before = df_geo.count()
    after = df_deduped.count()
    print(f"  Trước dedup : {before:,} dòng")
    print(f"  Sau dedup   : {after:,} dòng (unique zip codes)")
    print(f"  Giảm        : {before - after:,} dòng ({(before-after)/before*100:.1f}%)")

    return df_deduped


def build_bronze_layer(spark: SparkSession, base_dir: str) -> "pyspark.sql.DataFrame":
    """
    Hàm chính: Join 3 nguồn → Bronze Layer.
    Bronze = dữ liệu raw đã join, CHƯA transform business logic.
    """
    data_dir = os.path.join(base_dir, "data_source")
    output_dir = os.path.join(base_dir, "bronze_layer")

    # 1. Đọc 3 nguồn
    df_orders = read_with_spark(
        spark, "orders",
        os.path.join(data_dir, "orders.csv")
    )
    df_customers = read_with_spark(
        spark, "customers",
        os.path.join(data_dir, "customers.csv")
    )
    df_geo_raw = read_with_spark(
        spark, "geolocation",
        os.path.join(data_dir, "geolocation.csv")
    )

    # 2. Xử lý geolocation: dedup 1M → unique zip
    df_geo = deduplicate_geolocation(df_geo_raw)

    # 3. Rename cột zip để join rõ ràng hơn
    df_geo = df_geo.withColumnRenamed(
        "geolocation_zip_code_prefix", "zip_code"
    )
    df_customers = df_customers.withColumnRenamed(
        "customer_zip_code_prefix", "zip_code"
    )

    # 4. Join: orders ← customers (inner: chỉ lấy đơn có khách)
    print("\n--- Join 1: Orders ↔ Customers ---")
    df_joined = df_orders.join(
        df_customers,
        on="customer_id",
        how="inner"
    )
    print(f"  Kết quả: {df_joined.count():,} dòng")

    # 5. Join: + geolocation (left: giữ đơn dù không có tọa độ)
    print("\n--- Join 2: + Geolocation (left join) ---")
    df_bronze = df_joined.join(
        df_geo,
        on="zip_code",
        how="left"
    )
    print(f"  Kết quả: {df_bronze.count():,} dòng")

    # 6. Thêm metadata (Palantir luôn gắn ingestion timestamp)
    df_bronze = df_bronze.withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    ).withColumn(
        "data_source",
        F.lit("OLIST_BRONZE_v1")
    )

    # 7. Kiểm tra null ở join key sau khi join
    print("\n--- Kiểm tra chất lượng Bronze Layer ---")
    null_geo = df_bronze.filter(F.col("lat").isNull()).count()
    print(f"  Đơn hàng thiếu tọa độ (zip không match): {null_geo:,}")
    print(f"  Đơn hàng có tọa độ đầy đủ: {df_bronze.count() - null_geo:,}")

    # 8. Lưu ra Bronze Layer (định dạng Parquet — chuẩn Data Lake)
    output_path = os.path.join(output_dir, "orders_enriched")
    print(f"\n--- Lưu Bronze Layer → {output_path} ---")
    (
        df_bronze
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(output_path)
    )
    print(f"Đã lưu dạng Parquet (Snappy compressed)")

    return df_bronze


if __name__ == "__main__":
    print("="*55)
    print("SPARK PROCESSOR — Masan Data Ingestion")
    print("="*55)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    spark = create_spark_session()

    df_bronze = build_bronze_layer(spark, base_dir)

    print("\n--- Schema của Bronze Layer ---")
    df_bronze.printSchema()

    print("\n--- Mẫu 3 dòng đầu ---")
    df_bronze.select(
        "order_id", "customer_id", "order_status",
        "customer_state", "lat", "lng", "ingestion_timestamp"
    ).show(3, truncate=False)

    spark.stop()
    print("\nSpark đã dừng. Bronze Layer hoàn thành!")