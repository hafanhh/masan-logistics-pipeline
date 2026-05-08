# silver_processor.py
# Mô phỏng: Palantir Pipeline Transform — Silver Layer
# Nhiệm vụ: Đọc Bronze, làm sạch, chuẩn hóa → lưu Silver

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "Masan_SilverLayer") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"SparkSession khởi động: '{app_name}'")
    return spark


# -------------------------------------------------------
# Bước 1: Đọc Bronze Layer
# Palantir mindset: Silver KHÔNG đọc từ CSV gốc.
# Silver chỉ được đọc từ Bronze — đây là "lineage" (dòng dõi data)
# -------------------------------------------------------
def read_bronze(spark: SparkSession, base_dir: str):
    bronze_path = os.path.join(base_dir, "bronze_layer", "orders_enriched")
    print(f"\n--- Đọc Bronze Layer từ: {bronze_path} ---")

    df = spark.read.parquet(bronze_path)
    print(f"  Rows: {df.count():,} | Columns: {len(df.columns)}")
    return df


# -------------------------------------------------------
# Bước 2: Fix kiểu dữ liệu timestamp
# Vấn đề: Bronze lưu tất cả ngày giờ dạng string
# Palantir mindset: Silver phải enforce đúng dtype
# -------------------------------------------------------
def fix_timestamps(df):
    print("\n--- Bước 2: Fix timestamp columns ---")

    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    for col in timestamp_cols:
        df = df.withColumn(
            col,
            F.to_timestamp(F.col(col), "yyyy-MM-dd HH:mm:ss")
        )
        print(f" {col} → timestamp")

    return df


# -------------------------------------------------------
# Bước 3: Xử lý null
# Palantir mindset: Không xóa data — ghi rõ lý do null
# -------------------------------------------------------
def handle_nulls(df):
    print("\n--- Bước 3: Xử lý null values ---")

    before = df.count()

    # 3a. Đơn hàng thiếu tọa độ → fill bằng trung bình của state
    # Đây là kỹ thuật "imputation by group"
    print("  Tính trung bình lat/lng theo customer_state...")

    state_avg = df.groupBy("customer_state").agg(
        F.avg("lat").alias("avg_lat"),
        F.avg("lng").alias("avg_lng")
    )

    df = df.join(state_avg, on="customer_state", how="left")

    # Nếu lat null → dùng avg của state đó
    df = df.withColumn(
        "lat",
        F.when(F.col("lat").isNull(), F.col("avg_lat"))
        .otherwise(F.col("lat"))
    ).withColumn(
        "lng",
        F.when(F.col("lng").isNull(), F.col("avg_lng"))
        .otherwise(F.col("lng"))
    )

    # Thêm cột flag để biết đâu là tọa độ thật, đâu là imputed
    df = df.withColumn(
        "geo_imputed",
        F.when(F.col("avg_lat").isNotNull() & (F.col("lat") == F.col("avg_lat")),
               F.lit(True))
        .otherwise(F.lit(False))
    )

    df = df.drop("avg_lat", "avg_lng")

    null_after = df.filter(F.col("lat").isNull()).count()
    print(f"  Null lat/lng còn lại sau imputation: {null_after}")

    # 3b. order_approved_at null (160 đơn) → giữ nguyên null, không xóa
    # Lý do: đây là data thật (đơn chưa được approve)
    approved_null = df.filter(F.col("order_approved_at").isNull()).count()
    print(f"  order_approved_at null: {approved_null:,} (giữ nguyên — đơn chưa approve)")

    # 3c. Thêm cột ghi rõ lý do null cho delivered date
    df = df.withColumn(
        "delivery_null_reason",
        F.when(F.col("order_delivered_customer_date").isNull() &
               F.col("order_status").isin("delivered"),
               F.lit("DATA_QUALITY_ISSUE"))
        .when(F.col("order_delivered_customer_date").isNull(),
              F.lit("NOT_YET_DELIVERED"))
        .otherwise(F.lit(None))
    )

    after = df.count()
    print(f"  Rows: {before:,} → {after:,} (không xóa dòng nào)")
    
    
    # Xử lý tọa độ bất thường (ngoài biên giới Brazil thực tế)
    # Brazil: lat từ -33.75 đến 5.27, lng từ -73.99 đến -28.85
    print("  Kiểm tra và fix tọa độ ngoài biên giới Brazil...")
    bad_geo = df.filter(
        (F.col("lat") < -34) | (F.col("lat") > 6) |
        (F.col("lng") < -74) | (F.col("lng") > -28)
    ).count()
    print(f"  Tọa độ bất thường phát hiện: {bad_geo} dòng → thay bằng avg của state")

    # Tính lại avg theo state (không bao gồm các tọa độ bất thường)
    state_avg_clean = df.filter(
        (F.col("lat") >= -34) & (F.col("lat") <= 6) &
        (F.col("lng") >= -74) & (F.col("lng") <= -28)
    ).groupBy("customer_state").agg(
        F.avg("lat").alias("clean_avg_lat"),
        F.avg("lng").alias("clean_avg_lng")
    )

    df = df.join(state_avg_clean, on="customer_state", how="left")

    df = df.withColumn(
        "lat",
        F.when(
            (F.col("lat") < -34) | (F.col("lat") > 6) |
            (F.col("lng") < -74) | (F.col("lng") > -28),
            F.col("clean_avg_lat")
        ).otherwise(F.col("lat"))
    ).withColumn(
        "lng",
        F.when(
            (F.col("lat") < -34) | (F.col("lat") > 6) |
            (F.col("lng") < -74) | (F.col("lng") > -28),
            F.col("clean_avg_lng")
        ).otherwise(F.col("lng"))
    )

    df = df.drop("clean_avg_lat", "clean_avg_lng")
    print(f"Đã thay thế {bad_geo} tọa độ bất thường")

    return df


# -------------------------------------------------------
# Bước 4: Tính derived columns
# Palantir mindset: Silver chỉ tính các số đơn giản,
# KHÔNG tính KPI phức tạp — đó là việc của Gold Layer
# -------------------------------------------------------
def add_derived_columns(df):
    print("\n--- Bước 4: Thêm derived columns ---")

    # 4a. Số ngày giao hàng thực tế
    df = df.withColumn(
        "actual_delivery_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull() &
            F.col("order_purchase_timestamp").isNotNull(),
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_purchase_timestamp")
            )
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    print(" actual_delivery_days = ngày giao - ngày mua")

    # 4b. Số ngày trễ so với dự kiến (âm = giao sớm, dương = giao trễ)
    df = df.withColumn(
        "delay_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull() &
            F.col("order_estimated_delivery_date").isNotNull(),
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_estimated_delivery_date")
            )
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    print("delay_days = ngày giao thực tế - ngày giao dự kiến")
    print("    (âm = giao SỚM hơn dự kiến, dương = giao TRỄ)")

    # 4c. Tháng và năm của đơn hàng (dùng cho partition sau này)
    df = df.withColumn(
        "order_year",
        F.year(F.col("order_purchase_timestamp"))
    ).withColumn(
        "order_month",
        F.month(F.col("order_purchase_timestamp"))
    )
    print("  ✓ order_year, order_month → dùng để partition data")

    # 4d. Phân loại đơn hàng theo tốc độ giao
    df = df.withColumn(
        "delivery_speed_category",
        F.when(F.col("actual_delivery_days") <= 7,  F.lit("FAST"))
        .when(F.col("actual_delivery_days") <= 14, F.lit("NORMAL"))
        .when(F.col("actual_delivery_days") <= 30, F.lit("SLOW"))
        .when(F.col("actual_delivery_days") > 30,  F.lit("VERY_SLOW"))
        .otherwise(F.lit("UNKNOWN"))
    )
    print("delivery_speed_category: FAST / NORMAL / SLOW / VERY_SLOW")

    return df


# -------------------------------------------------------
# Bước 5: Validate chất lượng Silver
# Palantir mindset: Luôn có data quality check trước khi lưu
# -------------------------------------------------------
def validate_silver(df):
    print("\n--- Bước 5: Data Quality Check ---")

    total = df.count()

    checks = {
        "Không có order_id null": df.filter(F.col("order_id").isNull()).count() == 0,
        "Không có customer_id null": df.filter(F.col("customer_id").isNull()).count() == 0,
        "delivery_days hợp lệ (0-365)": df.filter(
            F.col("actual_delivery_days").isNotNull() &
            ((F.col("actual_delivery_days") < 0) | (F.col("actual_delivery_days") > 365))
        ).count() == 0,
        "lat trong khoảng Brazil (-34 đến 6)": df.filter(
            F.col("lat").isNotNull() &
            ((F.col("lat") < -34) | (F.col("lat") > 6))
        ).count() == 0,
    }

    all_pass = True
    for check_name, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {status} | {check_name}")
        if not passed:
            all_pass = False

    # Thống kê nhanh
    print(f"\n  Tổng đơn hàng       : {total:,}")

    delivered = df.filter(F.col("order_status") == "delivered").count()
    print(f"  Đơn đã giao         : {delivered:,} ({delivered/total*100:.1f}%)")

    avg_days = df.filter(F.col("actual_delivery_days").isNotNull()) \
                 .agg(F.avg("actual_delivery_days")).collect()[0][0]
    print(f"  Trung bình ngày giao: {avg_days:.1f} ngày")

    late = df.filter(F.col("delay_days") > 0).count()
    print(f"  Đơn giao trễ        : {late:,} ({late/total*100:.1f}%)")

    speed_dist = df.groupBy("delivery_speed_category").count().orderBy("count", ascending=False)
    print("\n  Phân bố tốc độ giao hàng:")
    speed_dist.show(truncate=False)

    return all_pass


# -------------------------------------------------------
# Bước 6: Lưu Silver Layer
# Partitioned by year/month → query theo thời gian nhanh hơn
# -------------------------------------------------------
def save_silver(df, base_dir: str):
    silver_path = os.path.join(base_dir, "silver_layer", "orders_cleaned")
    print(f"\n--- Bước 6: Lưu Silver Layer → {silver_path} ---")

    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("order_year", "order_month")   # ← quan trọng!
        .parquet(silver_path)
    )
    print("Đã lưu dạng Parquet, partition theo year/month")
    print("  Cấu trúc thư mục:")
    print("    silver_layer/orders_cleaned/")
    print("      order_year=2016/order_month=9/...")
    print("      order_year=2017/order_month=1/...")
    print("      order_year=2018/order_month=8/...")


# -------------------------------------------------------
# Main
# -------------------------------------------------------
if __name__ == "__main__":
    print("=" * 55)
    print("SILVER PROCESSOR — Masan Data Pipeline")
    print("=" * 55)

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Tạo thư mục silver nếu chưa có
    os.makedirs(os.path.join(base_dir, "silver_layer"), exist_ok=True)

    spark = create_spark_session()

    # Pipeline
    df = read_bronze(spark, base_dir)
    df = fix_timestamps(df)
    df = handle_nulls(df)
    df = add_derived_columns(df)

    # Validate trước khi lưu
    quality_ok = validate_silver(df)

    if quality_ok:
        save_silver(df, base_dir)
        print("\nSilver Layer hoàn thành!")
    else:
        print("\nData quality check FAILED — không lưu Silver!")
        print("  Kiểm tra lại Bronze Layer trước.")

    spark.stop()
    print("Spark đã dừng.")