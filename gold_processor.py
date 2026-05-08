# gold_processor.py
# Mô phỏng: Palantir Pipeline Transform — Gold Layer
# Nhiệm vụ: Đọc Silver → tính KPI → lưu Gold (analyst-ready)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Masan_GoldLayer")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession khởi động: 'Masan_GoldLayer'")
    return spark


def read_silver(spark, base_dir):
    silver_path = os.path.join(base_dir, "silver_layer", "orders_cleaned")
    print(f"\n--- Đọc Silver Layer ---")
    df = spark.read.parquet(silver_path)
    print(f"  Rows: {df.count():,} | Columns: {len(df.columns)}")
    return df


# -------------------------------------------------------
# KPI 1: Hiệu suất giao hàng theo State
# Câu hỏi business: "State nào giao hàng tệ nhất?"
# Masan dùng để: đàm phán lại với logistics partner theo vùng
# -------------------------------------------------------
def kpi_delivery_by_state(df, gold_dir):
    print("\n--- KPI 1: Hiệu suất giao hàng theo State ---")

    window_rank = Window.orderBy(F.col("avg_delivery_days").desc())

    kpi = (
        df.filter(F.col("order_status") == "delivered")
        .groupBy("customer_state")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.avg("actual_delivery_days"), 1).alias("avg_delivery_days"),
            F.round(F.avg("delay_days"), 1).alias("avg_delay_days"),
            F.sum(
                F.when(F.col("delay_days") > 0, 1).otherwise(0)
            ).alias("late_orders"),
            F.round(
                F.sum(F.when(F.col("delay_days") > 0, 1).otherwise(0)) /
                F.count("order_id") * 100, 1
            ).alias("late_rate_pct"),
            F.round(F.percentile_approx("actual_delivery_days", 0.5), 1)
             .alias("median_delivery_days")
        )
        .withColumn("rank_slowest", F.rank().over(window_rank))
    )

    print("  Top 5 state giao hàng CHẬM nhất:")
    kpi.orderBy("avg_delivery_days", ascending=False).show(5, truncate=False)

    print("  Top 5 state giao hàng NHANH nhất:")
    kpi.orderBy("avg_delivery_days", ascending=True).show(5, truncate=False)

    # Lưu Gold
    out = os.path.join(gold_dir, "kpi_delivery_by_state")
    kpi.write.mode("overwrite").option("compression", "snappy").parquet(out)
    kpi.toPandas().to_csv(out + ".csv", index=False)
    print(f" Đã lưu: kpi_delivery_by_state.csv")
    return kpi


# -------------------------------------------------------
# KPI 2: Trend giao hàng theo tháng
# Câu hỏi business: "Tháng nào logistics bị quá tải?"
# Masan dùng để: lên kế hoạch tồn kho và nhân lực theo mùa
# -------------------------------------------------------
def kpi_monthly_trend(df, gold_dir):
    print("\n--- KPI 2: Trend đơn hàng theo tháng ---")

    kpi = (
        df.groupBy("order_year", "order_month")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.avg("actual_delivery_days"), 1).alias("avg_delivery_days"),
            F.round(
                F.sum(F.when(F.col("delay_days") > 0, 1).otherwise(0)) /
                F.count("order_id") * 100, 1
            ).alias("late_rate_pct"),
            F.sum(
                F.when(F.col("delivery_speed_category") == "FAST", 1).otherwise(0)
            ).alias("fast_orders"),
            F.sum(
                F.when(F.col("delivery_speed_category") == "VERY_SLOW", 1).otherwise(0)
            ).alias("very_slow_orders")
        )
        .orderBy("order_year", "order_month")
    )

    print("  Trend theo tháng (2017-2018):")
    kpi.filter(F.col("order_year") >= 2017).show(24, truncate=False)

    out = os.path.join(gold_dir, "kpi_monthly_trend")
    kpi.write.mode("overwrite").option("compression", "snappy").parquet(out)
    kpi.toPandas().to_csv(out + ".csv", index=False)
    print(f"Đã lưu: kpi_monthly_trend.csv")
    return kpi


# -------------------------------------------------------
# KPI 3: Phân tích đơn giao trễ
# Câu hỏi business: "Đơn trễ tập trung ở đâu, bao lâu?"
# Masan dùng để: xác định root cause, cải thiện SLA
# -------------------------------------------------------
def kpi_late_orders_analysis(df, gold_dir):
    print("\n--- KPI 3: Phân tích đơn giao trễ ---")

    df_late = df.filter(
        (F.col("delay_days") > 0) &
        (F.col("order_status") == "delivered")
    )

    # Phân nhóm mức độ trễ
    df_late = df_late.withColumn(
        "delay_severity",
        F.when(F.col("delay_days") <= 3,  F.lit("MINOR"))     # trễ 1-3 ngày
        .when(F.col("delay_days") <= 7,   F.lit("MODERATE"))  # trễ 4-7 ngày
        .when(F.col("delay_days") <= 14,  F.lit("SEVERE"))    # trễ 1-2 tuần
        .otherwise(F.lit("CRITICAL"))                          # trễ hơn 2 tuần
    )

    # KPI theo severity
    kpi_severity = (
        df_late.groupBy("delay_severity")
        .agg(
            F.count("order_id").alias("num_orders"),
            F.round(F.avg("delay_days"), 1).alias("avg_delay_days"),
            F.max("delay_days").alias("max_delay_days")
        )
        .orderBy("avg_delay_days")
    )

    print(" Phân loại mức độ trễ:")
    kpi_severity.show(truncate=False)

    # KPI trễ theo state
    kpi_late_state = (
        df_late.groupBy("customer_state", "delay_severity")
        .agg(F.count("order_id").alias("late_orders"))
        .orderBy("late_orders", ascending=False)
    )

    print("Top 10 state + severity có nhiều đơn trễ nhất:")
    kpi_late_state.show(10, truncate=False)

    out = os.path.join(gold_dir, "kpi_late_orders")
    kpi_severity.write.mode("overwrite").option("compression", "snappy").parquet(out)
    kpi_severity.toPandas().to_csv(out + ".csv", index=False)
    print(f" Đã lưu: kpi_late_orders.csv")
    return kpi_severity


# -------------------------------------------------------
# KPI 4: Executive Summary — 1 bảng tổng hợp toàn bộ
# Đây là bảng analyst mở đầu tiên khi cần báo cáo nhanh
# -------------------------------------------------------
def kpi_executive_summary(df, kpi_state, kpi_trend, gold_dir):
    print("\n--- KPI 4: Executive Summary ---")

    total_orders    = df.count()
    delivered       = df.filter(F.col("order_status") == "delivered").count()
    avg_days        = df.filter(F.col("actual_delivery_days").isNotNull()) \
                        .agg(F.avg("actual_delivery_days")).collect()[0][0]
    late_orders     = df.filter(F.col("delay_days") > 0).count()
    fast_orders     = df.filter(F.col("delivery_speed_category") == "FAST").count()
    worst_state     = kpi_state.orderBy("avg_delivery_days", ascending=False) \
                               .first()["customer_state"]
    best_state      = kpi_state.orderBy("avg_delivery_days", ascending=True) \
                               .first()["customer_state"]
    peak_month_row  = kpi_trend.orderBy("total_orders", ascending=False).first()

    summary = {
        "metric": [
            "Tổng đơn hàng",
            "Đơn đã giao thành công",
            "Tỷ lệ giao thành công",
            "Trung bình ngày giao",
            "Đơn giao trễ",
            "Tỷ lệ giao trễ",
            "Đơn giao nhanh (≤7 ngày)",
            "State giao chậm nhất",
            "State giao nhanh nhất",
            "Tháng cao điểm đơn hàng",
        ],
        "value": [
            f"{total_orders:,}",
            f"{delivered:,}",
            f"{delivered/total_orders*100:.1f}%",
            f"{avg_days:.1f} ngày",
            f"{late_orders:,}",
            f"{late_orders/total_orders*100:.1f}%",
            f"{fast_orders:,} ({fast_orders/total_orders*100:.1f}%)",
            worst_state,
            best_state,
            f"{peak_month_row['order_year']}-{peak_month_row['order_month']:02d} "
            f"({peak_month_row['total_orders']:,} đơn)",
        ]
    }

    print("\n" + "="*50)
    print("  MASAN LOGISTICS — EXECUTIVE SUMMARY")
    print("  (Dựa trên dữ liệu OLIST Brazil)")
    print("="*50)
    for m, v in zip(summary["metric"], summary["value"]):
        print(f"  {m:<30} {v}")
    print("="*50)

    # Lưu CSV
    import pandas as pd
    df_summary = pd.DataFrame(summary)
    out_csv = os.path.join(gold_dir, "executive_summary.csv")
    df_summary.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\n Đã lưu: executive_summary.csv")


# -------------------------------------------------------
# Main
# -------------------------------------------------------
if __name__ == "__main__":
    print("=" * 55)
    print("GOLD PROCESSOR — Masan Data Pipeline")
    print("=" * 55)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    gold_dir = os.path.join(base_dir, "gold_layer")
    os.makedirs(gold_dir, exist_ok=True)

    spark = create_spark_session()
    df    = read_silver(spark, base_dir)

    # Chạy 4 KPI
    kpi_state = kpi_delivery_by_state(df, gold_dir)
    kpi_trend = kpi_monthly_trend(df, gold_dir)
    kpi_late  = kpi_late_orders_analysis(df, gold_dir)
    kpi_executive_summary(df, kpi_state, kpi_trend, gold_dir)

    spark.stop()
    print("\nGold Layer hoàn thành!")
    print(f"  Xem kết quả tại: {gold_dir}/")
    print("  Files: executive_summary.csv, kpi_*.csv")