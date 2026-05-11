# main.py
# Entry point — chạy toàn bộ pipeline bằng 1 lệnh:
# python main.py
# python main.py --step bronze   (chỉ chạy 1 bước)
# python main.py --step silver
# python main.py --step gold

import argparse
import logging
import os
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession

import config

# -------------------------------------------------------
# Setup logging ra cả terminal lẫn file
# -------------------------------------------------------
os.makedirs(config.LOG_DIR, exist_ok=True)

log_filename = os.path.join(
    config.LOG_DIR,
    f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("main")


# -------------------------------------------------------
# Spark session dùng chung cho toàn pipeline
# -------------------------------------------------------
def create_spark():
    spark = (
        SparkSession.builder
        .appName(config.SPARK_APP_NAME)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(config.SPARK_LOG_LEVEL)
    return spark


# -------------------------------------------------------
# Step runners — mỗi step là 1 hàm độc lập
# -------------------------------------------------------
def run_ingestion():
    """Bước 1: Kết nối và validate 3 nguồn data."""
    logger.info("=" * 55)
    logger.info("STEP 1 — DATA INGESTION")
    logger.info("=" * 55)
    from connector import get_connection

    results = {}
    for source_name in ["orders", "customers", "geolocation"]:
        start = time.time()
        df = get_connection(source_name, config.SOURCES[source_name])
        elapsed = time.time() - start
        results[source_name] = {
            "rows": len(df),
            "cols": len(df.columns),
            "elapsed_s": round(elapsed, 2)
        }
        logger.info(f" {source_name}: {len(df):,} rows | {elapsed:.2f}s")

    logger.info("STEP 1 DONE\n")
    return results


def run_schema_validation():
    """Bước 2: Kiểm tra join compatibility giữa các nguồn."""
    logger.info("=" * 55)
    logger.info("STEP 2 — SCHEMA VALIDATION")
    logger.info("=" * 55)
    from connector import get_connection
    from schema_validator import check_join_compatibility, print_validation_report

    df_orders    = get_connection("orders",    config.SOURCES["orders"])
    df_customers = get_connection("customers", config.SOURCES["customers"])
    df_geo       = get_connection("geolocation", config.SOURCES["geolocation"])

    report1 = check_join_compatibility(df_orders, df_customers, "orders_vs_customers")
    report2 = check_join_compatibility(df_customers, df_geo, "customers_vs_geolocation")

    print_validation_report(report1)
    print_validation_report(report2)

    if not report1["can_join"] or not report2["can_join"]:
        logger.error("Schema validation FAILED — dừng pipeline!")
        sys.exit(1)

    logger.info("STEP 2 DONE\n")


def run_bronze(spark):
    """Bước 3: Join 3 nguồn → Bronze Layer (Parquet)."""
    logger.info("=" * 55)
    logger.info("STEP 3 — BRONZE LAYER")
    logger.info("=" * 55)
    from spark_processor import build_bronze_layer

    os.makedirs(config.BRONZE_DIR, exist_ok=True)
    df_bronze = build_bronze_layer(spark, config.BASE_DIR)

    logger.info(f"  Bronze rows: {df_bronze.count():,}")
    logger.info(f"  Output: {config.BRONZE_ORDERS_PATH}")
    logger.info("STEP 3 DONE\n")
    return df_bronze


def run_silver(spark):
    """Bước 4: Clean, normalize → Silver Layer."""
    logger.info("=" * 55)
    logger.info("STEP 4 — SILVER LAYER")
    logger.info("=" * 55)
    from silver_processor import (
        read_bronze, fix_timestamps, handle_nulls,
        add_derived_columns, validate_silver, save_silver
    )

    os.makedirs(config.SILVER_DIR, exist_ok=True)

    df = read_bronze(spark, config.BASE_DIR)
    df = fix_timestamps(df)
    df = handle_nulls(df)
    df = add_derived_columns(df)

    quality_ok = validate_silver(df)
    if not quality_ok:
        logger.error("Data quality FAILED — không lưu Silver, dừng pipeline!")
        sys.exit(1)

    save_silver(df, config.BASE_DIR)
    logger.info(f"  Silver rows: {df.count():,}")
    logger.info(f"  Output: {config.SILVER_ORDERS_PATH}")
    logger.info("STEP 4 DONE\n")
    return df


def run_gold(spark):
    """Bước 5: Tính KPI → Gold Layer (analyst-ready CSV)."""
    logger.info("=" * 55)
    logger.info("STEP 5 — GOLD LAYER")
    logger.info("=" * 55)
    from gold_processor import (
        read_silver, kpi_delivery_by_state, kpi_monthly_trend,
        kpi_late_orders_analysis, kpi_executive_summary
    )

    os.makedirs(config.GOLD_DIR, exist_ok=True)

    df        = read_silver(spark, config.BASE_DIR)
    kpi_state = kpi_delivery_by_state(df, config.GOLD_DIR)
    kpi_trend = kpi_monthly_trend(df, config.GOLD_DIR)
    kpi_late  = kpi_late_orders_analysis(df, config.GOLD_DIR)
    kpi_executive_summary(df, kpi_state, kpi_trend, config.GOLD_DIR)

    logger.info(f"  Output: {config.GOLD_DIR}/")
    logger.info("STEP 5 DONE\n")


# -------------------------------------------------------
# Main orchestrator
# -------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Masan Logistics Data Pipeline — Palantir style"
    )
    parser.add_argument(
        "--step",
        choices=["ingest", "validate", "bronze", "silver", "gold", "all"],
        default="all",
        help="Chạy 1 bước cụ thể hoặc toàn bộ pipeline (default: all)"
    )
    args = parser.parse_args()

    # Header
    logger.info("╔" + "═" * 53 + "╗")
    logger.info("║       MASAN LOGISTICS DATA PIPELINE               ║")
    logger.info("║       Palantir Foundry Style — PySpark 4.x        ║")
    logger.info("╚" + "═" * 53 + "╝")
    logger.info(f"Start time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Step       : {args.step}")
    logger.info(f"Log file   : {log_filename}")
    logger.info(f"Base dir   : {config.BASE_DIR}\n")

    pipeline_start = time.time()

    # Kiểm tra data source tồn tại
    for name, path in config.SOURCES.items():
        if not os.path.exists(path):
            logger.error(f"Không tìm thấy file: {path}")
            logger.error("Hãy đặt CSV vào thư mục data_source/ trước khi chạy.")
            sys.exit(1)

    # Chạy theo step
    spark = None
    try:
        if args.step in ("ingest", "all"):
            run_ingestion()

        if args.step in ("validate", "all"):
            run_schema_validation()

        if args.step in ("bronze", "silver", "gold", "all"):
            spark = create_spark()

        if args.step in ("bronze", "all"):
            run_bronze(spark)

        if args.step in ("silver", "all"):
            run_silver(spark)

        if args.step in ("gold", "all"):
            run_gold(spark)

    except KeyboardInterrupt:
        logger.warning("Pipeline bị dừng bởi người dùng (Ctrl+C)")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Pipeline FAILED: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

    # Summary
    elapsed = time.time() - pipeline_start
    logger.info("╔" + "═" * 53 + "╗")
    logger.info("║  PIPELINE HOÀN THÀNH THÀNH CÔNG                   ║")
    logger.info("╚" + "═" * 53 + "╝")
    logger.info(f"Tổng thời gian : {elapsed:.1f}s ({elapsed/60:.1f} phút)")
    logger.info(f"Log đã lưu tại : {log_filename}")
    logger.info(f"Gold output    : {config.GOLD_DIR}/")


if __name__ == "__main__":
    main()