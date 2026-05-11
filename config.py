# config.py
# Tập trung toàn bộ cấu hình — không hardcode trong business logic

import os

# -------------------------------------------------------
# Base paths
# -------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_SOURCE_DIR = os.path.join(BASE_DIR, "data_source")
BRONZE_DIR      = os.path.join(BASE_DIR, "bronze_layer")
SILVER_DIR      = os.path.join(BASE_DIR, "silver_layer")
GOLD_DIR        = os.path.join(BASE_DIR, "gold_layer")
LOG_DIR         = os.path.join(BASE_DIR, "logs")

# -------------------------------------------------------
# Source files
# -------------------------------------------------------
SOURCES = {
    "orders":      os.path.join(DATA_SOURCE_DIR, "orders.csv"),
    "customers":   os.path.join(DATA_SOURCE_DIR, "customers.csv"),
    "geolocation": os.path.join(DATA_SOURCE_DIR, "geolocation.csv"),
}

# -------------------------------------------------------
# Bronze outputs
# -------------------------------------------------------
BRONZE_ORDERS_PATH = os.path.join(BRONZE_DIR, "orders_enriched")

# -------------------------------------------------------
# Silver outputs
# -------------------------------------------------------
SILVER_ORDERS_PATH = os.path.join(SILVER_DIR, "orders_cleaned")

# -------------------------------------------------------
# Gold outputs
# -------------------------------------------------------
GOLD_KPI_STATE_PATH   = os.path.join(GOLD_DIR, "kpi_delivery_by_state")
GOLD_KPI_TREND_PATH   = os.path.join(GOLD_DIR, "kpi_monthly_trend")
GOLD_KPI_LATE_PATH    = os.path.join(GOLD_DIR, "kpi_late_orders")
GOLD_SUMMARY_CSV      = os.path.join(GOLD_DIR, "executive_summary.csv")

# -------------------------------------------------------
# Spark settings
# -------------------------------------------------------
SPARK_APP_NAME   = "Masan_DataPipeline"
SPARK_LOG_LEVEL  = "WARN"

# -------------------------------------------------------
# Brazil geographic bounds (dùng để validate tọa độ)
# -------------------------------------------------------
BRAZIL_LAT_MIN = -34.0
BRAZIL_LAT_MAX =   6.0
BRAZIL_LNG_MIN = -74.0
BRAZIL_LNG_MAX = -28.0

# -------------------------------------------------------
# Business rules
# -------------------------------------------------------
DELIVERY_SPEED_THRESHOLDS = {
    "FAST":      7,   # ≤ 7 ngày
    "NORMAL":   14,   # ≤ 14 ngày
    "SLOW":     30,   # ≤ 30 ngày
    # > 30 ngày → VERY_SLOW
}

DELAY_SEVERITY_THRESHOLDS = {
    "MINOR":     3,   # trễ 1-3 ngày
    "MODERATE":  7,   # trễ 4-7 ngày
    "SEVERE":   14,   # trễ 8-14 ngày
    # > 14 ngày → CRITICAL
}

# -------------------------------------------------------
# Data quality thresholds
# -------------------------------------------------------
DQ_MAX_NULL_RATE_PCT    = 5.0   # tối đa 5% null mới pass
DQ_MIN_MATCH_RATE_PCT   = 95.0  # tối thiểu 95% join key match
DQ_MAX_DELIVERY_DAYS    = 365   # đơn hàng không thể giao hơn 1 năm