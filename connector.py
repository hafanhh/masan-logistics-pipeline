# Mô phỏng: Palantir Data Connection / Data Ingress

import pandas as pd
import os
import logging 
from datetime import datetime

# Set up logging (Palantir style: audit trail is always available)
logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# Định nghĩa schema mong dợi cho từng nguồn : Đây là "Define Spource" step trong Palantir
EXPECTED_SCHEMAS = {
    "orders": {
        "required_columns": [
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at", 
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ],
        "dtype_hint":{
            "order_id" : "object",
            "customer_id" : "object",
            "order_status": "object",
        }
    },
    "customers": {
        "required_columns": [
            "customer_id", "customer_unique_id",
            "customer_zip_code_prefix", "customer_city", "customer_state"
        ],
        "dtype_hint":{
            "customer_id": "object",
            "customer_unique_id": "object",
            "customer_zip_code_prefix": "int64",
        }
    },
    "geolocation":{
        "required_columns": [
            "geolocation_zip_code_prefix", "geolocation_lat",
            "geolocation_lng", "geolocation_city", "geolocation_state"
        ],
        "dtype_hint": {
            "geolocation_zip_code_prefix": "int64",
            "geolocation_lat": "float64",
            "geolocation_lng": "float64",
        }
    }
}


def validate_schema(df : pd.DataFrame, source_name: str) -> dict:
    """
    Kiểm tra schema ngay khi đọc data
    Trả về dict chứa kết quả validation
    """

    schema = EXPECTED_SCHEMAS.get(source_name)
    if not schema:
        logger.warning(f"Không tìm thấy schema nào định nghĩa cho: {source_name}")
        return {"valid": False, "errors": ["Schema chưa được định nghĩa"]}
    errors = []
    warnings = []

    # Kiểm tra cột bắt buộc:
    missing_cols = [
        col for col in schema["required_columns"]
        if col not in df.columns
    ]

    if missing_cols:
        errors.append(f"Thiếu cột: {missing_cols}")
    
    # Kiểm tra kiểu dữ liệu (chỉ warning, không phải error cứng)
    for col, expected_dtype in schema["dtype_hint"].items():
        if col in df.columns:
            actual_dtype = str(df[col].dtype)
            if actual_dtype != expected_dtype:
                warnings.append(
                    f"Cột '{col}': mong đợi {expected_dtype}, thực tế {actual_dtype}"
                )
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }


def get_connection(source_type: str, file_path: str = None) -> pd.DataFrame:
    """
    Hàm chính: Kết nối và đọc data từ CSV hoặc API.
    source_type: "orders" | "customers" | "geolocation"
    file_path: đường dẫn tới file CSV (nếu source là CSV)
    
    Return: DataFrame được validate schema
    """
    logger.info(f"=== Bắt đầu kết nối source: {source_type}' ===")
    start_time = datetime.now()
    
    # Xác định đường dẫn file:
    if file_path is None:
        # Mặc định: Tìm trong thư mục data_source/
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, "data_source", f"{source_type}.csv")
        
    # Đọc data:
    if not os.path.exists(file_path):
        logger.error(f"Không tìm thấy file: {file_path}")
        raise FileNotFoundError(f"File không tồn tại: {file_path}")
    try:
        df = pd.read_csv(file_path, low_memory=False)
        logger.info(f"Đọc thành công: {len(df):,} dòng, {len(df.columns)} cột")
    except Exception as e:
        logger.error(f"Lỗi khi đọc file: {e}")
        raise
    
    # Validate schema ngay lập tức (Palantir work):
    validation_result = validate_schema(df, source_type)
    if not validation_result['valid']:
        logger.error(f"Schema không hợp lệ: {validation_result['errors']}")
        raise ValueError(
			f"Source '{source_type}' failed schema validation: "
			f"{validation_result['errors']}"
		)
    if validation_result['warnings']:
        for w in validation_result['warnings']:
            logger.warning(f"Schema warning: {w}")
            
    # Log thông tin cơ bản
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"Source '{source_type}' kết nối thành công trong {elapsed:.2f}s")
    logger.info(f"Columns: {list(df.columns)}")
    logger.info(f"Null counts:\n{df.isnull().sum()[df.isnull().sum() > 0].to_dict()}")
    
    return df

# Chạy thử trực tiếp để test
if __name__ == "__main__":
    print("\n" + "="*50)
    print("TEST: Kết nối 3 nguồn dữ liệu OLIST")
    print("="*50)

    sources = ["orders", "customers", "geolocation"]

    for source in sources:
        print(f"\n--- {source.upper()} ---")
        try:
            df = get_connection(source)
            print(f"✓ OK | Shape: {df.shape}")
            print(f"  Mẫu dữ liệu:\n{df.head(2).to_string()}")
        except Exception as e:
            print(f"✗ LỖI: {e}")