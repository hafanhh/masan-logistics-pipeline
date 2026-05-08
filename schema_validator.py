# Mô phỏng: Palantir Schema Enforcement khi join nhiều nguồn

import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Định nghĩa "contract" giữa các nguồn khi join
# Trong Palantir, đây gọi là "Join Key Compatibility Check"

JOIN_CONTRACT = {
    "orders_vs_customers": {
        "left_key":"customer_id",
        "right_key":"customer_id",
        "expected_dtype":"object",
        "description":"ERP join CRM via customer_id"
    },
    "customers_vs_geolocation":{
        "left_key":"customer_zip_code_prefix",
        "right_key":"geolocation_zip_code_prefix",
        "expected_dtype": "int64",
        "description": "CRM join Logistics via zip code"
    }
}

def check_join_compatibility(
        df_left: pd.DataFrame,
        df_right: pd.DataFrame,
        contract_name: str
) -> dict:
    """
    Kiểm tra 2 DataFrame có join được không.
    Trả về report chi tiết - không raise lỗi, chỉ báo cáo
    """
    contract = JOIN_CONTRACT[contract_name]
    left_key = contract["left_key"]
    right_key = contract['right_key']
    expected_dtype = contract['expected_dtype']

    report = {
        "contract": contract_name,
        "description": contract['description'],
        "can_join": True,
        "issues": [],
        "stats": {}
    }

    # Check if join key columns exist:
    if left_key not in df_left.columns:
        report['issues'].append(f"LEFT table lack of join key: '{left_key}'")
        report['can_join'] = False
    
    if right_key not in df_right.columns:
        report['issues'].append(f"RIGHT table lack of join key: '{right_key}'")
        report['can_join'] = False
    
    if not report['can_join']:
        return report
    
    # Check dtype of join keys:
    left_dtype = str(df_left[left_key].dtype)
    right_dtype  = str(df_right[right_key].dtype)

    if left_dtype != expected_dtype:
        report['issues'].append(
            f"LEFT dtype sai: mong {expected_dtype}, nhưng lại có {left_dtype}"
        )

    if left_dtype != right_dtype:
        report['issues'].append(
            f"Dtype mismatch: LEFT '{left_key}' = '{left_dtype}', "
            f"RIGHT '{right_key}' = '{right_dtype}'."
        )
        report['can_join'] = False


    # Thống kê overlap (bao nhiêu % key match được)
    left_keys = set(df_left[left_key].dropna().unique())
    right_keys = set(df_right[right_key].dropna().unique())

    overlap = left_keys & right_keys # Tập hợp các phần tử vừa có trong left vừa có trong right
    left_only = left_keys - right_keys # Tập hợp các phần tử chỉ có trong left

    report['stats'] = {
        "left_unique_keys":len(left_keys),
        "right_unique_keys": len(right_keys),
        "matching_keys":len(overlap),
        "left_keys_no_match": len(left_only),
        "match_rate": f"{len(overlap)/len(left_keys)*100:.1f}%" if left_keys else "0%"
    }
    
    return report



def fix_dtype_mismatch(
        df: pd.DataFrame,
        column: str,
        target_dtype: str
) -> pd.DataFrame:
    '''
    Ép kiểu dữ liệu để fix schema mismatch.
    Đât là bước "Schema Harmonization' trong Palantir
    '''
    logger.info(f"Fix dtype: column '{column}' -> {target_dtype} ")
    df = df.copy()

    try:
        if target_dtype == "int64":
            # Handle NaN before convert to int:
            df[column] = pd.to_numeric(df[column], errors = "coerce")
            df[column] = df[column].fillna(0).astype("int64")

        elif target_dtype == "object":
            df[column] = df[column].astype(str)
        
        elif target_dtype == "float64":
            df[column] = pd.to_numeric(df[column], errors = "coerce")

        logger.info(f" Converted '{column}' into '{target_dtype}'")

    except Exception as e:
        logger.error(f"Could not convert into dtype: {e}")
        raise

    return df



def print_validation_report(report: dict):
    '''
    Print out the report
    '''
    print(f"\n{'='*55}")
    print(f"CONTRACT: {report['contract']}")
    print(f"Mô tả   : {report['description']}")
    print(f"Kết quả : {'CÓ THỂ JOIN' if report['can_join'] else 'KHÔNG THỂ JOIN'}")

    if report["issues"]:
        print(f"\nVẤN ĐỀ PHÁT HIỆN:")
        for issue in report["issues"]:
            print(f"{issue}")

    if report["stats"]:
        s = report["stats"]
        print(f"\nTHỐNG KÊ KEY:")
        print(f"  LEFT  unique keys : {s['left_unique_keys']:,}")
        print(f"  RIGHT unique keys : {s['right_unique_keys']:,}")
        print(f"  Khớp nhau         : {s['matching_keys']:,}")
        print(f"  LEFT không match  : {s['left_keys_no_match']:,}")
        print(f"  Match rate        : {s['match_rate']}")
    print('='*55)


# --- Test trực tiếp ---
if __name__ == "__main__":
    from connector import get_connection

    print("\nLoad 3 nguồn dữ liệu...")
    df_orders = get_connection("orders")
    df_customers = get_connection("customers")
    df_geo = get_connection("geolocation")

    # Kiểm tra contract 1: orders ↔ customers
    report1 = check_join_compatibility(
        df_orders, df_customers, "orders_vs_customers"
    )
    print_validation_report(report1)

    # Kiểm tra contract 2: customers ↔ geolocation
    report2 = check_join_compatibility(
        df_customers, df_geo, "customers_vs_geolocation"
    )
    print_validation_report(report2)

    # Nếu có vấn đề dtype → fix và kiểm tra lại
    if not report2["can_join"]:
        print("\nĐang fix schema mismatch...")
        df_customers = fix_dtype_mismatch(
            df_customers, "customer_zip_code_prefix", "int64"
        )
        df_geo = fix_dtype_mismatch(
            df_geo, "geolocation_zip_code_prefix", "int64"
        )
        report2_fixed = check_join_compatibility(
            df_customers, df_geo, "customers_vs_geolocation"
        )
        print_validation_report(report2_fixed)