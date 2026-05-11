# tests/test_schema_validator.py
# Test các hàm trong schema_validator.py

import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connector import validate_schema
from schema_validator import check_join_compatibility, fix_dtype_mismatch


# -------------------------------------------------------
# Test validate_schema
# -------------------------------------------------------
class TestValidateSchema:

    def test_orders_valid_schema(self):
        """Schema đúng → valid = True, không có error"""
        df = pd.DataFrame({
            "order_id":                      ["abc123"],
            "customer_id":                   ["cust001"],
            "order_status":                  ["delivered"],
            "order_purchase_timestamp":      ["2017-10-02 10:56:33"],
            "order_approved_at":             ["2017-10-02 10:56:33"],
            "order_delivered_carrier_date":  ["2017-10-10 15:25:13"],
            "order_delivered_customer_date": ["2017-10-10 21:25:13"],
            "order_estimated_delivery_date": ["2017-10-15 00:00:00"],
        })
        result = validate_schema(df, "orders")
        assert result["valid"] is True
        assert len(result["errors"]) == 0

    def test_orders_missing_required_column(self):
        """Thiếu cột bắt buộc → valid = False"""
        df = pd.DataFrame({
            "order_id":     ["abc123"],
            "customer_id":  ["cust001"],
            # thiếu order_status, order_purchase_timestamp, order_delivered_customer_date
        })
        result = validate_schema(df, "orders")
        assert result["valid"] is False
        assert len(result["errors"]) > 0

    def test_customers_valid_schema(self):
        """Customers schema đúng → pass"""
        df = pd.DataFrame({
            "customer_id":               ["cust001"],
            "customer_unique_id":        ["uniq001"],
            "customer_zip_code_prefix":  [14409],
            "customer_city":             ["franca"],
            "customer_state":            ["SP"],
        })
        result = validate_schema(df, "customers")
        assert result["valid"] is True

    def test_unknown_source_returns_invalid(self):
        """Source không tồn tại → valid = False"""
        df = pd.DataFrame({"col1": [1]})
        result = validate_schema(df, "unknown_source")
        assert result["valid"] is False

    def test_empty_dataframe_missing_columns(self):
        """DataFrame rỗng nhưng không có cột → fail"""
        df = pd.DataFrame()
        result = validate_schema(df, "orders")
        assert result["valid"] is False


# -------------------------------------------------------
# Test check_join_compatibility
# -------------------------------------------------------
class TestJoinCompatibility:

    def test_orders_customers_perfect_match(self):
        """100% key match → can_join = True, match_rate = 100%"""
        df_orders = pd.DataFrame({
            "customer_id": ["A", "B", "C"],
            "order_id":    ["o1", "o2", "o3"]
        })
        df_customers = pd.DataFrame({
            "customer_id":              ["A", "B", "C"],
            "customer_zip_code_prefix": [1000, 2000, 3000]
        })
        report = check_join_compatibility(
            df_orders, df_customers, "orders_vs_customers"
        )
        assert report["can_join"] is True
        assert report["stats"]["matching_keys"] == 3
        assert report["stats"]["left_keys_no_match"] == 0

    def test_partial_match(self):
        """Một số key không match → can_join vẫn True nhưng có left_keys_no_match"""
        df_orders = pd.DataFrame({
            "customer_id": ["A", "B", "C", "D"],
            "order_id":    ["o1", "o2", "o3", "o4"]
        })
        df_customers = pd.DataFrame({
            "customer_id":              ["A", "B"],
            "customer_zip_code_prefix": [1000, 2000]
        })
        report = check_join_compatibility(
            df_orders, df_customers, "orders_vs_customers"
        )
        assert report["stats"]["left_keys_no_match"] == 2
        assert report["stats"]["matching_keys"] == 2

    def test_missing_join_key_left(self):
        """LEFT thiếu join key → can_join = False"""
        df_left  = pd.DataFrame({"wrong_col": ["A", "B"]})
        df_right = pd.DataFrame({"customer_id": ["A", "B"]})
        report = check_join_compatibility(
            df_left, df_right, "orders_vs_customers"
        )
        assert report["can_join"] is False

    def test_missing_join_key_right(self):
        """RIGHT thiếu join key → can_join = False"""
        df_left  = pd.DataFrame({"customer_id": ["A", "B"]})
        df_right = pd.DataFrame({"wrong_col": ["A", "B"]})
        report = check_join_compatibility(
            df_left, df_right, "orders_vs_customers"
        )
        assert report["can_join"] is False


# -------------------------------------------------------
# Test fix_dtype_mismatch
# -------------------------------------------------------
class TestFixDtype:

    def test_fix_object_to_int(self):
        """Cột string số → int64 thành công"""
        df = pd.DataFrame({"zip": ["1000", "2000", "3000"]})
        result = fix_dtype_mismatch(df, "zip", "int64")
        assert str(result["zip"].dtype) == "int64"
        assert result["zip"].tolist() == [1000, 2000, 3000]

    def test_fix_int_to_object(self):
        """Cột int → string thành công"""
        df = pd.DataFrame({"col": [1, 2, 3]})
        result = fix_dtype_mismatch(df, "col", "object")
        assert str(result["col"].dtype) == "object"

    def test_fix_handles_nan(self):
        """Cột có NaN → ép int64, NaN → 0"""
        df = pd.DataFrame({"zip": ["1000", None, "3000"]})
        result = fix_dtype_mismatch(df, "zip", "int64")
        assert str(result["zip"].dtype) == "int64"
        assert result["zip"].iloc[1] == 0

    def test_original_df_not_mutated(self):
        """fix_dtype không sửa DataFrame gốc"""
        df = pd.DataFrame({"zip": ["1000", "2000"]})
        original_dtype = str(df["zip"].dtype)
        fix_dtype_mismatch(df, "zip", "int64")
        assert str(df["zip"].dtype) == original_dtype