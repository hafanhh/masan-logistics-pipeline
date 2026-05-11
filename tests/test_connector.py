# tests/test_connector.py
# Test connector.py — đọc file và validate schema

import pytest
import pandas as pd
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connector import validate_schema, get_connection


class TestValidateSchemaConnector:

    def test_valid_orders_schema(self):
        df = pd.DataFrame({
            "order_id":                      ["x"],
            "customer_id":                   ["y"],
            "order_status":                  ["delivered"],
            "order_purchase_timestamp":      ["2017-01-01"],
            "order_approved_at":             ["2017-01-01"],
            "order_delivered_carrier_date":  ["2017-01-08"],
            "order_delivered_customer_date": ["2017-01-10"],
            "order_estimated_delivery_date": ["2017-01-10"],
        })
        result = validate_schema(df, "orders")
        assert result["valid"] is True

    def test_geolocation_dtype_warning(self):
        """lat/lng là string → valid nhưng có warning"""
        df = pd.DataFrame({
            "geolocation_zip_code_prefix": [1000],
            "geolocation_lat":             ["not_a_float"],  # sai dtype
            "geolocation_lng":             ["-46.6"],
            "geolocation_city":            ["sao paulo"],
            "geolocation_state":           ["SP"],
        })
        result = validate_schema(df, "geolocation")
        assert result["valid"] is True           # vẫn valid vì dtype chỉ là warning
        assert len(result["warnings"]) > 0      # nhưng có warning


class TestGetConnection:

    def test_file_not_found_raises(self):
        """File không tồn tại → FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            get_connection("orders", "/nonexistent/path.csv")

    def test_read_valid_csv(self):
        """Đọc CSV hợp lệ → trả về DataFrame đúng"""
        # Tạo file CSV tạm
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("order_id,customer_id,order_status,"
                    "order_purchase_timestamp,order_approved_at,"
                    "order_delivered_carrier_date,order_delivered_customer_date,"
                    "order_estimated_delivery_date\n")
            f.write("abc,cust1,delivered,2017-01-01,2017-01-01,2017-01-08,2017-01-10,2017-01-10\n")
            tmp_path = f.name

        try:
            df = get_connection("orders", tmp_path)
            assert len(df) == 1
            assert "order_id" in df.columns
        finally:
            os.unlink(tmp_path)

    def test_invalid_schema_raises_value_error(self):
        """CSV thiếu cột bắt buộc → ValueError"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("wrong_col1,wrong_col2\n")
            f.write("val1,val2\n")
            tmp_path = f.name

        try:
            with pytest.raises(ValueError):
                get_connection("orders", tmp_path)
        finally:
            os.unlink(tmp_path)