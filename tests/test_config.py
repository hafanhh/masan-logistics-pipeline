# tests/test_config.py
# Test config.py — đảm bảo paths và constants đúng

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


class TestConfig:

    def test_base_dir_exists(self):
        """BASE_DIR phải tồn tại"""
        assert os.path.exists(config.BASE_DIR)

    def test_sources_keys(self):
        """SOURCES phải có đủ 3 nguồn"""
        assert "orders"      in config.SOURCES
        assert "customers"   in config.SOURCES
        assert "geolocation" in config.SOURCES

    def test_brazil_bounds_valid(self):
        """Biên giới Brazil phải hợp lý"""
        assert config.BRAZIL_LAT_MIN < config.BRAZIL_LAT_MAX
        assert config.BRAZIL_LNG_MIN < config.BRAZIL_LNG_MAX
        assert config.BRAZIL_LAT_MIN < 0    # Brazil nằm dưới xích đạo
        assert config.BRAZIL_LNG_MIN < -28  # Brazil nằm bên trái

    def test_delivery_thresholds_ordered(self):
        """FAST < NORMAL < SLOW"""
        t = config.DELIVERY_SPEED_THRESHOLDS
        assert t["FAST"] < t["NORMAL"] < t["SLOW"]

    def test_delay_severity_ordered(self):
        """MINOR < MODERATE < SEVERE"""
        t = config.DELAY_SEVERITY_THRESHOLDS
        assert t["MINOR"] < t["MODERATE"] < t["SEVERE"]

    def test_dq_thresholds_in_valid_range(self):
        """DQ thresholds phải trong khoảng 0-100"""
        assert 0 < config.DQ_MAX_NULL_RATE_PCT  < 100
        assert 0 < config.DQ_MIN_MATCH_RATE_PCT < 100
        assert config.DQ_MAX_DELIVERY_DAYS > 0
        