"""
Tests for gold/detect.py — detect_heatwaves and detect_coldwaves

These tests directly encode the official KNMI meteorological definitions:

  Heatwave : >= 5 consecutive days with daily_max >= 25°C,
             of which >= 3 days with daily_max >= 30°C  [tropical days]

  Coldwave : >= 5 consecutive days with daily_max < 0°C,
             of which >= 3 days with daily_min < -10°C  [high-frost days]

Values are in actual degrees Celsius, matching the thresholds in detect.py:
  31.0  = tropical day       (>= 30°C)
  26.0  = hot but not tropical (>= 25°C, < 30°C)
  20.0  = cool day           (< 25°C, breaks heatwave streak)
  -0.5  = freezing day       (max < 0°C, qualifies for coldwave)
  -12.0 = high frost day     (min < -10°C)
  -5.0  = cold but not high frost (min >= -10°C)
"""
import datetime

import pytest
from pyspark.sql import Row

from gold.detect import detect_heatwaves, detect_coldwaves


def day(date_str, daily_max, daily_min=0.0):
    return Row(
        date=datetime.date.fromisoformat(date_str),
        daily_max=float(daily_max),
        daily_min=float(daily_min),
    )


# ===========================================================================
# HEATWAVE TESTS
# ===========================================================================

class TestDetectHeatwaves:

    # -----------------------------------------------------------------------
    # Happy path — wave IS detected
    # -----------------------------------------------------------------------

    def test_minimum_valid_heatwave_detected(self, spark):
        """Exactly 5 days >= 25, exactly 3 tropical (>= 30)."""
        rows = [
            day("2003-08-01", 31.0),
            day("2003-08-02", 31.0),
            day("2003-08-03", 31.0),
            day("2003-08-04", 26.0),
            day("2003-08-05", 26.0),
        ]
        df = spark.createDataFrame(rows)
        assert detect_heatwaves(df).count() == 1

    def test_long_heatwave_detected(self, spark):
        """10 consecutive tropical days well exceeds both thresholds."""
        rows = [day(f"2003-08-{i:02d}", 32.0) for i in range(1, 11)]
        df = spark.createDataFrame(rows)
        assert detect_heatwaves(df).count() == 1

    def test_heatwave_duration_is_correct(self, spark):
        rows = [
            day("2003-08-01", 31.0),
            day("2003-08-02", 31.0),
            day("2003-08-03", 31.0),
            day("2003-08-04", 26.0),
            day("2003-08-05", 26.0),
        ]
        df = spark.createDataFrame(rows)
        result = detect_heatwaves(df).first()
        assert result["duration"] == 5

    def test_heatwave_start_and_end_dates_are_correct(self, spark):
        rows = [
            day("2003-08-01", 31.0),
            day("2003-08-02", 31.0),
            day("2003-08-03", 31.0),
            day("2003-08-04", 26.0),
            day("2003-08-05", 26.0),
        ]
        df = spark.createDataFrame(rows)
        result = detect_heatwaves(df).first()
        assert result["from_date"] == datetime.date(2003, 8, 1)
        assert result["to_date"]   == datetime.date(2003, 8, 5)

    def test_tropical_day_count_is_correct(self, spark):
        rows = [
            day("2003-08-01", 31.0),  # tropical
            day("2003-08-02", 31.0),  # tropical
            day("2003-08-03", 31.0),  # tropical
            day("2003-08-04", 26.0),  # hot, not tropical
            day("2003-08-05", 26.0),  # hot, not tropical
        ]
        df = spark.createDataFrame(rows)
        result = detect_heatwaves(df).first()
        assert result["special_days"] == 3

    def test_max_temperature_reported_correctly(self, spark):
        rows = [
            day("2003-08-01", 33.5),
            day("2003-08-02", 31.0),
            day("2003-08-03", 31.0),
            day("2003-08-04", 26.0),
            day("2003-08-05", 26.0),
        ]
        df = spark.createDataFrame(rows)
        result = detect_heatwaves(df).first()
        assert result["extreme_temp"] == 33.5

    # -----------------------------------------------------------------------
    # Boundary — wave is NOT detected
    # -----------------------------------------------------------------------

    def test_only_4_days_not_a_heatwave(self, spark):
        """One day short of the 5-day minimum."""
        rows = [
            day("2003-08-01", 31.0),
            day("2003-08-02", 31.0),
            day("2003-08-03", 31.0),
            day("2003-08-04", 26.0),
        ]
        df = spark.createDataFrame(rows)
        assert detect_heatwaves(df).count() == 0

    def test_only_2_tropical_days_not_a_heatwave(self, spark):
        """5 days >= 25 but only 2 tropical — needs 3."""
        rows = [
            day("2003-08-01", 31.0),
            day("2003-08-02", 31.0),
            day("2003-08-03", 26.0),
            day("2003-08-04", 26.0),
            day("2003-08-05", 26.0),
        ]
        df = spark.createDataFrame(rows)
        assert detect_heatwaves(df).count() == 0

    def test_days_below_25_are_not_counted(self, spark):
        """A cool day in the middle breaks the streak — the two halves do not combine."""
        rows = [
            day("2003-08-01", 31.0),
            day("2003-08-02", 20.0),  # cool day — breaks the streak
            day("2003-08-03", 31.0),
            day("2003-08-04", 31.0),
            day("2003-08-05", 26.0),
            day("2003-08-06", 26.0),
        ]
        df = spark.createDataFrame(rows)
        assert detect_heatwaves(df).count() == 0

    def test_empty_dataframe_returns_no_heatwaves(self, spark):
        schema = "date DATE, daily_max DOUBLE, daily_min DOUBLE"
        df = spark.createDataFrame([], schema)
        assert detect_heatwaves(df).count() == 0

    # -----------------------------------------------------------------------
    # Gaps-and-islands correctness
    # -----------------------------------------------------------------------

    def test_gap_splits_consecutive_days_into_two_waves(self, spark):
        """A single cool day between two hot runs must produce two separate waves."""
        rows = [
            # First wave
            day("2003-08-01", 31.0), day("2003-08-02", 31.0), day("2003-08-03", 31.0),
            day("2003-08-04", 26.0), day("2003-08-05", 26.0),
            # Gap day (below 25 — not included in either wave)
            # Second wave
            day("2003-08-07", 31.0), day("2003-08-08", 31.0), day("2003-08-09", 31.0),
            day("2003-08-10", 26.0), day("2003-08-11", 26.0),
        ]
        df = spark.createDataFrame(rows)
        assert detect_heatwaves(df).count() == 2

    def test_two_waves_have_correct_individual_dates(self, spark):
        rows = [
            day("2003-08-01", 31.0), day("2003-08-02", 31.0), day("2003-08-03", 31.0),
            day("2003-08-04", 26.0), day("2003-08-05", 26.0),
            day("2003-08-07", 31.0), day("2003-08-08", 31.0), day("2003-08-09", 31.0),
            day("2003-08-10", 26.0), day("2003-08-11", 26.0),
        ]
        df = spark.createDataFrame(rows)
        results = sorted(detect_heatwaves(df).collect(), key=lambda r: r["from_date"])
        assert results[0]["from_date"] == datetime.date(2003, 8, 1)
        assert results[1]["from_date"] == datetime.date(2003, 8, 7)


# ===========================================================================
# COLDWAVE TESTS
# ===========================================================================

class TestDetectColdwaves:

    # -----------------------------------------------------------------------
    # Happy path — wave IS detected
    # -----------------------------------------------------------------------

    def test_minimum_valid_coldwave_detected(self, spark):
        """Exactly 5 days max < 0, exactly 3 high-frost days (min < -10)."""
        rows = [
            day("2012-02-01", -0.5, -12.0),
            day("2012-02-02", -0.5, -12.0),
            day("2012-02-03", -0.5, -12.0),
            day("2012-02-04", -0.5,  -5.0),
            day("2012-02-05", -0.5,  -5.0),
        ]
        df = spark.createDataFrame(rows)
        assert detect_coldwaves(df).count() == 1

    def test_coldwave_duration_is_correct(self, spark):
        rows = [
            day("2012-02-01", -0.5, -12.0),
            day("2012-02-02", -0.5, -12.0),
            day("2012-02-03", -0.5, -12.0),
            day("2012-02-04", -0.5,  -5.0),
            day("2012-02-05", -0.5,  -5.0),
        ]
        df = spark.createDataFrame(rows)
        result = detect_coldwaves(df).first()
        assert result["duration"] == 5

    def test_coldwave_start_and_end_dates_are_correct(self, spark):
        rows = [
            day("2012-02-01", -0.5, -12.0),
            day("2012-02-02", -0.5, -12.0),
            day("2012-02-03", -0.5, -12.0),
            day("2012-02-04", -0.5,  -5.0),
            day("2012-02-05", -0.5,  -5.0),
        ]
        df = spark.createDataFrame(rows)
        result = detect_coldwaves(df).first()
        assert result["from_date"] == datetime.date(2012, 2, 1)
        assert result["to_date"]   == datetime.date(2012, 2, 5)

    def test_high_frost_day_count_is_correct(self, spark):
        rows = [
            day("2012-02-01", -0.5, -12.0),  # high frost
            day("2012-02-02", -0.5, -12.0),  # high frost
            day("2012-02-03", -0.5, -12.0),  # high frost
            day("2012-02-04", -0.5,  -5.0),  # cold, not high frost
            day("2012-02-05", -0.5,  -5.0),  # cold, not high frost
        ]
        df = spark.createDataFrame(rows)
        result = detect_coldwaves(df).first()
        assert result["special_days"] == 3

    def test_min_temperature_reported_correctly(self, spark):
        rows = [
            day("2012-02-01", -0.5, -15.0),  # coldest night
            day("2012-02-02", -0.5, -12.0),
            day("2012-02-03", -0.5, -12.0),
            day("2012-02-04", -0.5,  -5.0),
            day("2012-02-05", -0.5,  -5.0),
        ]
        df = spark.createDataFrame(rows)
        result = detect_coldwaves(df).first()
        assert result["extreme_temp"] == -15.0

    # -----------------------------------------------------------------------
    # Boundary — wave is NOT detected
    # -----------------------------------------------------------------------

    def test_only_4_days_not_a_coldwave(self, spark):
        rows = [
            day("2012-02-01", -0.5, -12.0),
            day("2012-02-02", -0.5, -12.0),
            day("2012-02-03", -0.5, -12.0),
            day("2012-02-04", -0.5,  -5.0),
        ]
        df = spark.createDataFrame(rows)
        assert detect_coldwaves(df).count() == 0

    def test_only_2_high_frost_days_not_a_coldwave(self, spark):
        """5 days max < 0 but only 2 with min < -10 — needs 3."""
        rows = [
            day("2012-02-01", -0.5, -12.0),
            day("2012-02-02", -0.5, -12.0),
            day("2012-02-03", -0.5,  -5.0),
            day("2012-02-04", -0.5,  -5.0),
            day("2012-02-05", -0.5,  -5.0),
        ]
        df = spark.createDataFrame(rows)
        assert detect_coldwaves(df).count() == 0

    def test_day_with_max_above_zero_breaks_coldwave(self, spark):
        """A single day with max >= 0 must break the consecutive streak."""
        rows = [
            day("2012-02-01", -0.5, -12.0),
            day("2012-02-02",  5.0, -12.0),  # above zero — breaks streak
            day("2012-02-03", -0.5, -12.0),
            day("2012-02-04", -0.5, -12.0),
            day("2012-02-05", -0.5,  -5.0),
            day("2012-02-06", -0.5,  -5.0),
        ]
        df = spark.createDataFrame(rows)
        assert detect_coldwaves(df).count() == 0

    def test_empty_dataframe_returns_no_coldwaves(self, spark):
        schema = "date DATE, daily_max DOUBLE, daily_min DOUBLE"
        df = spark.createDataFrame([], schema)
        assert detect_coldwaves(df).count() == 0

    # -----------------------------------------------------------------------
    # Gaps-and-islands correctness
    # -----------------------------------------------------------------------

    def test_gap_splits_consecutive_days_into_two_coldwaves(self, spark):
        rows = [
            # First wave
            day("2012-02-01", -0.5, -12.0), day("2012-02-02", -0.5, -12.0),
            day("2012-02-03", -0.5, -12.0), day("2012-02-04", -0.5,  -5.0),
            day("2012-02-05", -0.5,  -5.0),
            # Warm gap (not included)
            # Second wave
            day("2012-02-07", -0.5, -12.0), day("2012-02-08", -0.5, -12.0),
            day("2012-02-09", -0.5, -12.0), day("2012-02-10", -0.5,  -5.0),
            day("2012-02-11", -0.5,  -5.0),
        ]
        df = spark.createDataFrame(rows)
        assert detect_coldwaves(df).count() == 2
