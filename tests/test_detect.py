"""
Tests for gold/detect.py — detect_heatwaves and detect_coldwaves

Directly encodes the official KNMI meteorological definitions:

  Heatwave : >= 5 consecutive days with daily_max >= 25°C,
             of which >= 3 days with daily_max >= 30°C (tropical days)

  Coldwave : >= 5 consecutive days with daily_max < 0°C,
             of which >= 3 days with daily_min < -10°C (high-frost days)

Values are in actual degrees Celsius:
  31.0  = tropical day        (>= 30°C)
  26.0  = hot but not tropical (>= 25°C, < 30°C)
  -0.5  = freezing day        (max < 0°C)
  -12.0 = high frost day      (min < -10°C)
  -5.0  = cold but not high frost
"""
import datetime

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

def test_minimum_valid_heatwave_detected(spark):
    """Exactly 5 days >= 25, exactly 3 tropical (>= 30)."""
    rows = [
        day("2003-08-01", 31.0),
        day("2003-08-02", 31.0),
        day("2003-08-03", 31.0),
        day("2003-08-04", 26.0),
        day("2003-08-05", 26.0),
    ]
    assert detect_heatwaves(spark.createDataFrame(rows)).count() == 1


def test_only_4_days_not_a_heatwave(spark):
    """One day short of the 5-day minimum."""
    rows = [
        day("2003-08-01", 31.0),
        day("2003-08-02", 31.0),
        day("2003-08-03", 31.0),
        day("2003-08-04", 26.0),
    ]
    assert detect_heatwaves(spark.createDataFrame(rows)).count() == 0


def test_only_2_tropical_days_not_a_heatwave(spark):
    """5 days >= 25 but only 2 tropical — needs 3."""
    rows = [
        day("2003-08-01", 31.0),
        day("2003-08-02", 31.0),
        day("2003-08-03", 26.0),
        day("2003-08-04", 26.0),
        day("2003-08-05", 26.0),
    ]
    assert detect_heatwaves(spark.createDataFrame(rows)).count() == 0


def test_gap_splits_into_two_heatwaves(spark):
    """A cool day between two hot runs must produce two separate waves."""
    rows = [
        # First wave
        day("2003-08-01", 31.0), day("2003-08-02", 31.0), day("2003-08-03", 31.0),
        day("2003-08-04", 26.0), day("2003-08-05", 26.0),
        # Gap day (below 25 — not in either wave, Aug 6 missing)
        # Second wave
        day("2003-08-07", 31.0), day("2003-08-08", 31.0), day("2003-08-09", 31.0),
        day("2003-08-10", 26.0), day("2003-08-11", 26.0),
    ]
    assert detect_heatwaves(spark.createDataFrame(rows)).count() == 2


# ===========================================================================
# COLDWAVE TESTS
# ===========================================================================

def test_minimum_valid_coldwave_detected(spark):
    """Exactly 5 days max < 0, exactly 3 high-frost days (min < -10)."""
    rows = [
        day("2012-02-01", -0.5, -12.0),
        day("2012-02-02", -0.5, -12.0),
        day("2012-02-03", -0.5, -12.0),
        day("2012-02-04", -0.5,  -5.0),
        day("2012-02-05", -0.5,  -5.0),
    ]
    assert detect_coldwaves(spark.createDataFrame(rows)).count() == 1
