"""
Unit tests for gold/detect.py — the core detection algorithm.

KNMI definitions:
  Heatwave : >= 5 consecutive days daily_max >= 25°C, of which >= 3 days >= 30°C
  Coldwave : >= 5 consecutive days daily_max < 0°C,  of which >= 3 days min < -10°C
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


def test_minimum_valid_heatwave_detected(spark):
    """Exactly 5 days >= 25°C with exactly 3 tropical days (>= 30°C) — bare minimum."""
    rows = [
        day("2003-08-01", 31.0),
        day("2003-08-02", 31.0),
        day("2003-08-03", 31.0),
        day("2003-08-04", 26.0),
        day("2003-08-05", 26.0),
    ]
    assert detect_heatwaves(spark.createDataFrame(rows)).count() == 1


def test_gap_splits_into_two_heatwaves(spark):
    """A cool day between two hot runs must produce two separate waves, not one."""
    rows = [
        # First wave
        day("2003-08-01", 31.0), day("2003-08-02", 31.0), day("2003-08-03", 31.0),
        day("2003-08-04", 26.0), day("2003-08-05", 26.0),
        # Aug 6 missing — gap day below 25°C
        # Second wave
        day("2003-08-07", 31.0), day("2003-08-08", 31.0), day("2003-08-09", 31.0),
        day("2003-08-10", 26.0), day("2003-08-11", 26.0),
    ]
    assert detect_heatwaves(spark.createDataFrame(rows)).count() == 2


def test_minimum_valid_coldwave_detected(spark):
    """Exactly 5 days max < 0°C with exactly 3 high-frost days (min < -10°C) — bare minimum."""
    rows = [
        day("2012-02-01", -0.5, -12.0),
        day("2012-02-02", -0.5, -12.0),
        day("2012-02-03", -0.5, -12.0),
        day("2012-02-04", -0.5,  -5.0),
        day("2012-02-05", -0.5,  -5.0),
    ]
    assert detect_coldwaves(spark.createDataFrame(rows)).count() == 1
