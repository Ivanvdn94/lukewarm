"""
Tests for gold/format.py — format_output

Validates that:
- Dates are formatted as human-readable strings ("1 Aug 2003")
- Column labels switch correctly between heatwave and coldwave
- Temperature is rounded to 1 decimal place
- Results are ordered chronologically by start date
"""
import datetime

import pytest
from pyspark.sql import Row

from gold.format import format_output


def make_wave(from_date, to_date, duration, special_days, extreme_temp):
    return Row(
        from_date=datetime.date.fromisoformat(from_date),
        to_date=datetime.date.fromisoformat(to_date),
        duration=duration,
        special_days=float(special_days),
        extreme_temp=float(extreme_temp),
    )


# ---------------------------------------------------------------------------
# Column labels
# ---------------------------------------------------------------------------

def test_heatwave_uses_tropical_days_label(spark):
    df = spark.createDataFrame([make_wave("2003-08-01", "2003-08-10", 10, 7, 335)])
    result = format_output(df, "heatwave")
    assert "Number of tropical days" in result.columns


def test_coldwave_uses_high_frost_days_label(spark):
    df = spark.createDataFrame([make_wave("2012-02-01", "2012-02-10", 10, 5, -150)])
    result = format_output(df, "coldwave")
    assert "Number of high frost days" in result.columns


def test_heatwave_uses_max_temperature_label(spark):
    df = spark.createDataFrame([make_wave("2003-08-01", "2003-08-10", 10, 7, 335)])
    result = format_output(df, "heatwave")
    assert "Max temperature" in result.columns


def test_coldwave_uses_min_temperature_label(spark):
    df = spark.createDataFrame([make_wave("2012-02-01", "2012-02-10", 10, 5, -150)])
    result = format_output(df, "coldwave")
    assert "Min temperature" in result.columns


# ---------------------------------------------------------------------------
# Date formatting
# ---------------------------------------------------------------------------

def test_from_date_formatted_as_readable_string(spark):
    df = spark.createDataFrame([make_wave("2003-08-01", "2003-08-10", 10, 7, 335)])
    result = format_output(df, "heatwave").first()
    assert result["From date"] == "1 Aug 2003"


def test_to_date_formatted_as_readable_string(spark):
    df = spark.createDataFrame([make_wave("2003-08-01", "2003-08-10", 10, 7, 335)])
    result = format_output(df, "heatwave").first()
    assert result["To date (inc.)"] == "10 Aug 2003"


# ---------------------------------------------------------------------------
# Temperature rounding
# ---------------------------------------------------------------------------

def test_temperature_rounded_to_one_decimal(spark):
    df = spark.createDataFrame([make_wave("2003-08-01", "2003-08-10", 10, 7, 335)])
    result = format_output(df, "heatwave").first()
    # Value should be rounded, not a long float
    val = result["Max temperature"]
    assert round(val, 1) == val


# ---------------------------------------------------------------------------
# Ordering
# ---------------------------------------------------------------------------

def test_results_ordered_by_start_date(spark):
    rows = [
        make_wave("2003-08-20", "2003-08-27", 8, 5, 310),
        make_wave("2003-08-01", "2003-08-10", 10, 7, 335),
    ]
    df = spark.createDataFrame(rows)
    result = format_output(df, "heatwave").collect()
    assert result[0]["From date"] == "1 Aug 2003"
    assert result[1]["From date"] == "20 Aug 2003"


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

def test_output_has_exactly_five_columns(spark):
    df = spark.createDataFrame([make_wave("2003-08-01", "2003-08-10", 10, 7, 335)])
    result = format_output(df, "heatwave")
    assert len(result.columns) == 5
