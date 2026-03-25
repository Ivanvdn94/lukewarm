"""
Tests for gold/format.py — format_output

Validates date formatting, column labels, and output ordering.
"""
import datetime

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


def test_heatwave_uses_tropical_days_label(spark):
    df = spark.createDataFrame([make_wave("2003-08-01", "2003-08-10", 10, 7, 335)])
    assert "Number of tropical days" in format_output(df, "heatwave").columns


def test_from_date_formatted_as_readable_string(spark):
    df = spark.createDataFrame([make_wave("2003-08-01", "2003-08-10", 10, 7, 335)])
    result = format_output(df, "heatwave").first()
    assert result["From date"] == "1 Aug 2003"
