"""
Tests for gold/aggregate.py — daily_aggregate

Validates that multiple 10-minute readings per day are correctly collapsed
to a single row containing the day's maximum tx and minimum tn.
"""
import datetime

import pytest
from pyspark.sql import Row

from gold.aggregate import daily_aggregate


def make_reading(date_str, tx, tn):
    return Row(
        date=datetime.date.fromisoformat(date_str),
        tx=float(tx),
        tn=float(tn),
    )


# ---------------------------------------------------------------------------
# Aggregation correctness
# ---------------------------------------------------------------------------

def test_multiple_readings_collapse_to_one_row(spark):
    rows = [
        make_reading("2003-08-01", 250, 180),
        make_reading("2003-08-01", 285, 150),
        make_reading("2003-08-01", 310, 200),
    ]
    df = spark.createDataFrame(rows)
    result = daily_aggregate(df).collect()
    assert len(result) == 1


def test_daily_max_is_highest_tx(spark):
    rows = [
        make_reading("2003-08-01", 250, 180),
        make_reading("2003-08-01", 310, 160),
    ]
    df = spark.createDataFrame(rows)
    result = daily_aggregate(df).first()
    assert result["daily_max"] == 310.0


def test_daily_min_is_lowest_tn(spark):
    rows = [
        make_reading("2003-08-01", 250, 180),
        make_reading("2003-08-01", 310, 120),
    ]
    df = spark.createDataFrame(rows)
    result = daily_aggregate(df).first()
    assert result["daily_min"] == 120.0


def test_separate_days_produce_separate_rows(spark):
    rows = [
        make_reading("2003-08-01", 285, 180),
        make_reading("2003-08-02", 310, 160),
    ]
    df = spark.createDataFrame(rows)
    result = daily_aggregate(df).collect()
    assert len(result) == 2


def test_output_ordered_by_date(spark):
    rows = [
        make_reading("2003-08-03", 260, 180),
        make_reading("2003-08-01", 310, 160),
        make_reading("2003-08-02", 285, 170),
    ]
    df = spark.createDataFrame(rows)
    dates = [r["date"] for r in daily_aggregate(df).collect()]
    assert dates == sorted(dates)


def test_single_reading_passes_through(spark):
    rows = [make_reading("2003-08-01", 285, 180)]
    df = spark.createDataFrame(rows)
    result = daily_aggregate(df).first()
    assert result["daily_max"] == 285.0
    assert result["daily_min"] == 180.0


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

def test_output_columns_are_date_daily_max_daily_min(spark):
    rows = [make_reading("2003-08-01", 285, 180)]
    df = spark.createDataFrame(rows)
    result = daily_aggregate(df)
    assert set(result.columns) == {"date", "daily_max", "daily_min"}
