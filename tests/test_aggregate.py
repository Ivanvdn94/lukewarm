"""
Tests for gold/aggregate.py — daily_aggregate

Validates that multiple 10-minute readings per day collapse correctly
to a single row with the day's max tx and min tn.
"""
import datetime

from pyspark.sql import Row

from gold.aggregate import daily_aggregate


def make_reading(date_str, tx, tn):
    return Row(
        date=datetime.date.fromisoformat(date_str),
        tx=float(tx),
        tn=float(tn),
    )


def test_daily_max_is_highest_tx(spark):
    rows = [
        make_reading("2003-08-01", 250, 180),
        make_reading("2003-08-01", 310, 160),
    ]
    df = spark.createDataFrame(rows)
    assert daily_aggregate(df).first()["daily_max"] == 310.0


def test_daily_min_is_lowest_tn(spark):
    rows = [
        make_reading("2003-08-01", 250, 180),
        make_reading("2003-08-01", 310, 120),
    ]
    df = spark.createDataFrame(rows)
    assert daily_aggregate(df).first()["daily_min"] == 120.0
