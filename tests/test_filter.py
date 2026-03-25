"""
Tests for silver/filter.py — filter_station

Validates station filtering, timestamp parsing, type casting, and null handling.
"""
import datetime

from pyspark.sql import Row

from silver.filter import filter_station

STATION = "260_T_a"
OTHER   = "999_X_x"


def make_row(dtg, location=STATION, tx="285", tn="180"):
    return Row(DTG=dtg, LOCATION=location, TX_DRYB_10=tx, TN_DRYB_10=tn)


def test_keeps_correct_station(spark):
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00")])
    assert filter_station(df, STATION).count() == 1


def test_trims_whitespace_from_location(spark):
    """Fixed-width derived CSVs often have trailing spaces in string columns."""
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00", location="  260_T_a  ")])
    assert filter_station(df, STATION).count() == 1


def test_parses_full_timestamp_format(spark):
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00")])
    result = filter_station(df, STATION).first()
    assert result["date"] == datetime.date(2003, 8, 1)


def test_empty_tx_becomes_null(spark):
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00", tx="")])
    result = filter_station(df, STATION).first()
    assert result["tx"] is None
