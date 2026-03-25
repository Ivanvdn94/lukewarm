"""
Tests for silver/filter.py — filter_station

Validates station filtering, timestamp parsing across multiple date formats,
type casting of temperature columns, and null handling.
"""
import datetime

import pytest
from pyspark.sql import Row

from silver.filter import filter_station

STATION = "260_T_a"
OTHER   = "999_X_x"


def make_row(dtg, location=STATION, tx="285", tn="180"):
    return Row(DTG=dtg, LOCATION=location, TX_DRYB_10=tx, TN_DRYB_10=tn)


# ---------------------------------------------------------------------------
# Station filtering
# ---------------------------------------------------------------------------

def test_keeps_correct_station(spark):
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00")])
    assert filter_station(df, STATION).count() == 1


def test_excludes_other_stations(spark):
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00", location=OTHER)])
    assert filter_station(df, STATION).count() == 0


def test_trims_whitespace_from_location(spark):
    """Fixed-width derived CSVs often have trailing spaces in string columns."""
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00", location="  260_T_a  ")])
    assert filter_station(df, STATION).count() == 1


def test_mixed_stations_only_keeps_target(spark):
    rows = [
        make_row("2003-08-01 12:00:00", location=STATION),
        make_row("2003-08-01 12:00:00", location=OTHER),
        make_row("2003-08-02 12:00:00", location=STATION),
    ]
    df = spark.createDataFrame(rows)
    assert filter_station(df, STATION).count() == 2


# ---------------------------------------------------------------------------
# Timestamp / date parsing
# ---------------------------------------------------------------------------

def test_parses_full_timestamp_format(spark):
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00")])
    result = filter_station(df, STATION).first()
    assert result["date"] == datetime.date(2003, 8, 1)


def test_parses_compact_timestamp_format(spark):
    df = spark.createDataFrame([make_row("20030801120000")])
    result = filter_station(df, STATION).first()
    assert result["date"] == datetime.date(2003, 8, 1)


def test_drops_rows_with_unparseable_date(spark):
    df = spark.createDataFrame([make_row("not-a-date")])
    assert filter_station(df, STATION).count() == 0


def test_drops_rows_with_empty_date(spark):
    df = spark.createDataFrame([make_row("")])
    assert filter_station(df, STATION).count() == 0


# ---------------------------------------------------------------------------
# Temperature casting
# ---------------------------------------------------------------------------

def test_tx_cast_to_double(spark):
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00", tx="285")])
    result = filter_station(df, STATION).first()
    assert result["tx"] == 285.0
    assert isinstance(result["tx"], float)


def test_tn_cast_to_double(spark):
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00", tn="180")])
    result = filter_station(df, STATION).first()
    assert result["tn"] == 180.0


def test_empty_tx_becomes_null(spark):
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00", tx="")])
    result = filter_station(df, STATION).first()
    assert result["tx"] is None


def test_empty_tn_becomes_null(spark):
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00", tn="")])
    result = filter_station(df, STATION).first()
    assert result["tn"] is None


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

def test_output_only_has_date_tx_tn_columns(spark):
    df = spark.createDataFrame([make_row("2003-08-01 12:00:00")])
    result = filter_station(df, STATION)
    assert set(result.columns) == {"date", "tx", "tn"}
