"""
Integration test — Silver → Gold pipeline.

Feeds raw string rows (as they arrive from the Bronze CSV layer) through
filter_station → daily_aggregate → detect_heatwaves and asserts the correct
wave is found. No network, no files — everything runs in-memory.
"""
from pyspark.sql import Row

from silver.filter import filter_station
from gold.aggregate import daily_aggregate
from gold.detect import detect_heatwaves

STATION = "260_T_a"


def raw_row(dtg, tx, tn, location=STATION):
    """Mimics a row as loaded by silver/load.py — all values are strings."""
    return Row(DTG=dtg, LOCATION=location, TX_DRYB_10=tx, TN_DRYB_10=tn)


def test_silver_to_gold_detects_heatwave(spark):
    """
    5 hot days for De Bilt, each with a single 10-minute reading.
    The pipeline must filter, aggregate, and detect one heatwave.
    """
    rows = [
        raw_row("2003-08-01 12:00:00", "31.0", "18.0"),
        raw_row("2003-08-02 12:00:00", "31.0", "19.0"),
        raw_row("2003-08-03 12:00:00", "31.0", "20.0"),
        raw_row("2003-08-04 12:00:00", "26.0", "17.0"),
        raw_row("2003-08-05 12:00:00", "26.0", "16.0"),
        # A row for a different station — must be filtered out
        raw_row("2003-08-01 12:00:00", "35.0", "22.0", location="999_X_x"),
    ]

    raw_df     = spark.createDataFrame(rows)
    station_df = filter_station(raw_df, STATION)
    daily_df   = daily_aggregate(station_df)
    waves_df   = detect_heatwaves(daily_df)

    assert waves_df.count() == 1
