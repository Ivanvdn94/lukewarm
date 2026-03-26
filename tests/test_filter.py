"""
Unit test for silver/filter.py — null safety on temperature casting.
"""
from pyspark.sql import Row

from silver.filter import filter_station

STATION = "260_T_a"


def test_empty_tx_becomes_null(spark):
    """An empty string in TX must cast to null, not crash or silently produce 0."""
    df = spark.createDataFrame([
        Row(DTG="2003-08-01 12:00:00", LOCATION=STATION, TX_DRYB_10="", TN_DRYB_10="180")
    ])
    result = filter_station(df, STATION).first()
    assert result["tx"] is None
