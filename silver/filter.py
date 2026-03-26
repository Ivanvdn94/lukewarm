"""
Silver layer — station filtering and type casting.

Takes the raw string DataFrame from the Bronze layer and produces a clean,
typed DataFrame containing only the rows and columns needed downstream:

    date  (DateType)   — observation date, parsed from the DTG timestamp
    tx    (DoubleType) — daily maximum dry-bulb temperature in °C
    tn    (DoubleType) — daily minimum dry-bulb temperature in °C

All other stations and columns are discarded here so that the Gold layer
works with the smallest possible dataset.
"""

from pyspark.sql import functions as F


def filter_station(raw_df, station: str):
    """
    Filter to one station, parse the timestamp, and cast temperature columns.

    Three things happen in sequence:

    1. Station filter — rows where LOCATION (trimmed) does not match the target
       station are dropped. trim() is applied because the fixed-width parser can
       leave trailing spaces in string columns.

    2. Date parsing — the DTG column contains timestamps in multiple formats
       across archive years. coalesce() tries each format in order and returns
       the first non-null result. Rows where no format matches are dropped by
       the isNotNull() filter at the end.

    3. Temperature casting — TX_DRYB_10 and TN_DRYB_10 are cast from string to
       double. An explicit when() guard treats empty strings as null before
       casting; without this, Spark would silently produce null anyway but the
       intent is made explicit to prevent confusion.

    Args:
        raw_df:  Raw string DataFrame from silver/load.py.
        station: Station identifier to keep (e.g. '260_T_a' for De Bilt).

    Returns:
        Spark DataFrame with columns [date, tx, tn], nulls dropped on date.
    """
    return (
        raw_df
        .filter(F.trim(F.col("LOCATION")) == station)
        .withColumn(
            "date",
            F.coalesce(
                F.to_date(F.try_to_timestamp(F.col("DTG"), F.lit("yyyy-MM-dd HH:mm:ss"))),
                F.to_date(F.try_to_timestamp(F.col("DTG"), F.lit("yy-MM-dd HH:mm:ss"))),
                F.to_date(F.try_to_timestamp(F.col("DTG"), F.lit("yyyyMMddHHmmss"))),
            )
        )
        .withColumn("tx", F.when(F.col("TX_DRYB_10") != "", F.col("TX_DRYB_10").cast("double")))
        .withColumn("tn", F.when(F.col("TN_DRYB_10") != "", F.col("TN_DRYB_10").cast("double")))
        .filter(F.col("date").isNotNull())
        .select("date", "tx", "tn")
    )
