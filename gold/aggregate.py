"""
Gold layer — daily aggregation.

The KNMI archive contains one observation record every 10 minutes (~144 rows
per day per station). Before wave detection can be applied, all readings for
the same calendar day must be collapsed to a single row containing the day's
peak maximum temperature and overnight minimum temperature.
"""

from pyspark.sql import functions as F


def daily_aggregate(station_df):
    """
    Collapse 10-minute readings to one row per calendar day.

    Groups all rows by date and computes:
      daily_max — highest tx value recorded that day (used to identify hot/freezing days)
      daily_min — lowest tn value recorded that day (used to identify high-frost nights)

    Null tx or tn values are ignored by Spark's max/min aggregations, so days
    with partial data still produce a valid row.

    Args:
        station_df: Typed DataFrame from silver/filter.py with columns [date, tx, tn].

    Returns:
        Spark DataFrame with columns [date, daily_max, daily_min], ordered by date.
    """
    return (
        station_df
        .groupBy("date")
        .agg(
            F.max("tx").alias("daily_max"),
            F.min("tn").alias("daily_min"),
        )
        .orderBy("date")
    )
