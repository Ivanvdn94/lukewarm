from pyspark.sql import functions as F


def daily_aggregate(station_df):
    """Reduce 10-minute readings to one row per day: max tx and min tn."""
    return (
        station_df
        .groupBy("date")
        .agg(
            F.max("tx").alias("daily_max"),
            F.min("tn").alias("daily_min"),
        )
        .orderBy("date")
    )
