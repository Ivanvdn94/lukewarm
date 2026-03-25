from pyspark.sql import functions as F


def filter_station(raw_df, station: str):
    """
    Keep only rows for the given station, parse the timestamp to a date,
    and cast TX_DRYB_10 (max temp) and TN_DRYB_10 (min temp) to double.
    Empty strings are treated as null rather than causing a cast failure.
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
