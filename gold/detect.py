from pyspark.sql import functions as F
from pyspark.sql.window import Window


def detect_heatwaves(daily_df):
    """
    Gaps-and-islands: consecutive days with daily_max >= 25°C share the same group_id.
    A heatwave requires duration >= 5 and at least 3 tropical days (max >= 30°C).
    """
    w = Window.orderBy("date")
    hot_days = (
        daily_df
        .filter(F.col("daily_max") >= 25)
        .withColumn("is_tropical", (F.col("daily_max") >= 30).cast("int"))
        .withColumn("rn", F.row_number().over(w))
        .withColumn("group_id", F.datediff(F.col("date"), F.lit("2003-01-01")) - F.col("rn"))
    )
    return (
        hot_days
        .groupBy("group_id")
        .agg(
            F.min("date").alias("from_date"),
            F.max("date").alias("to_date"),
            F.count("*").alias("duration"),
            F.sum("is_tropical").alias("special_days"),
            F.max("daily_max").alias("extreme_temp"),
        )
        .filter((F.col("duration") >= 5) & (F.col("special_days") >= 3))
    )


def detect_coldwaves(daily_df):
    """
    Gaps-and-islands: consecutive days with daily_max < 0°C share the same group_id.
    A coldwave requires duration >= 5 and at least 3 high-frost days (min < -10°C).
    """
    w = Window.orderBy("date")
    cold_days = (
        daily_df
        .filter(F.col("daily_max") < 0)
        .withColumn("is_high_frost", (F.col("daily_min") < -10).cast("int"))
        .withColumn("rn", F.row_number().over(w))
        .withColumn("group_id", F.datediff(F.col("date"), F.lit("2003-01-01")) - F.col("rn"))
    )
    return (
        cold_days
        .groupBy("group_id")
        .agg(
            F.min("date").alias("from_date"),
            F.max("date").alias("to_date"),
            F.count("*").alias("duration"),
            F.sum("is_high_frost").alias("special_days"),
            F.min("daily_min").alias("extreme_temp"),
        )
        .filter((F.col("duration") >= 5) & (F.col("special_days") >= 3))
    )
