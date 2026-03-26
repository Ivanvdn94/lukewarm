"""
Gold layer — heatwave and coldwave detection.

Applies the official KNMI meteorological definitions using a gaps-and-islands
algorithm implemented with Spark window functions.

Gaps-and-islands technique
--------------------------
For a sequence of qualifying days, assign each day a row number (rn) ordered
by date. Then compute:

    group_id = datediff(date, reference_date) - rn

For truly consecutive days, datediff increases by 1 each day and rn increases
by 1 each day, so group_id stays constant. Any gap in the qualifying days
breaks the sequence: datediff jumps by more than 1 while rn only advances by 1,
producing a different group_id. Grouping by group_id therefore isolates each
consecutive run into its own island.

KNMI definitions
----------------
Heatwave : >= 5 consecutive days with daily_max >= 25°C,
           of which >= 3 days with daily_max >= 30°C  (tropical days)

Coldwave : >= 5 consecutive days with daily_max < 0°C,
           of which >= 3 days with daily_min < -10°C  (high-frost days)
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def detect_heatwaves(daily_df):
    """
    Detect heatwaves in a daily temperature DataFrame.

    Filters to days where daily_max >= 25°C, groups consecutive runs using the
    gaps-and-islands technique, then keeps only groups that meet both KNMI
    thresholds: duration >= 5 days and at least 3 tropical days (max >= 30°C).

    Args:
        daily_df: DataFrame from gold/aggregate.py with columns
                  [date, daily_max, daily_min].

    Returns:
        Spark DataFrame with one row per detected heatwave:
          from_date    (DateType)  — first day of the wave
          to_date      (DateType)  — last day of the wave
          duration     (LongType)  — number of qualifying days
          special_days (LongType)  — number of tropical days (max >= 30°C)
          extreme_temp (DoubleType)— highest daily_max recorded during the wave
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
    Detect coldwaves in a daily temperature DataFrame.

    Filters to days where daily_max < 0°C, groups consecutive runs using the
    gaps-and-islands technique, then keeps only groups that meet both KNMI
    thresholds: duration >= 5 days and at least 3 high-frost days (min < -10°C).

    Args:
        daily_df: DataFrame from gold/aggregate.py with columns
                  [date, daily_max, daily_min].

    Returns:
        Spark DataFrame with one row per detected coldwave:
          from_date    (DateType)  — first day of the wave
          to_date      (DateType)  — last day of the wave
          duration     (LongType)  — number of qualifying days
          special_days (LongType)  — number of high-frost days (min < -10°C)
          extreme_temp (DoubleType)— lowest daily_min recorded during the wave
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
