from pyspark.sql import functions as F


def format_output(waves_df, wave_type: str):
    """Order by start date and format columns for display."""
    special_label = "Number of tropical days"   if wave_type == "heatwave" else "Number of high frost days"
    extreme_label = "Max temperature"           if wave_type == "heatwave" else "Min temperature"

    return (
        waves_df
        .orderBy("from_date")
        .select(
            F.date_format("from_date", "d MMM yyyy").alias("From date"),
            F.date_format("to_date",   "d MMM yyyy").alias("To date (inc.)"),
            F.col("duration").alias("Duration (in days)"),
            F.round("special_days", 0).cast("int").alias(special_label),
            F.round("extreme_temp", 1).alias(extreme_label),
        )
    )
