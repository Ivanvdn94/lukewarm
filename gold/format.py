"""
Gold layer — output formatting.

Transforms the raw detection results into a human-readable DataFrame suitable
for printing to the console. Column names become plain English, dates are
formatted as 'd MMM yyyy' (e.g. '1 Aug 2003'), and temperatures are rounded
to one decimal place.

This is the final step before results are displayed. No business logic lives
here — only presentation concerns.
"""

from pyspark.sql import functions as F


def format_output(waves_df, wave_type: str):
    """
    Format detected wave results for console display.

    Applies wave-type-specific column labels ('Number of tropical days' for
    heatwaves, 'Number of high frost days' for coldwaves), formats date columns
    as readable strings, rounds temperatures, and orders results chronologically.

    Args:
        waves_df:  DataFrame from detect_heatwaves() or detect_coldwaves().
        wave_type: Either 'heatwave' or 'coldwave' — controls column labels.

    Returns:
        Spark DataFrame with five display-ready columns, ordered by start date:
          From date            — start date as 'd MMM yyyy' string
          To date (inc.)       — end date as 'd MMM yyyy' string
          Duration (in days)   — integer count of qualifying days
          Number of tropical days / Number of high frost days — integer count
          Max temperature / Min temperature — rounded to 1 decimal place
    """
    special_label = "Number of tropical days"  if wave_type == "heatwave" else "Number of high frost days"
    extreme_label = "Max temperature"          if wave_type == "heatwave" else "Min temperature"

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
