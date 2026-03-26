"""
Silver layer — data loading.

Reads the CSV files produced by the Bronze layer into a single Spark DataFrame.
All values are loaded as strings (inferSchema=false) so that the Silver filter
step retains full control over type casting and null handling.
"""


def load(spark, extract_dir: str):
    """
    Read all CSV files in extract_dir into one Spark DataFrame.

    Spark reads all files in the directory in parallel and unions them into a
    single DataFrame. Schema inference is deliberately disabled — every column
    arrives as a string so that filter_station() can cast types explicitly and
    handle malformed values without interference from Spark's inferred types.

    Options:
        header=true      — First row of each CSV is treated as the column header.
        inferSchema=false — All columns remain StringType; casting is done in Silver.
        nullValue=""      — Empty strings are stored as null from the start.
        mode=PERMISSIVE  — Malformed rows are kept (with nulls) rather than dropped,
                           preserving visibility of data quality issues.

    Args:
        spark:       Active SparkSession.
        extract_dir: Local directory containing the Bronze-layer CSV files.

    Returns:
        Spark DataFrame with all monthly CSV files combined, all columns as StringType.
    """
    spark_path = "file:///" + extract_dir.replace("\\", "/")
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .csv(spark_path)
    )
