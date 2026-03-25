def load(spark, extract_dir: str):
    """Read all CSVs in extract_dir into a Spark DataFrame in parallel."""
    spark_path = "file:///" + extract_dir.replace("\\", "/")
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .csv(spark_path)
    )
