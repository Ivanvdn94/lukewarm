import os
import platform
import pytest
from pyspark.sql import SparkSession

# winutils.exe is required for Spark to access the local filesystem on Windows
if platform.system() == "Windows":
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")


@pytest.fixture(scope="session")
def spark():
    """
    Single SparkSession shared across all tests.
    scope="session" means it is created once and reused — creating a new
    SparkSession per test would add ~10s overhead per test.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
