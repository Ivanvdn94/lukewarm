"""
Pytest configuration — shared fixtures for the test suite.

The SparkSession fixture is scoped to the full test session so that Spark is
started once and reused across all tests. Starting a new SparkSession per test
would add ~10 seconds of JVM startup overhead per test.

Windows requires winutils.exe for Spark to access the local filesystem.
The HADOOP_HOME block here mirrors the same check in heatwave.py so the tests
run correctly on both Windows and Linux (Docker) without any manual setup.
"""

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
    Provide a single SparkSession for the entire test run.

    Configured for minimal resource usage:
      local[1]                    — single-threaded, no parallelism overhead
      shuffle.partitions=1        — avoids unnecessary partition fan-out on small test data
      timeParserPolicy=CORRECTED  — matches the production SparkSession config so
                                    date parsing behaves identically in tests and in prod
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
