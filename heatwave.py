import os
import platform
import shutil
import tempfile
import click

# winutils.exe is only needed on Windows — not required inside a Linux container
if platform.system() == "Windows":
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession

from config import DE_BILT
from bronze.extract import extract
from silver.load import load
from silver.filter import filter_station
from gold.aggregate import daily_aggregate
from gold.detect import detect_heatwaves, detect_coldwaves
from gold.format import format_output

def run(wave_type: str, start_year: int, start_month: int, end_year: int, end_month: int):
    target_months = {
        f"{y}{m:02d}"
        for y in range(start_year, end_year + 1)
        for m in range(1, 13)
        if (y, m) >= (start_year, start_month) and (y, m) <= (end_year, end_month)
    }

    """The two if conditions use tuple comparison — Python compares tuples element by element, so (2003, 7) >= (2003, 7) is True and (2003, 6) >= (2003, 7) is False. This handles year boundaries correctly — e.g. a range from 2003-11 to 2004-02 correctly includes Nov, Dec, Jan, Feb without any special casing.
        m:02d is a format specifier meaning "integer, minimum 2 digits, zero-padded" — so month 7 becomes "07" not "7", matching the filename kis_tot_200307 in the archive.
    """

    spark = (
        SparkSession.builder
        .appName("Heatwave" if wave_type == "heatwave" else "Coldwave")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    extract_dir = tempfile.mkdtemp(prefix="knmi_")
    try:
        extract(extract_dir, target_months)

        if not os.listdir(extract_dir):
            print("No files found for the selected date range.")
            return

        raw_df     = load(spark, extract_dir)
        station_df = filter_station(raw_df, DE_BILT)
        daily_df   = daily_aggregate(station_df)

        waves_df = detect_heatwaves(daily_df) if wave_type == "heatwave" else detect_coldwaves(daily_df)
        result   = format_output(waves_df, wave_type)
        count    = result.count()

        if count == 0:
            print(f"\nNo {wave_type}s found in the selected date range.")
        else:
            print(f"\n{count} {wave_type}(s) found:")
            result.show(truncate=False)

    finally:
        shutil.rmtree(extract_dir)
        spark.stop()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def date_options(f):
    """Shared click options for start/end year and month."""
    f = click.option("--end-month",   default=12, type=click.IntRange(1, 12), show_default=True, help="End month 1–12")(f)
    f = click.option("--end-year",    required=True, type=int, help="End year (e.g. 2025)")(f)
    f = click.option("--start-month", default=1,  type=click.IntRange(1, 12), show_default=True, help="Start month 1–12")(f)
    f = click.option("--start-year",  required=True, type=int, help="Start year (e.g. 2003)")(f)
    return f


@click.group()
def cli():
    """KNMI wave calculator — detects heatwaves and coldwaves for De Bilt."""
    pass


@cli.command()
@date_options
def heatwave(start_year, start_month, end_year, end_month):
    """Detect heatwaves: >= 5 days max >= 25°C, of which >= 3 days max >= 30°C."""
    if (end_year, end_month) < (start_year, start_month):
        raise click.BadParameter("End date must be after start date.")
    run("heatwave", start_year, start_month, end_year, end_month)


@cli.command()
@date_options
def coldwave(start_year, start_month, end_year, end_month):
    """Detect coldwaves: >= 5 days max < 0°C, of which >= 3 days min < -10°C."""
    if (end_year, end_month) < (start_year, start_month):
        raise click.BadParameter("End date must be after start date.")
    run("coldwave", start_year, start_month, end_year, end_month)


if __name__ == "__main__":
    cli()
