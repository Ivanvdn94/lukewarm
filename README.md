# Heatwave Calculator – Netherlands

Calculates heatwaves and coldwaves in the Netherlands from 2003 onwards using 10-minute weather station data from KNMI, processed with Apache Spark.

---

## Definitions

| Wave type | Consecutive days | Special days |
|-----------|-----------------|--------------|
| Heatwave  | >= 5 days with daily max >= 25°C | >= 3 days with daily max >= 30°C (tropical days) |
| Coldwave  | >= 5 days with daily max < 0°C   | >= 3 days with daily min < -10°C (high frost days) |

---

## Output

```
2 heatwave(s) found:
+------------+---------------+------------------+-----------------------+---------------+
|From date   |To date (inc.) |Duration (in days)|Number of tropical days|Max temperature|
+------------+---------------+------------------+-----------------------+---------------+
|1 Aug 2003  |13 Aug 2003    |13                |7                      |35.0           |
+------------+---------------+------------------+-----------------------+---------------+
```

---

## Data Source

- **Location:** Azure Blob Storage (`gddassesmentdata.blob.core.windows.net/knmi-data/data.tgz`)
- **Access:** Time-limited SAS URL (read-only, expires 2026-12-31)
- **Format:** Single `.tgz` archive containing one fixed-width file per month, named `kis_tot_YYYYMM`
- **Station:** De Bilt is identified by `LOCATION = 260_T_a`
- **Key columns:**
  - `TX_DRYB_10` — maximum air temperature per 10-minute interval (°C)
  - `TN_DRYB_10` — minimum air temperature per 10-minute interval (°C)
- **Note:** File format (column positions, date format) varies across years

---

## Architecture

### Constraints

- No SQL-based solutions (no Hive, no Spark SQL, no `.sql()`)
- Must be horizontally scalable across multiple machines
- Must be easily extensible for other data processing tasks

### Pipeline Overview

```
[Azure Blob .tgz]
       │
       ▼
[Stage 1 – Extract]                ← driver only, sequential
       │  stream .tgz over HTTP
       │  detect column positions from "# DTG" header
       │  convert fixed-width → CSV
       │  write one CSV per month to local temp dir
       ▼
[Stage 2 – Load]                   ← Spark, parallel across workers
       │  spark.read.csv(temp_dir)
       │  all CSVs read in parallel
       ▼
[Stage 3 – Filter]                 ← Spark, distributed
       │  keep rows where LOCATION = 260_T_a (De Bilt)
       │  parse DTG string → date
       │  parse TX_DRYB_10 → double (max temp)
       │  parse TN_DRYB_10 → double (min temp)
       ▼
[Stage 4 – Aggregate]              ← Spark, distributed
       │  groupBy(date) → max(tx) as daily_max
       │                → min(tn) as daily_min
       ▼
[Stage 5 – Detect waves]           ← Spark, distributed
       │  gaps-and-islands algorithm
       │  group_id = datediff(date, anchor) - row_number
       │  consecutive qualifying days share the same group_id
       │  filter: duration >= 5 AND special_days >= 3
       ▼
[Stage 6 – Format output]
       │  orderBy(from_date)
       │  format dates as "d MMM yyyy"
       ▼
[Result table printed to console]
```

---

## Code Walkthrough

### Stage 1 – `fixed_width_to_csv()` and `extract()`

KNMI files are **fixed-width**, not CSV — each column occupies a fixed character range. The format also changes between years (different column order, different date format).

**How it works:**
1. Every file starts with comment lines (`#`). The line beginning with `# DTG` is the column header.
2. `fixed_width_to_csv` scans that header line character by character to find where each column name starts. Those positions become the slice boundaries for every data row.
3. Each data row is sliced using those boundaries and written as a standard CSV row.
4. `extract` streams the `.tgz` directly over HTTP — it never downloads the full 1GB archive. It only extracts the months you requested.

**Why dynamic column detection?**
Older files (pre-2005) have fewer columns and a different layout than newer files. Hard-coding positions would break silently on older data. Reading the header makes the parser format-agnostic.

```python
# The "# DTG" line tells us exactly where every column begins
if line.startswith("# DTG"):
    stripped = line[2:]   # remove the "# " prefix
    # walk character by character to find each column name's start position
    col_names, col_starts = [], []
    ...
```

---

### Stage 2 – `load()`

Spark reads all the CSVs in the temp directory **in parallel**. This is the point where single-machine extraction hands off to distributed processing.

```python
spark_path = "file:///" + extract_dir.replace("\\", "/")
spark.read.option("header", "true").csv(spark_path)
```

**Note on Windows:** Spark requires `winutils.exe` + `hadoop.dll` in `C:\hadoop\bin` to access the local filesystem. `heatwave.py` sets `HADOOP_HOME` automatically at startup, but only when running on Windows — inside a Linux Docker container this block is skipped entirely as it is not needed.

---

### Stage 3 – `filter_station()`

Keeps only rows from De Bilt (`260_T_a`) and parses the raw string columns into usable types.

**Date parsing challenge:** The date format changed across years:
- Recent files: `2019-07-01 00:10:00` → `yyyy-MM-dd HH:mm:ss`
- Older files: `03-07-01 00:10:00` → `yy-MM-dd HH:mm:ss`

`try_to_timestamp` is used instead of `to_timestamp` because Spark 4.x raises an exception (rather than returning null) when a format does not match. `coalesce` tries each format in order and takes the first non-null result.

**Temperature parsing challenge:** Empty strings (`""`) cannot be cast to `double`. `F.when(col != "", col.cast("double"))` safely returns null for missing readings instead of crashing.

```python
.withColumn("date", F.coalesce(
    F.to_date(F.try_to_timestamp(F.col("DTG"), F.lit("yyyy-MM-dd HH:mm:ss"))),
    F.to_date(F.try_to_timestamp(F.col("DTG"), F.lit("yy-MM-dd HH:mm:ss"))),
    F.to_date(F.try_to_timestamp(F.col("DTG"), F.lit("yyyyMMddHHmmss"))),
))
.withColumn("tx", F.when(F.col("TX_DRYB_10") != "", F.col("TX_DRYB_10").cast("double")))
.withColumn("tn", F.when(F.col("TN_DRYB_10") != "", F.col("TN_DRYB_10").cast("double")))
```

---

### Stage 4 – `daily_aggregate()`

The raw data has one row every 10 minutes (~144 rows per station per day). We need one row per day with the day's highest and lowest temperature reading.

```python
.groupBy("date")
.agg(
    F.max("tx").alias("daily_max"),
    F.min("tn").alias("daily_min"),
)
```

This distributes well — Spark shuffles rows by date and each worker independently aggregates its assigned dates.

---

### Stage 5 – `detect_heatwaves()` / `detect_coldwaves()`

The core challenge: find **consecutive** sequences of qualifying days.

#### The Gaps-and-Islands Algorithm

For a perfectly consecutive sequence of dates, the difference between the calendar date (as a number) and the row number (its position in the sorted list) is **constant**. Any gap in the date sequence breaks that constant — creating a new "island".

```
Date         datediff   row_number   group_id (diff − rn)
2003-08-01     212          1            211   ← island 1 starts
2003-08-02     213          2            211
2003-08-03     214          3            211
                                               ← gap: 2003-08-04 was below 25°C
2003-08-05     216          4            212   ← island 2 starts
2003-08-06     217          5            212
```

All rows with the same `group_id` are consecutive. A `groupBy(group_id)` then counts duration and special days per island.

```python
w = Window.orderBy("date")
hot_days = (
    daily_df
    .filter(F.col("daily_max") >= 25)            # keep qualifying days only
    .withColumn("rn", F.row_number().over(w))    # position in sorted sequence
    .withColumn("group_id",
        F.datediff(F.col("date"), F.lit("2003-01-01")) - F.col("rn"))
)
hot_days.groupBy("group_id").agg(
    F.count("*").alias("duration"),
    F.sum("is_tropical").alias("special_days"),
    ...
).filter((F.col("duration") >= 5) & (F.col("special_days") >= 3))
```

**Why `F.lit("2003-01-01")` as the anchor?** `datediff` converts a date to an integer (days since the anchor). The anchor itself does not matter — any fixed date produces the same relative differences and therefore the same grouping. `2003-01-01` is just the start of the dataset.

**Coldwave differences from heatwave:**
- Qualifying day: `daily_max < 0` instead of `>= 25`
- Special day: `daily_min < -10` instead of `daily_max >= 30`
- Extreme temperature reported as `min(daily_min)` instead of `max(daily_max)`

---

### Stage 6 – `format_output()`

Orders results by start date (ascending) and formats date columns into human-readable strings. Column labels are parameterised by `wave_type`.

```python
.orderBy("from_date")   # must happen before date_format — strings sort alphabetically
.select(
    F.date_format("from_date", "d MMM yyyy").alias("From date"),
    ...
)
```

`orderBy` is applied *before* `date_format` because once dates become strings (`"1 Aug 2003"`), alphabetical sort no longer gives chronological order.

---

## Architectural Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| Processing engine | PySpark DataFrame API | Horizontally scalable; no SQL |
| Intermediate format | CSV on local disk | Spark reads natively without Py4J overhead; human-readable for debugging |
| Fixed-width parsing | Dynamic column detection from `# DTG` header | Handles format changes across years automatically |
| Date parsing | `try_to_timestamp` + `coalesce` | Multiple date formats per year; Spark 4.x throws on mismatch instead of returning null |
| Temperature cast | `F.when(col != "", col.cast(...))` | Empty strings in fixed-width files crash a direct `.cast("double")` |
| Consecutive day detection | Gaps-and-islands (`datediff − row_number`) | Pure DataFrame API, no SQL, horizontally scalable |
| Temperature scale | Already in °C (no scaling) | Confirmed from file headers and field descriptions |
| Windows compatibility | `HADOOP_HOME` via `os.environ` in-process | No need to set environment variable before launching Python |
| CLI | `click` group + shared `date_options` decorator | Clean subcommand separation; avoids repeating the same four options |

---

## Tradeoffs

### Extraction is sequential on the driver

The `.tgz` is a single sequential stream — files inside must be read in order. There is no way to give multiple workers independent access to individual months without first extracting them to shared storage.

**Impact:** On a real cluster, one machine does all the download and extraction work before Spark workers can begin. For 22 years of data this is a meaningful bottleneck.

**Mitigation:** Pre-extract the archive to Azure Blob / HDFS / S3 once. Workers can then read individual files directly. No code changes required — only configuration.

### Local filesystem is not cluster-compatible

Spark workers read CSVs from the driver's local temp directory. In local mode (single machine) this works fine. On a multi-machine cluster, workers on other nodes cannot access the driver's filesystem.

**Mitigation:** Point `spark.read.csv()` at shared storage. `spark.read.csv("abfss://...")` or `spark.read.csv("s3://...")` works identically to `spark.read.csv("file:///...")`.

### CSV as intermediate format

Writing CSVs adds a disk I/O round-trip. Parquet would be faster to read and takes less space.

**Why CSV here:** At development scale the difference is negligible, and CSV is human-readable — easy to inspect when debugging format issues. Switching to Parquet is a one-line change once shared storage is in place.

### `mapPartitions` was explored and abandoned

`heatwave_v2.py` attempted to push parsing to Spark workers via `mapPartitions`. It failed in local mode because:
1. Serializing 167MB raw file strings through the Py4J bridge (Python ↔ JVM channel) crashed Python workers with OOM.
2. Serializing millions of parsed row tuples through Py4J caused `EOFException`.

`spark.read.csv()` avoids this entirely — Spark workers read files natively, never sending data through Python.

`mapPartitions` *is* architecturally correct for a real cluster (workers fetch from shared storage without going through the driver), but is not viable in local mode.

---

## Possible Improvements

| Improvement | Benefit | Effort |
|-------------|---------|--------|
| Pre-extract `.tgz` to shared storage (Blob/S3/HDFS) | Workers parse in parallel; removes sequential bottleneck | Medium |
| Replace CSV temp files with Parquet | Faster reads, schema enforcement, compression | Low — change `spark.read.csv` to `spark.read.parquet` |
| Add `requirements-dev.txt` and Docker test stage | Run tests inside the container as part of CI | Low |
| Parameterise station ID via `--station` CLI option | Detect waves for any KNMI station, not just De Bilt | Low |
| Add `--output csv` / `--output json` flag | Machine-readable output for downstream pipelines | Low |
| Cache `daily_df` with `.cache()` | Avoids re-aggregating if pipeline is extended to run both wave types in one session | Low |
| Cluster deployment config | Separate cluster settings (master URL, storage connector) from pipeline logic | Medium |

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `pyspark` | Distributed data processing |
| `requests` | Streaming download of blob |
| `click` | CLI framework |
| Java 21 | Required by PySpark's JVM |
| `winutils.exe` + `hadoop.dll` | Required for Spark to access local filesystem on Windows only |

---

## Running

### Option A — Docker (recommended, no local setup required)

Install [Docker Desktop](https://www.docker.com/products/docker-desktop/), then:

```bash
# Build the image (once, or after code changes)
docker build -t heat-cold .

# Heatwave detection
docker run heat-cold heatwave --start-year 2003 --start-month 7 --end-year 2003 --end-month 9

# Coldwave detection
docker run heat-cold coldwave --start-year 2012 --end-year 2012

# Show help
docker run heat-cold
```

Everything — Java, PySpark, and all Python packages — is installed inside the image. No local configuration needed.

### Option B — Run locally

**1. Install Java 21**
```bash
winget install Microsoft.OpenJDK.21
```

**2. Install Python dependencies**
```bash
pip install -r requirements.txt
```

**3. Windows only — install winutils**
- Place `winutils.exe` and `hadoop.dll` in `C:\hadoop\bin`
- Download from [cdarlint/winutils](https://github.com/cdarlint/winutils) — match the Hadoop version bundled with your PySpark version

**4. Run**
```bash
# Show available commands
python heatwave.py --help

# Heatwave detection
python heatwave.py heatwave --start-year 2003 --start-month 7 --end-year 2003 --end-month 9
python heatwave.py heatwave --start-year 2003 --end-year 2025

# Coldwave detection
python heatwave.py coldwave --start-year 2010 --start-month 1 --end-year 2012 --end-month 3
python heatwave.py coldwave --start-year 2003 --end-year 2025
```

### Test ranges (known events)

| Range | Expected result |
|-------|----------------|
| 2003-07 → 2003-09 | Aug 2003 heatwave (~35°C) |
| 2019-07 → 2019-07 | Jul 2019 heatwave (record 40.7°C) |
| 2006-07 → 2006-08 | Jul 2006 heatwave (~36°C) |
| 2010-01 → 2010-03 | Known cold period in the Netherlands |
| 2012-02 → 2012-02 | Feb 2012 cold spell |

---

## v2 Reference — `mapPartitions` experiment (`heatwave_v2.py`)

An alternative approach that pushes parsing to Spark workers. Correct for production; fails in local mode.

```
[Azure Blob .tgz]
       │
       ▼
[Driver: stream & collect raw content]    ← sequential (single .tgz constraint)
       │  tarfile streaming
       │  returns list of raw strings (one per month)
       ▼
[sc.parallelize(monthly_contents)]        ← distribute to workers
       │  one partition per month
       ▼
[mapPartitions: parse_partition()]        ← parallel across workers
       │  each worker detects column positions and parses its months
       │  yields (DTG, LOCATION, TX_DRYB_10) tuples
       ▼
[spark.createDataFrame(rows_rdd, SCHEMA)] ← explicit schema, no inference
       │
       ▼ (same pipeline as v1 from here)
[filter_station()]  →  [daily_aggregate()]  →  [detect_*waves()]  →  [format_output()]
```

**Why abandoned locally:** The Py4J bridge is not built for moving 167MB objects. In production, workers never receive large payloads from the driver — they fetch from shared storage directly. `mapPartitions` remains the right architecture for a deployed cluster.
