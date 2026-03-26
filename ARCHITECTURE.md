# Architecture Overview

## Overview

This is a **weather event detection pipeline** built on **PySpark** that ingests KNMI (Royal Dutch Meteorological Institute) hourly/10-minute weather data from Azure Blob Storage and identifies historical **heatwaves** and **coldwaves** for the De Bilt weather station in the Netherlands. It is structured around the **Medallion Architecture** — a widely-used data engineering pattern.

---

## Architecture: The Medallion Pattern

The folder structure directly maps to a three-tier data quality model:

```
Bronze  →  Raw ingestion (just get the data in)
Silver  →  Cleaned & filtered (make it trustworthy)
Gold    →  Business logic (make it useful)
```

This separation is a core data engineering principle. Each layer has one responsibility, which makes the pipeline easier to test, debug, and extend. You can swap out the bronze layer (e.g. change the data source) without touching the detection logic in gold.

---

## Data Flow

```
Azure Blob (.tgz)
       ↓
[Bronze] Stream & filter archive → parse fixed-width → write CSVs to temp dir
       ↓
[Silver] Spark reads CSVs → filter to De Bilt → parse dates → cast temperatures
       ↓
[Gold]  Aggregate to daily max/min → gaps-and-islands detection → format output
       ↓
CLI output: table of wave events
```

---

## Why PySpark

### The short answer

PySpark was chosen so that the processing code does not need to change when the scale of data or infrastructure changes. The same DataFrame operations that run on a laptop in local mode will run unchanged on a 100-node cluster.

### The longer answer — what this means in practice

Spark processes data as **distributed columns, not rows**. This has a direct consequence for how every function in Silver and Gold is written:

**No Python loops over rows.** A Python `for row in df` loop runs entirely on the driver machine — it cannot be distributed. It also defeats the JVM optimisations Spark applies to DataFrame operations. Every transformation in this pipeline uses DataFrame methods (`filter`, `groupBy`, `withColumn`, `agg`) that Spark can optimise, distribute, and run in parallel.

**No User-Defined Functions (UDFs) unless necessary.** Python UDFs break out of the Spark execution engine, serialise each row to Python, process it, and serialise back. This is significantly slower than native DataFrame operations. All transformations here use built-in Spark functions from `pyspark.sql.functions` — they stay inside the JVM and benefit from Spark's Catalyst query optimiser.

**The gaps-and-islands algorithm is the clearest example.** Finding consecutive sequences in data is naturally expressed as a loop in Python:

```python
# Python loop — cannot be distributed
streak = 0
for row in days:
    if row.temp >= 25:
        streak += 1
    else:
        streak = 0
```

Instead, the algorithm uses `row_number()` and `datediff()` — two window functions that operate on entire columns at once and can be distributed across a cluster:

```python
# Spark window function — fully distributable
.withColumn("rn", F.row_number().over(w))
.withColumn("group_id", F.datediff(F.col("date"), F.lit("2003-01-01")) - F.col("rn"))
```

This is not just a performance choice — it is the only way to express the logic in a form Spark can execute.

### Why inferSchema=false is required

`inferSchema=true` makes Spark scan all files twice — once to guess types, once to read data. More importantly, the KNMI temperature columns contain a mix of numeric strings (`"285"`) and empty strings (`""`). Spark's guess would be inconsistent. Silver's `filter_station()` knows exactly what types are needed and handles empty strings deliberately as nulls. Loading as strings gives Silver full control with no interference.

### Why shuffle.partitions is set to 8

The default is 200 partitions after any shuffle operation (groupBy, join, etc.). With a small local dataset, 200 partitions means 200 tasks doing almost no work each — more scheduling overhead than actual computation. Setting it to 8 matches the data volume. In production with a large cluster and many months of data, this would be raised accordingly.

### Why coalesce() is used for date parsing

The DTG column has three different timestamp formats across the archive's history. Spark's `coalesce()` tries each format in order and returns the first non-null result — exactly like a try/fallthrough. The `timeParserPolicy=CORRECTED` setting is required for this to work: without it, Spark 3.0+ throws an exception on a format mismatch instead of returning null, which breaks the coalesce fallback chain entirely.

---

## Why We Stream the Data

### What streaming means here

`requests` is used in streaming mode — the HTTP response body is piped directly into `tarfile` without downloading the file to disk first. The archive is read as a sequential byte stream.

### Why streaming was chosen

The KNMI archive is one large `.tgz` file containing monthly data going back to the 1950s. Loading it entirely into memory before processing would require buffering potentially gigabytes of data. Streaming processes each tar member as it arrives, keeping memory usage constant regardless of archive size.

### The fundamental constraint of gzip compression

Streaming is not just a design choice — for a `.tgz` it is the only viable option. Gzip compression is sequential: the compressed bytes for month 200307 depend on all the bytes that came before them. There is no internal index. Even if the full file were downloaded to disk first, you would still need to decompress from the beginning to reach any given month. Downloading first would add disk I/O on top of the sequential read — it would be strictly slower with no benefit.

```
Request months: 200307, 200308 only

Stream reads:   1951-01 skip → 1951-02 skip → ... → 2003-07 extract → 2003-08 extract → ...continues
```

### Why thread pooling does not help

Thread pooling lets multiple downloads happen simultaneously. That requires multiple independent things to download. Here there is one stream — it cannot be split into parallel chunks without seeking, and seeking is not possible in a gzip stream. Adding more threads has no effect on a single sequential read.

### The honest tradeoff

The streaming approach means every run re-reads the full archive from the start. This is the biggest performance bottleneck in the pipeline. The fix is structural: move Bronze extraction to the cloud (an Airflow job running monthly) so that individual Parquet files per month are pre-built and independently accessible. The Docker pipeline then reads only the partitions it needs. See `docs/cloud_usage.md` for the full hybrid architecture.

---

## Why Docker

### The dependency problem

Running PySpark locally requires three separate things to be installed and correctly configured:

1. **Python** — specifically 3.11. PySpark 3.5.x does not support Python 3.12 or 3.13. A user with a newer Python version will get a cryptic JVM error, not a clear version mismatch message.
2. **Java** — a JRE must be present and on the system PATH. Spark runs on the JVM; without Java, it will not start.
3. **winutils.exe on Windows** — Spark uses Hadoop's filesystem abstraction even in local mode. On Windows, it needs a small Windows binary (`winutils.exe`) placed in a specific location (`C:\hadoop\bin`). Without it, any file operation fails with an obscure error about a missing native library.

Docker eliminates all three problems by packaging the exact correct versions of everything into one image.

### What the image contains

```
python:3.11-slim          ← exact Python version, minimal OS footprint
OpenJDK 21                ← Java runtime, installed and configured
PySpark 3.5.1             ← exact version, pre-installed
JAVA_HOME set correctly   ← no manual PATH configuration needed
No winutils needed        ← Linux container has native filesystem access
```

A user with Docker installed runs one command. The environment is identical on every machine it runs on.

### Layer caching — why the Dockerfile is ordered the way it is

Docker builds images in layers. Each layer is cached. If nothing in a layer changed since the last build, Docker skips it entirely. The Dockerfile deliberately orders layers from least-changed to most-changed:

```
[1] python:3.11-slim base image    ← pulled once, cached forever
[2] apt-get Java                   ← only reruns if the apt command changes
[3] WORKDIR                        ← never changes
[4] COPY requirements.txt          ← only changes when dependencies change
    pip install                    ← the slow step — ~3 min for PySpark
[5] COPY . .                       ← changes on every code edit
[6] ENTRYPOINT                     ← never changes
```

If `requirements.txt` and the application code were copied in the same step, any `.py` file edit would invalidate the pip cache and force a full PySpark reinstall (~3 minutes) on every build. Splitting them means code edits only rerun step 5 — which takes seconds.

### The platform.system() check

The `HADOOP_HOME` environment variable setup in `heatwave.py` and `conftest.py` is wrapped in a platform check:

```python
if platform.system() == "Windows":
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
```

Inside a Linux container, Spark has native filesystem access and does not need winutils. The check ensures the same codebase runs on Windows (local development) and Linux (Docker) without modification.

### The timeParserPolicy setting

Spark 3.0 changed how datetime parsing failures are handled. Previously, a format mismatch returned null. From 3.0 onwards, it throws a `SparkUpgradeException` and aborts the job. The KNMI archive contains two-digit year timestamps (`03-08-01 00:10:00`) in older files. Without the policy override, the pipeline crashes on these. Setting `CORRECTED` restores null-on-mismatch, which is what `coalesce()` in `filter_station()` depends on.

This setting must appear in both `heatwave.py` (for the real pipeline) and `tests/conftest.py` (for the test SparkSession) — otherwise tests pass locally but the pipeline fails, or vice versa.

---

## Entry Point: heatwave.py

This is the **orchestrator and CLI**. It wires all layers together.

**CLI Design — `click`**

The app uses the `click` library to expose two commands: `heatwave` and `coldwave`. A shared decorator `date_options` applies the date range arguments to both commands — avoiding duplication (DRY principle).

```
python heatwave.py heatwave --start-year 2003 --start-month 7 --end-year 2003 --end-month 9
python heatwave.py coldwave --start-year 1997 --start-month 1 --end-year 1997 --end-month 3
```

**Target Month Set**

A set comprehension computes exactly which months are needed before any I/O happens. Using a `set` gives O(1) lookup when filtering the archive stream, and prevents duplicate months. Tuple comparison (`(y, m) >= (start_year, start_month)`) handles year boundaries correctly — a range from November 2003 to February 2004 works without any special December/January logic.

**Temp Directory as a Clean-Room**

`tempfile.mkdtemp()` creates a fresh, isolated working directory per run. The `try/finally` block guarantees cleanup via `shutil.rmtree()` even if the pipeline crashes mid-run — preventing leftover files accumulating across runs.

**Spark Session**

- `spark.sql.shuffle.partitions = 8` — the default (200) causes unnecessary overhead for small/local data
- `timeParserPolicy = CORRECTED` — required for coalesce-based date parsing to work across all archive years
- `setLogLevel("ERROR")` — silences Spark's verbose INFO logs so only meaningful output reaches the user

---

## Bronze Layer: bronze/extract.py

**Responsibility:** Get raw data out of remote storage and land it locally as CSVs.

**Streaming the Archive**

The source is a single `.tgz` file on Azure Blob Storage. The code uses `requests` in streaming mode and opens the tarfile directly against the HTTP response stream via `mode="r|gz"` (pipe = streaming tar). This processes entries sequentially without buffering the entire archive into memory first.

Only files matching `kis_tot_` with a month suffix in the target set are extracted. Everything else is skipped mid-stream — **early filtering at the source** (push predicates as close to the data as possible).

**Fixed-Width to CSV Conversion**

Raw KNMI files use a fixed-width format common in meteorological and government data. Column positions are detected **dynamically from the `# DTG` header line** rather than being hardcoded. KNMI changed their file format across years, so hardcoded offsets would silently break on older or newer data. Dynamic detection makes the parser resilient to format drift.

Parsed data is immediately written as CSV, which is Spark-friendly and enables parallel reads in the next stage.

---

## Silver Layer

### silver/load.py

**Responsibility:** Read the landing CSVs into a distributed Spark DataFrame.

`spark.read.csv()` is used with `inferSchema=false` — all columns come in as strings. This avoids schema inference (which requires a full double-scan of the data) and gives explicit control over type casting downstream. `mode=PERMISSIVE` keeps malformed rows with nulls rather than crashing the job — pragmatic for real-world messy data.

### silver/filter.py

**Responsibility:** Clean, validate, and narrow the data to what matters.

Three things happen in sequence:

1. **Station filter** — only rows for `260_T_a` (De Bilt) are kept. `F.trim()` handles trailing whitespace common in fixed-width-derived data.
2. **Timestamp parsing** — the `DTG` field has inconsistent formats across years. `F.coalesce()` tries three timestamp patterns in order, taking the first non-null result. This is **schema-on-read** for a field with format drift.
3. **Type casting** — `TX_DRYB_10` (max temp) and `TN_DRYB_10` (min temp) are cast from string to double. The `F.when(col != "")` guard treats empty strings as null rather than causing cast failures. Rows where `date` is null are dropped — they are unrecoverable.

Only `date`, `tx`, and `tn` are selected going forward. This **projection pushdown** reduces memory pressure and shuffle cost for the rest of the pipeline.

---

## Gold Layer

### gold/aggregate.py

**Responsibility:** Collapse 10-minute observations into one summary row per day.

The raw KNMI data contains multiple readings per day (10-minute intervals). A `groupBy("date").agg(max("tx"), min("tn"))` reduces this to a single daily record — the day's peak temperature and overnight minimum — which is the granularity the meteorological definitions operate on.

### gold/detect.py

**Responsibility:** Identify sequences of days that meet the heatwave or coldwave criteria.

This uses the classic **gaps-and-islands** technique — a standard pattern for identifying consecutive sequences in time-series data.

**How gaps-and-islands works:**

For a given wave type, only qualifying days are kept (e.g. `daily_max >= 25` for heatwaves). A sequential `row_number()` is assigned to those days. Subtracting `rn` from `datediff(date, anchor)` produces a `group_id` that is **identical for consecutive qualifying days** and changes the moment a gap appears.

```
Date        datediff  rn   group_id
2003-08-01     212     1     211    ← run starts
2003-08-02     213     2     211    ← same group
2003-08-03     214     3     211    ← same group
(non-qualifying day — excluded, rn does not advance)
2003-08-05     216     4     212    ← new group (datediff jumped by 2, rn by 1)
```

After grouping by `group_id`, each island becomes one row with start date, end date, duration, and a count of "special" days. A final filter applies the official KNMI criteria:

- **Heatwave**: >= 5 consecutive days with max >= 25°C, of which >= 3 days with max >= 30°C (tropical days)
- **Coldwave**: >= 5 consecutive days with max < 0°C, of which >= 3 days with min < -10°C (high-frost days)

Both functions share the same structural algorithm — only the thresholds and special-day definition differ. This symmetry allows both wave types to share the same downstream formatting logic.

### gold/format.py

**Responsibility:** Produce human-readable output.

Internal column names (`special_days`, `extreme_temp`) are generic enough to work for both wave types. The formatter applies the correct display labels based on `wave_type`, formats dates as `"1 Aug 2003"`, rounds temperatures to one decimal place, and orders results chronologically. Pure presentation logic, cleanly separated from detection logic.

---

## Supporting Files

### config.py

A **single source of truth** for shared constants: the Azure Blob URL and the De Bilt station identifier. Centralising these means a URL rotation or station change requires one edit, not a search-and-replace across files.

---

## Testing

### Structure

```
tests/
├── conftest.py          ← shared SparkSession fixture (created once per session)
├── test_filter.py       ← silver layer — null safety on temperature casting
├── test_detect.py       ← gold layer — core detection algorithm (3 unit tests)
└── test_integration.py  ← Silver → Gold end-to-end pipeline test
```

### Running Tests

| Command | What it does |
|---|---|
| `pytest` | Run all tests |
| `pytest -v` | Verbose — shows each test name pass/fail |
| `pytest --cov=. --cov-report=term-missing` | Run with coverage report |
| `pytest tests/test_detect.py` | Run a single file |

### What Each File Tests

**test_filter.py** (1 unit test)
- Empty string in TX column becomes null rather than crashing or silently producing 0 — the most common silent data bug in typed pipelines

**test_detect.py** (3 unit tests — most critical)
- Minimum valid heatwave detected (5 days, 3 tropical) — the core happy path
- Gap between two hot runs produces two separate waves, not one — validates the gaps-and-islands grouping
- Minimum valid coldwave detected (5 days max < 0, 3 high-frost nights) — the cold path of the algorithm

**test_integration.py** (1 integration test)
- Raw string rows fed through `filter_station` → `daily_aggregate` → `detect_heatwaves` end to end
- Confirms the layers connect correctly and a row for a different station is filtered out
- No network or files — fully in-memory

### Design Notes

- The `conftest.py` fixture uses `scope="session"` — a single SparkSession is created once and shared across all Spark tests. Creating one per test would add ~10 seconds per test.
- `test_detect.py` directly encodes the KNMI meteorological definitions and will catch any regression if thresholds or logic change.
- The integration test is the safety net — it proves the layers wire together correctly, which unit tests on isolated functions cannot verify.

---

## Docker

### Why Docker

The app has three non-trivial dependencies — Python, Java, and PySpark — plus a Windows-specific requirement for `winutils.exe`. Without Docker, every user must install and configure all of these at the correct versions. Docker bakes everything into a single image that runs identically on any machine with Docker installed.

### Dockerfile Structure

The Dockerfile has 6 layers, ordered deliberately from least-changed to most-changed:

```
[1/6] Pull python:3.11-slim          ← base OS + Python, never changes
[2/6] apt-get install Java 21        ← system dependency, rarely changes
[3/6] WORKDIR /app                   ← working directory, never changes
[4/6] COPY requirements.txt          ← only changes when dependencies change
      RUN pip install
[5/6] COPY . .                       ← changes every time code changes
[6/6] ENTRYPOINT                     ← never changes
```

**Layer caching** means Docker skips any step whose inputs have not changed since the last build. In practice, editing a `.py` file only reruns step 5 — the expensive Java install and pip install are cached and skipped. If `requirements.txt` were copied alongside the code in step 5, every code change would invalidate the pip cache and force a full PySpark reinstall (~3 minutes) on every build.

### Windows vs Linux

The `HADOOP_HOME` and `PATH` environment variables are only needed on Windows so Spark can find `winutils.exe`. Inside a Linux container, Spark has native filesystem access and these are not needed. A platform check in `heatwave.py` handles this:

```python
if platform.system() == "Windows":
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
```

This means the same codebase runs locally on Windows and inside Docker on Linux without any modification.

### Spark DateTime Parser Policy

Spark 3.0+ changed datetime parsing behaviour — instead of returning null when a format does not match, it throws a `SparkUpgradeException` and aborts the job. The KNMI data contains two-digit year timestamps (`03-08-01 00:10:00`) which trigger this. Setting `timeParserPolicy` to `CORRECTED` restores null-on-mismatch behaviour that `coalesce` depends on:

```python
.config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
```

### Building and Running

```bash
# Build the image
docker build -t heat-cold .

# Run heatwave detection
docker run heat-cold heatwave --start-year 2003 --start-month 7 --end-year 2003 --end-month 9

# Run coldwave detection
docker run heat-cold coldwave --start-year 2012 --end-year 2012

# Show help
docker run heat-cold
```

### Rebuild Behaviour After Changes

| What changed | Layers that rerun | Speed |
|---|---|---|
| Any `.py` file | Step 5 only | Fast (~2s) |
| `requirements.txt` | Steps 4 and 5 | Slow (full pip reinstall ~3 min) |
| `Dockerfile` | Everything from the changed line down | Depends on position |

---

## Scalability — Honest Assessment

### What scales horizontally

Silver and Gold are pure Spark DataFrame operations. Changing `.master("local[*]")` to point at a real cluster distributes those steps across as many machines as are available. The pipeline code does not change.

### What does not scale

Bronze runs entirely on the driver machine. It opens one HTTP stream, reads it sequentially, and writes to local disk. Spark workers on other machines cannot see that local disk. No amount of additional machines can speed up a single sequential read — this is sometimes called the **funnel problem**.

```
Bronze: one machine, one stream, local disk  ← ceiling for the whole solution
         │
         ▼
Silver + Gold: could run on 100 machines — but they wait for Bronze first
```

### How to fix it

Move Bronze to a monthly Airflow job in the cloud that writes partitioned Parquet files to Azure Data Lake. Individual Parquet files are independently addressable — Spark workers on a cluster can each read a different partition in parallel. The funnel is removed and the pipeline becomes genuinely horizontally scalable end to end. One line changes in `silver/load.py`. See `docs/cloud_usage.md`.

---

## Key Engineering Decisions

| Decision | Why |
|---|---|
| PySpark for Silver + Gold | Same DataFrame code runs in local mode or on a cluster — no rewrite to scale |
| All transforms as DataFrame operations, no Python loops | Loops run on the driver only; DataFrame operations distribute across workers |
| Built-in Spark functions only, no UDFs | UDFs break Catalyst optimisation and force Python serialisation per row |
| Medallion Architecture (Bronze/Silver/Gold) | Separation of concerns — ingestion, cleaning, and logic are independently testable and replaceable |
| Streaming `.tgz` extraction | Gzip has no index — sequential read is unavoidable; streaming avoids buffering the full archive in memory |
| Dynamic column detection in fixed-width parser | Resilient to format changes across decades of historical KNMI data |
| `inferSchema=false` in Spark CSV read | Avoids double-scan; gives explicit control over casting in Silver |
| `timeParserPolicy=CORRECTED` | Required for coalesce-based date fallback to work; Spark 3.0 made format mismatches throw instead of returning null |
| Gaps-and-islands with `row_number + datediff` | Only way to express consecutive-sequence detection as distributed column operations |
| `shuffle.partitions=8` | Default of 200 adds scheduling overhead with no benefit on small local data |
| Temp directory with `try/finally` cleanup | Deterministic resource cleanup regardless of pipeline success or failure |
| `click` CLI with shared decorator | DRY command definitions; input validation built into the framework |
| Docker with split COPY layers | Separating requirements.txt from source code preserves pip cache across code changes |
| `platform.system()` check for HADOOP_HOME | Same codebase runs on Windows (local dev) and Linux (Docker) without modification |
| Generic internal column names in Gold | Allows format.py and detection functions to serve both wave types without branching |
