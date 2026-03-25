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

## Entry Point: heatwave.py

This is the **orchestrator and CLI**. It wires all layers together.

**CLI Design — `click`**

The app uses the `click` library to expose two commands: `heatwave` and `coldwave`. A shared decorator `date_options` applies the date range arguments to both commands — avoiding duplication (DRY principle).

```
python heatwave.py heatwave --start-year 2003 --start-month 7 --end-year 2003 --end-month 9
python heatwave.py coldwave --start-year 1997 --start-month 1 --end-year 1997 --end-month 3
```

**Target Month Set**

A set comprehension computes exactly which months are needed before any I/O happens. Using a `set` gives O(1) lookup when filtering the archive stream, and prevents duplicate months.

**Temp Directory as a Clean-Room**

`tempfile.mkdtemp()` creates a fresh, isolated working directory per run. The `try/finally` block guarantees cleanup via `shutil.rmtree()` even if the pipeline crashes mid-run — preventing leftover files accumulating across runs.

**Spark Session**

- `spark.sql.shuffle.partitions = 8` — the default (200) causes unnecessary overhead for small/local data.
- `setLogLevel("ERROR")` — silences Spark's verbose INFO logs so only meaningful output reaches the user.

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
(non-qualifying day — excluded)
2003-08-05     216     4     212    ← new group
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

A **single source of truth** for shared constants: the Azure Blob URL and the De Bilt station identifier. Centralising these means a URL rotation or station change requires one edit, not a search-and-replace across files. This follows the **configuration externalization** principle from 12-factor app design.

### heatwave_v2.py

The original monolithic prototype written before the refactor. Everything lives in one file: fetching, parsing, Spark logic, and detection. Useful as a reference for intent without abstraction layers, but has hard-coded parameters, no CLI, and no configurable date range. The refactored version fixes all of these.

---

## Testing

### Setup

```bash
pip install pytest pytest-cov
```

### Structure

```
tests/
├── conftest.py        ← shared SparkSession fixture (created once per session)
├── test_extract.py    ← bronze layer — pure Python, no Spark
├── test_filter.py     ← silver layer — station filter, date parsing, type casting
├── test_aggregate.py  ← gold layer — daily max/min aggregation
├── test_detect.py     ← gold layer — heatwave and coldwave detection logic
└── test_format.py     ← gold layer — column labels, date formatting, ordering
```

### Running Tests

| Command | What it does |
|---|---|
| `pytest tests/` | Run all tests |
| `pytest tests/ -v` | Verbose — shows each test name pass/fail |
| `pytest tests/ --cov=. --cov-report=term-missing` | Run with coverage report |
| `pytest tests/test_detect.py` | Run a single file |
| `pytest tests/test_detect.py::TestDetectHeatwaves::test_minimum_valid_heatwave_detected` | Run a single test |

### What Each File Tests

**test_extract.py** — Bronze, pure Python (fast, no Spark overhead)
- CSV header matches fixed-width column names
- Comment lines (`#`) are excluded from output
- Data values land in the correct columns
- Parser handles reordered column headers (format resilience)
- All stations pass through (filtering is silver's job, not bronze's)
- Empty input produces no output

**test_filter.py** — Silver
- Correct station is kept; all others are excluded
- Whitespace trimming on the LOCATION field
- Timestamp parsing works for all three DTG formats (`yyyy-MM-dd HH:mm:ss`, `yy-MM-dd HH:mm:ss`, `yyyyMMddHHmmss`)
- Rows with unparseable or empty dates are dropped
- `TX_DRYB_10` and `TN_DRYB_10` are cast from string to double
- Empty strings become null rather than causing cast errors
- Output schema is exactly `{date, tx, tn}`

**test_aggregate.py** — Gold
- Multiple readings per day collapse to a single row
- `daily_max` is the highest `tx` reading of the day
- `daily_min` is the lowest `tn` reading of the day
- Multiple days produce multiple rows, ordered by date

**test_detect.py** — Gold (most critical — encodes KNMI definitions)
- Heatwaves detected when both thresholds are met (5 days, 3 tropical)
- Heatwaves not detected when duration is too short (4 days)
- Heatwaves not detected when too few tropical days (only 2)
- A cool day gap splits two hot runs into two separate waves
- Start/end dates, duration, and tropical day count are all correct
- Mirror set for coldwaves: 5 days max < 0, 3 high-frost days (min < -10)
- Empty DataFrame returns no results for both wave types

**test_format.py** — Gold
- Column label switches between "Number of tropical days" (heatwave) and "Number of high frost days" (coldwave)
- Temperature label switches between "Max temperature" and "Min temperature"
- Dates formatted as `"1 Aug 2003"` not ISO strings
- Temperature rounded to 1 decimal place
- Results ordered chronologically by start date
- Output has exactly 5 columns

### Design Notes

- The `conftest.py` fixture uses `scope="session"` — a single SparkSession is created once and shared across all Spark tests. Creating one per test would add ~10 seconds per test.
- `test_extract.py` requires no Spark at all — the fixed-width parser is pure Python and runs instantly.
- `test_detect.py` is the most valuable test file. It directly encodes the KNMI meteorological definitions and will catch any regression if thresholds or logic change.

---

## Docker

### Why Docker

The app has three non-trivial dependencies — Python, Java, and PySpark — plus a Windows-specific requirement for `winutils.exe`. Without Docker, every user has to install and configure all of these manually and correctly. Docker bakes everything into a single image that runs identically on any machine with Docker installed.

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

**Layer caching** means Docker skips any step whose inputs haven't changed since the last build. In practice, editing a `.py` file only reruns step 5 — the expensive Java install and pip install are cached and skipped. If `requirements.txt` were copied alongside the code in step 5, every code change would invalidate the pip cache and force a full PySpark reinstall on every build.

### Windows vs Linux

The `HADOOP_HOME` and `PATH` environment variables are only needed on Windows so Spark can find `winutils.exe`. Inside a Linux container, Spark has native filesystem access and these are not needed. A platform check in `heatwave.py` handles this:

```python
if platform.system() == "Windows":
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
```

This means the same codebase runs locally on Windows and inside Docker on Linux without any modification.

### Spark DateTime Parser Policy

Spark 3.0+ changed datetime parsing behaviour — instead of returning null when a format doesn't match, it throws a `SparkUpgradeException` and aborts the job. The KNMI data contains two-digit year timestamps (`03-08-01 00:10:00`) which trigger this. Setting `timeParserPolicy` to `CORRECTED` restores the null-on-mismatch behaviour that `coalesce` depends on:

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
| `requirements.txt` | Steps 4 and 5 | Slow (full pip reinstall) |
| `Dockerfile` | Everything from the changed line down | Depends on position |

---

## Key Engineering Decisions

| Decision | Why |
|---|---|
| Medallion Architecture (Bronze/Silver/Gold) | Separation of concerns — ingestion, cleaning, and logic are independently testable and replaceable |
| Streaming `.tgz` extraction | Avoids loading the whole archive into memory |
| Dynamic column detection in fixed-width parser | Resilient to format changes across historical KNMI data |
| `inferSchema=false` in Spark CSV read | Avoids double-scan; gives explicit control over casting |
| Gaps-and-islands with `row_number + datediff` | Single-pass Spark-native pattern for consecutive sequence detection — no UDFs, no loops |
| Temp directory with `try/finally` cleanup | Deterministic resource cleanup regardless of pipeline success or failure |
| `click` CLI with shared decorator | DRY command definitions; input validation built into the framework |
| `config.py` for constants | Single source of truth; avoids scattered hardcoded values |
| Generic internal column names in gold | Allows `format.py` and detection functions to serve both wave types without branching |
