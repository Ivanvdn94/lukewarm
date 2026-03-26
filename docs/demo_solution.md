# KNMI Wave Detector — Solution Overview

## What Does It Do?

This application analyses historical weather data from the Royal Dutch Meteorological Institute (KNMI) and automatically detects **heatwaves** and **coldwaves** for the De Bilt weather station in the Netherlands.

A user provides a date range and chooses a wave type. The application fetches the relevant data, processes it, and returns a clean table of every wave event found in that period.

---

## The Definitions

The app uses the official KNMI meteorological definitions:

| Wave | Definition |
|---|---|
| Heatwave | At least 5 consecutive days where the daily maximum temperature reaches 25°C or above, of which at least 3 days reach 30°C or above (tropical days) |
| Coldwave | At least 5 consecutive days where the daily maximum temperature stays below 0°C, of which at least 3 days have a minimum temperature below -10°C (high frost days) |

---

## Example Output

```
1 heatwave(s) found:
+------------+---------------+------------------+-----------------------+---------------+
| From date  | To date (inc.)| Duration (days)  | Tropical days         | Max temp      |
+------------+---------------+------------------+-----------------------+---------------+
| 1 Aug 2003 | 13 Aug 2003   | 13               | 7                     | 35.0          |
+------------+---------------+------------------+-----------------------+---------------+
```

---

## How It Works — Step by Step

```
  User runs a command
         │
         ▼
┌─────────────────────┐
│   1. FETCH          │  Stream the weather archive from Azure Blob Storage.
│                     │  Only download the months that are needed — nothing more.
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   2. PARSE          │  The raw files are in an old fixed-width format.
│                     │  Convert them to clean CSV files ready for processing.
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   3. LOAD & FILTER  │  Apache Spark reads the CSVs in parallel.
│                     │  Keep only the De Bilt station rows.
│                     │  Parse dates and temperature values into usable types.
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   4. AGGREGATE      │  The raw data has a reading every 10 minutes.
│                     │  Collapse these down to one row per day:
│                     │  the day's highest and lowest temperature.
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   5. DETECT         │  Apply the KNMI wave definitions.
│                     │  Find consecutive qualifying days using the
│                     │  gaps-and-islands algorithm.
│                     │  Filter to only sequences that meet both thresholds.
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   6. OUTPUT         │  Format the results into a readable table.
│                     │  Display to the user.
└─────────────────────┘
```

---

## The Technology

| Component | Technology | Why |
|---|---|---|
| Data processing | Apache Spark (PySpark) | DataFrame API runs identically in local mode or on a cluster — no code changes to scale up |
| CLI | Click (Python) | Clean command-line interface with argument validation built in |
| Containerisation | Docker | Runs anywhere — no local Java, Hadoop, or Python dependency setup required |

---

## How To Run It

```bash
# Detect heatwaves — summer of 2003
docker run heat-cold heatwave --start-year 2003 --start-month 7 --end-year 2003 --end-month 9

# Detect coldwaves — full year 2012
docker run heat-cold coldwave --start-year 2012 --end-year 2012
```

---

## The Code Structure

The codebase follows the **Medallion Architecture** — a standard data engineering pattern that separates the pipeline into three layers, each with a single responsibility:

```
Bronze  — Get the raw data in (fetch and parse the .tgz archive)
Silver  — Make the data trustworthy (clean, filter, type-cast)
Gold    — Make the data useful (aggregate, detect, format)
```

Each layer only does its own job. Changing the data source only touches Bronze. Adding a new detection rule only touches Gold. Nothing bleeds between layers.

---

## Known Constraints and Honest Tradeoffs

### Bronze is the scaling ceiling

Bronze streams one `.tgz` file sequentially on one machine. Silver and Gold are fully distributable with Spark, but they wait for Bronze to finish first. No amount of additional machines can speed up a single sequential stream. This is sometimes called the **funnel problem**.

In practice this means the pipeline scales well for the processing logic but not for ingestion. For a demo or analyst tool this is fine. For a production system handling large volumes it is the first thing to address.

### The .tgz is read from the beginning every time

The archive contains decades of data. Even if only 2 months are needed, the stream reads from the 1950s to reach them. This is a property of gzip compression — there is no index to jump ahead. Thread pooling cannot help because there is only one stream.

### The Bronze / Silver / Gold fit

The naming broadly holds but has two honest gaps:

- `gold/aggregate.py` — collapsing 10-minute readings to daily is data normalisation (Silver work), not business logic. It sits in Gold because the project is small enough that the distinction does not matter.
- `gold/format.py` — formatting for console display is a presentation concern that sits beyond what Gold typically means. In a larger system it would be a separate output or serving layer.

---

## The Natural Next Step

Offload Bronze to a monthly Airflow job that writes Parquet files to Azure Data Lake. The Docker pipeline reads those files directly instead of streaming the archive.

Result: one line changes in `silver/load.py`. Everything else stays the same. The funnel problem is solved, the pipeline becomes genuinely horizontally scalable, and the archive is no longer re-read on every run.

See `docs/cloud_usage.md` for the full hybrid architecture.
