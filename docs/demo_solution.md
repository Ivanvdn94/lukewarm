# KNMI Wave Detector — Solution Overview

## What Does It Do?

This application analyses historical weather data from the Royal Dutch Meteorological Institute (KNMI) and automatically detects **heatwaves** and **coldwaves** for the De Bilt weather station in the Netherlands.

A user provides a date range and chooses a wave type. The application fetches the relevant data, processes it, and returns a clean table of every wave event found in that period.

---

## The Definitions

The app uses the official KNMI meteorological definitions:

| Wave Type | Rule |
|---|---|
| Heatwave | At least 5 consecutive days where the daily maximum temperature reaches 25°C or above, of which at least 3 days reach 30°C or above (tropical days) |
| Coldwave | At least 5 consecutive days where the daily maximum temperature stays below 0°C, of which at least 3 days have a minimum temperature below -10°C (high frost days) |

---

## Example Output

```
2 heatwave(s) found:
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
| Data processing | Apache Spark (PySpark) | Horizontally scalable — the same code runs on a laptop or a 100-node cluster |
| Data source | Azure Blob Storage | Central, accessible storage for the KNMI archive |
| CLI | Click (Python) | Simple command-line interface for running heatwave or coldwave detection |
| Containerisation | Docker | Runs anywhere — no local Java or dependency setup required |

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
Bronze  — Get the raw data in (fetch and parse)
Silver  — Make the data trustworthy (clean, filter, type-cast)
Gold    — Make the data useful (aggregate, detect, format)
```

This means any layer can be swapped out independently. Changing the data source only touches bronze. Adding a new detection rule only touches gold. Nothing bleeds between layers.
