# Presentation Flows

Two flow diagrams for the PowerPoint — one for the current local architecture, one for the hybrid cloud architecture.

---

## Slide 1 — Current Architecture (Local / Docker)

**Title:** KNMI Wave Detector — Current Architecture
**Subtitle:** Single machine pipeline running locally or in Docker

### Flow

```
User runs CLI command
python heatwave.py heatwave --start-year 2003 --end-year 2003
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│  BRONZE                                                     │
│                                                             │
│  Azure Blob (.tgz)  →  Stream & Filter  →  Parse to CSV    │
│                         archive on driver    fixed-width    │
│                                          →  Temp folder     │
│                                             (local disk)    │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER                                                     │
│                                                             │
│  spark.read.csv()  →  filter_station()  →  Cast types      │
│  Load all CSVs        Keep De Bilt          TX/TN → double  │
│  in parallel          Parse dates           Drop nulls      │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD                                                       │
│                                                             │
│  daily_aggregate()  →  detect_heatwaves()  →  format_output()  →  Console │
│  10-min → 1 row/day    or detect_coldwaves()   Dates, labels,     Table   │
│  max TX, min TN        Gaps-and-islands        rounding           printed │
│                        KNMI thresholds                                     │
└─────────────────────────────────────────────────────────────┘
```

### Key Points (speaker notes)
- Everything runs on one machine — laptop or Docker container
- Spark runs in local mode — no cluster needed
- Data is fetched fresh each run by streaming the .tgz from Azure Blob
- The .tgz is sequential — must be read start to finish to find target months
- On Windows: requires winutils.exe for Spark filesystem access
- In Docker: Linux container, no Windows dependencies needed
- The pipeline logic (Silver + Gold) is pure Spark DataFrame API

---

## Slide 2 — Hybrid Cloud Architecture

**Title:** KNMI Wave Detector — Hybrid Architecture
**Subtitle:** Cloud handles bronze extraction monthly — Docker pipeline unchanged

### Flow

```
CLOUD — Airflow DAG runs monthly (1st of each month, 06:00 UTC)
─────────────────────────────────────────────────────

KNMI Source (.tgz on Azure Blob)
         │
         ▼
Check: is last month's file in bronze/?
         │ no                     │ yes
         ▼                        ▼
Extract new month         Skip — nothing to do
.tgz → Parquet file
         │
         ▼
┌─────────────────────────────────┐
│     Azure Data Lake Gen2        │
│                                 │
│  bronze/                        │
│  ├── year=2003/month=07/*.parquet│
│  ├── year=2003/month=08/*.parquet│
│  └── grows by one file per month│
└─────────────────────────────────┘

─────────────────────────────────────────────────────
LOCAL — Docker container, completely unchanged
─────────────────────────────────────────────────────

docker run heat-cold heatwave --start-year 2003 --end-year 2003
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER                                                     │
│                                                             │
│  spark.read.parquet()  →  filter_station()  →  Cast types  │
│  Reads from ADLS           Keep De Bilt         TX/TN →    │
│  (no temp dir needed)      Parse dates          double      │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD                                                       │
│                                                             │
│  daily_aggregate()  →  detect_heatwaves()  →  format_output()  →  Console │
│  10-min → 1 row/day    or detect_coldwaves()   Dates, labels,     Table   │
│  max TX, min TN        Gaps-and-islands        rounding           printed │
│                        KNMI thresholds                                     │
└─────────────────────────────────────────────────────────────┘
```

### Key Points (speaker notes)
- Only one thing changes — where bronze data comes from
- Airflow DAG runs on the 1st of each month — picks up the previous month's data
- Parquet is columnar and compressed — faster to read than CSV, schema is embedded
- The Docker container skips the .tgz streaming entirely — reads clean Parquet from ADLS
- Silver, Gold, CLI, and tests are completely unchanged
- Low cost — ADLS storage is cheap, Airflow can run on a small VM or managed service
- This solves the biggest bottleneck without any infrastructure complexity
- KNMI data is monthly so a monthly schedule is all that is needed — no over-engineering

---

## Side-by-Side Comparison Slide

**Title:** Local vs Hybrid — What Changes

| Area | Local / Docker | Hybrid |
|---|---|---|
| Bronze extraction | Streams .tgz on every run | Airflow DAG runs monthly, writes Parquet |
| Storage | CSV in local temp directory | Parquet on ADLS Gen2 |
| Silver load path | Local filesystem | ADLS blob path |
| Secrets | Hardcoded SAS URL in config.py | Key Vault (optional) |
| Silver / Gold logic | Unchanged | Unchanged |
| Docker / CLI | Unchanged | Unchanged |
| Tests | Unchanged | Unchanged |
| **Code changes needed** | — | **One line in silver/load.py** |
