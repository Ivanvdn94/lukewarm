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
│  Azure Blob (.tgz)  →  Stream & filter  →  Parse to CSV    │
│  One sequential         reads archive       fixed-width     │
│  stream, start          start to finish     to CSV          │
│  to finish              on driver           temp folder     │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER                                                     │
│                                                             │
│  spark.read.csv()  →  filter_station()  →  Cast types      │
│  Load all CSVs        Keep De Bilt          TX/TN → double  │
│  in parallel          Trim whitespace       Drop nulls      │
│                       Parse 3 date formats                  │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────┐
│  GOLD                                                                │
│                                                                      │
│  daily_aggregate()  →  detect_heatwaves()   →  format_output()  →  Console │
│  10-min → 1 row/day    or detect_coldwaves()    Dates, labels,      Table   │
│  max TX, min TN        Gaps-and-islands         rounding            printed │
│                        KNMI thresholds                                       │
└──────────────────────────────────────────────────────────────────────┘
```

### Key Points (speaker notes)
- Everything runs on one machine — laptop or Docker container
- Spark runs in local mode — no cluster needed
- Data is fetched fresh each run by streaming the .tgz from Azure Blob
- The .tgz is a gzip-compressed sequential stream — must be read start to finish to reach any target month, even if only 2 months are needed
- Thread pooling cannot help — there is one stream, it cannot be split
- Bronze is the scaling ceiling — Silver and Gold could distribute across a cluster but are waiting for one sequential stream to finish first (the funnel problem)
- On Windows: requires winutils.exe for Spark filesystem access
- In Docker: Linux container, no Windows dependencies needed
- The pipeline logic (Silver + Gold) is pure Spark DataFrame API — identical code would run on a cluster unchanged

---

## Slide 2 — Hybrid Cloud Architecture

**Title:** KNMI Wave Detector — Hybrid Architecture
**Subtitle:** Cloud handles bronze extraction monthly — Docker pipeline reads Parquet directly

### Flow

```
CLOUD — Airflow DAG runs monthly (1st of each month, 06:00 UTC)
─────────────────────────────────────────────────────

KNMI Source (.tgz on Azure Blob)
         │
         ▼
Check: is last month's Parquet already in bronze/?
         │ no                     │ yes
         ▼                        ▼
Extract new month         Skip — nothing to do
.tgz → Parquet file
         │
         ▼
┌──────────────────────────────────┐
│     Azure Data Lake Gen2         │
│                                  │
│  bronze/                         │
│  ├── year=2003/month=07/*.parquet│
│  ├── year=2003/month=08/*.parquet│
│  └── grows by one file per month │
└──────────────────────────────────┘

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
│  Reads only the            Keep De Bilt         TX/TN →    │
│  requested partitions      Parse dates          double      │
│  from ADLS directly                                         │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────┐
│  GOLD                                                                │
│                                                                      │
│  daily_aggregate()  →  detect_heatwaves()   →  format_output()  →  Console │
│  10-min → 1 row/day    or detect_coldwaves()    Dates, labels,      Table   │
│  max TX, min TN        Gaps-and-islands         rounding            printed │
│                        KNMI thresholds                                       │
└──────────────────────────────────────────────────────────────────────┘
```

### Key Points (speaker notes)
- Only one thing changes — where bronze data comes from
- Airflow DAG runs on the 1st of each month — picks up the previous month automatically
- The heavy sequential stream runs once per month in the cloud, not on every user run
- Parquet is columnar and compressed — faster to read than CSV, schema is embedded (no type guessing)
- Partition pruning: asking for 2003-07 to 2003-09 opens exactly 3 folders — everything else is skipped without reading a single file
- The funnel problem is solved — each Parquet file is independently addressable, Spark workers on a cluster can read different partitions in parallel
- The pipeline is now genuinely horizontally scalable end to end
- Silver, Gold, CLI, and tests are completely unchanged
- Low cost — ADLS storage is cheap, Airflow can run on a small VM or managed service
- One line of code changes in silver/load.py — that is the entire migration

---

## Side-by-Side Comparison Slide

**Title:** Local vs Hybrid — What Changes

| Area | Local / Docker | Hybrid |
|---|---|---|
| Bronze extraction | Streams full .tgz sequentially on every run | Airflow DAG runs once monthly, writes one Parquet per month |
| Scaling ceiling | Bronze sequential stream blocks horizontal scale | Removed — each Parquet partition independently readable |
| Storage | CSV in local temp directory, deleted after each run | Parquet on ADLS Gen2, persisted and partitioned by year/month |
| Read performance | Full CSV scan, types inferred | Columnar read, partition pruning, schema embedded |
| Silver load path | Local filesystem | ADLS blob path |
| Secrets | Hardcoded SAS URL in config.py | Key Vault (optional) |
| Silver / Gold logic | Unchanged | Unchanged |
| Docker / CLI | Unchanged | Unchanged |
| Tests | Unchanged | Unchanged |
| **Code changes needed** | — | **One line in silver/load.py** |
