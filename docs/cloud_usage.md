# Cloud Usage — Hybrid Architecture

## Overview

The local solution is built to facilitate local testing and serve as a live on-machine demo. The natural next step is a lightweight hybrid — offload only the bronze extraction to the cloud so that Parquet files are pre-built on blob storage and kept up to date automatically. The local Docker pipeline reads from those files instead of streaming the `.tgz` on every run.

---

## Why Hybrid

The `.tgz` streaming in `bronze/extract.py` is the slowest and most fragile part of the pipeline:
- Network dependent — fails if the blob is temporarily unavailable
- Sequential — the archive must be read start to finish to find target months
- Repeated work — the same historical data is re-downloaded on every run

Moving this one step to the cloud means:
- The heavy extraction happens once per new month, not on every run
- The Docker container skips bronze entirely and reads clean Parquet files directly
- The rest of the pipeline — Silver, Gold, CLI, tests — stays completely unchanged

---

## Architecture

```
CLOUD — runs daily via ADF / Airflow DAG
─────────────────────────────────────────────────────────────
KNMI Source (.tgz on Azure Blob)
         │
         ▼
Airflow DAG — runs 1st of each month at 06:00 UTC
(cron: 0 6 1 * *)
         │
         ▼
Check: is last month's file in bronze/?
         │ no                         │ yes
         ▼                            ▼
Extract new month             Skip — nothing to do
.tgz → Parquet file
write to ADLS bronze/
         │
         ▼
Azure Data Lake Gen2
└── bronze/
    ├── year=2003/month=07/kis_tot_200307.parquet
    ├── year=2003/month=08/kis_tot_200308.parquet
    └── ...  (grows by one file per month automatically)

─────────────────────────────────────────────────────────────
LOCAL — Docker container, unchanged
─────────────────────────────────────────────────────────────
docker run heat-cold heatwave --start-year 2003 --end-year 2003
         │
         ▼  (bronze/extract.py no longer runs)
silver/load.py     → reads Parquet from ADLS instead of local CSV
silver/filter.py   → unchanged
gold/aggregate.py  → unchanged
gold/detect.py     → unchanged
gold/format.py     → unchanged
         │
         ▼
Console output — same as today
```

---

## What Changes in the Code

One line in `silver/load.py`:

```python
# Before — reads local CSVs from temp directory
spark.read.csv("file:///tmp/knmi_...")

# After — reads Parquet directly from blob
spark.read.parquet("abfss://knmi-data@account.dfs.core.windows.net/bronze/")
```

`bronze/extract.py` becomes a one-time historical migration script rather than part of every run. The temp directory and `shutil.rmtree` cleanup in `heatwave.py` are also removed.

---

## What You Need in Azure

| Component | Purpose | Cost |
|---|---|---|
| ADLS Gen2 storage account | Stores the Parquet files | Low — pay per GB stored |
| Airflow DAG | Runs the monthly extraction job | Low — small VM or managed Airflow |
| Azure Key Vault (optional) | Stores the storage connection string securely | Low |

---

## Secrets

The SAS URL in `config.py` is replaced with a storage connection string or Key Vault reference. The rest of `config.py` stays the same.

---

## Summary of Changes

| Area | Local / Docker | Hybrid |
|---|---|---|
| Bronze extraction | Streams `.tgz` on every run | Airflow DAG runs monthly, writes Parquet |
| Storage | CSV in temp directory | Parquet on ADLS Gen2 |
| Silver load path | `file:///tmp/knmi_...` | `abfss://...bronze/` |
| Secrets | Hardcoded SAS URL | Key Vault (optional) |
| Silver / Gold logic | Unchanged | Unchanged |
| Docker / CLI | Unchanged | Unchanged |
| Tests | Unchanged | Unchanged |
