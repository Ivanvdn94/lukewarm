# Cloud Usage — Hybrid Architecture

## Overview

The local solution is built for local testing and serves as a live on-machine demo. The natural next step is a lightweight hybrid — offload only the bronze extraction to the cloud so that Parquet files are pre-built on blob storage and kept up to date automatically. The local Docker pipeline reads from those files instead of streaming the `.tgz` on every run.

---

## Why Hybrid — The Real Bottleneck

The `.tgz` streaming in `bronze/extract.py` is the slowest and most fragile part of the pipeline for two distinct reasons.

### 1. The streaming problem

The KNMI archive is one large gzip-compressed tar file. Gzip compression is sequential — there is no internal index. To reach August 2003, the stream must read through every file before it, going back to the 1950s. There is no way to jump directly to a target month.

```
Request: heatwave 2003
Stream reads: 1951... 1952... ... 2002... 2003-07 ✓  2003-08 ✓  ...continues
```

This is not a network speed problem. Even if the connection were instant, the sequential read would still happen in full.

### 2. Thread pooling does not help

Thread pooling lets you download multiple files at the same time. It only helps when there are multiple independent things to download. Here there is one stream that must be read in order — more threads cannot change that constraint.

### 3. The horizontal scaling ceiling (the funnel problem)

Bronze runs on one machine, reads one sequential stream, and writes to local disk. Spark workers on other machines cannot see that local disk. Even though Silver and Gold are fully distributable across a cluster, they are waiting for Bronze to finish on one machine first. The slowest step determines overall speed regardless of how many machines are added downstream.

```
Bronze: one machine, one stream, local disk  ← ceiling for the whole solution
         │
         ▼
Silver + Gold: could run on 100 machines — but they are waiting for Bronze
```

Moving Bronze to the cloud removes the funnel entirely.

---

## Architecture

```
CLOUD — Airflow DAG runs on the 1st of each month at 06:00 UTC
─────────────────────────────────────────────────────────────
KNMI Source (.tgz on Azure Blob)
         │
         ▼
Check: is last month's Parquet file already in bronze/?
         │ no                           │ yes
         ▼                              ▼
Extract new month               Skip — nothing to do
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
LOCAL — Docker container, completely unchanged
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

## Why Parquet Over CSV

| | CSV (current) | Parquet (hybrid) |
|---|---|---|
| Schema | Inferred or guessed at read time | Embedded in the file — always correct |
| Read speed | Full file scan, all columns | Columnar — only reads columns needed |
| Size on disk | Uncompressed text | Compressed — typically 5–10x smaller |
| Partitioning | Not possible | year=/month= folders enable partition pruning |
| Spark read | Must infer types | Types known immediately, no casting needed |

Partition pruning is the key win. When the user asks for 2003-07 to 2003-09, Spark reads only:

```
bronze/year=2003/month=07/
bronze/year=2003/month=08/
bronze/year=2003/month=09/
```

Everything else is skipped without opening a single file.

---

## How Scalability Changes

**Current (local):** Bronze is the ceiling. Silver and Gold could scale horizontally but are blocked waiting for one sequential stream on one machine.

**Hybrid:** Each Parquet file is independently addressable on ADLS. Spark workers on a cluster can each read a different partition directly from blob storage in parallel. The funnel is removed — the pipeline becomes genuinely horizontally scalable end to end.

---

## What Changes in the Code

One line in `silver/load.py`:

```python
# Before — reads local CSVs from temp directory
spark.read.csv("file:///tmp/knmi_...")

# After — reads Parquet directly from ADLS
spark.read.parquet("abfss://knmi-data@account.dfs.core.windows.net/bronze/")
```

`bronze/extract.py` becomes a one-time historical migration script rather than part of every run. The temp directory and `shutil.rmtree` cleanup in `heatwave.py` are also removed.

Everything else — Silver, Gold, CLI, tests — is completely unchanged.

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
| Bronze extraction | Streams full .tgz sequentially on every run | Airflow DAG runs monthly, writes one Parquet per month |
| Horizontal scaling | Blocked by Bronze sequential stream (funnel problem) | Fully scalable — each Parquet file independently readable |
| Storage | CSV in temp directory, deleted after each run | Parquet on ADLS Gen2, persisted and partitioned |
| Silver load path | `file:///tmp/knmi_...` | `abfss://...bronze/` |
| Read performance | Full CSV scan, type inference | Columnar read, partition pruning, schema embedded |
| Secrets | Hardcoded SAS URL | Key Vault (optional) |
| Silver / Gold logic | Unchanged | Unchanged |
| Docker / CLI | Unchanged | Unchanged |
| Tests | Unchanged | Unchanged |
| **Code changes needed** | — | **One line in silver/load.py** |
