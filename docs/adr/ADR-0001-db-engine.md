# ADR-0001: Database Engine (DuckDB first)

Status: Accepted  
Date: 2026-02-23

## Context
We are building a private, local NFL data warehouse (15+ years historical + weekly updates) on a Windows 11 laptop.
We need:
- fast analytical queries (joins, aggregations) for ML/simulations
- low operational overhead (no server admin for the initial phase)
- deterministic, testable ingestion runs with an audit trail (run registry)
- ability to store large fact tables (e.g., play-by-play) and dimension data (teams, players, venues)
- robustness: raw snapshots + staging + canonical layers, with reconciliation and quarantine for inconsistencies

## Decision
We will use **DuckDB** as the initial database engine:
- the database is a single local file at `data/nfl.duckdb` (ignored by Git)
- ingestion will write immutable raw snapshots to `data/raw/...`
- staging tables will mirror sources (nflverse, ESPN, NOAA, ...)
- canonical tables will be normalized and constrained for consistency
- every ingestion run is recorded in `audit.ingest_run`

## Why DuckDB (now)
- Embedded DB: zero service setup, ideal for a single-user laptop workflow
- Excellent analytics performance for large columnar-style workloads
- Works well with Parquet/CSV pipelines and incremental loads
- Keeps our first iterations small and reproducible

## Alternatives considered
### PostgreSQL (local service)
Pros:
- strong concurrency, mature ecosystem, easy API/web integration later
- pgvector possible if we later want vector search inside the DB
Cons:
- operational overhead (service install/maintenance/backups)
- slower iteration start for a private hobby workflow

### SQLite
Pros:
- embedded and simple
Cons:
- not designed for large analytics workloads like play-by-play at scale

## Consequences
- + Fast start, low ops overhead
- + Works well with a “raw → staging → canonical” pipeline
- - Not ideal for multi-user concurrency (future web UI could hit limits)
- - Some features (e.g., role-based access, multi-session heavy writes) are not the focus

## Follow-ups
- Revisit DB choice once we add a local web UI + API bridge and see real concurrency needs.
- If/when we migrate to Postgres, we keep the canonical schema stable and migrate data via Parquet exports.
