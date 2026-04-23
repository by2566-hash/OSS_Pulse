# OSS Pulse GH Archive Pipeline

This repository contains the Spark ingestion, profiling, cleaning, and HPC rolling-run scripts for the OSS Pulse project.

## Analysis Window

- Full project target: January 1, 2025 00:00:00 UTC through December 31, 2025 23:59:59 UTC
- Local smoke-test example used during development: a smaller UTC slice such as `2025-01-16`

The full one-year run is designed for SSH/HPC with the rolling pipeline documented in [hpc/README.md](hpc/README.md).

## Requirements

- Python 3.13
- `pyspark==4.1.1`
- Java 17

If you run the Spark modules directly, make sure `JAVA_HOME` points to a Java 17 installation. On this machine, the project smoke script auto-selects the Java 17 runtime inside the Anaconda environment.

## Main Entry Points

- Ingestion: `python -m ingestion.ingest_gharchive`
- Profiling: `python -m profiling.profile_gharchive`
- Cleaning: `python -m cleaning.clean_gharchive`
- Local smoke run: `bash ingestion/run_local_gharchive_smoke.sh`
- Full-year HPC rolling run: `bash hpc/scripts/run_gharchive_2025_rolling.sh hpc/env/oss_pulse_2025.env`

## Sharing on GitHub

This repo is set up to share code and documentation only. Downloaded GH Archive data, derived Parquet outputs, local profiling CSVs, and machine-specific env files are ignored by Git so the repository stays lightweight for collaborators.
