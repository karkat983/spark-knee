# spark-knee

> Status: **in progress.** Sections marked *planned* are not built yet.

## Problem
Distributed jobs are usually sized by assuming runtime grows linearly with data, but past some point shuffle, spill and skew make it bend upward, and that point is rarely measured. This matters because capacity plans and cost estimates built on the linear assumption fail exactly when data grows. See Ousterhout et al. 2015, "Making Sense of Performance in Data Analytics Frameworks" (NSDI).

## Approach
*Planned.* Run one aggregation + join job on public NYC TLC trip data at 1x to 16x size and several partition counts, record wall-clock time and shuffle/spill metrics, and fit a two-segment model that locates the knee where runtime stops being linear.

## Results
*Planned.* No numbers yet.

| Size | Partitions | Runtime (s) | Shuffle (GB) |
|------|------------|-------------|--------------|
| —    | —          | —           | —            |

## Run it
Requires Java 17+ (for Spark).
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/download_tlc.py     # 1 month of yellow-taxi trips + zone lookup into data/raw/
python -m src.job                  # runs the job once and prints runtime
pytest
```
Day 1 check: `pytest` passes (4 tests) with PySpark 4.2 on Python 3.12 and OpenJDK 17. The size sweep and knee fit are *planned*.

## What I learned
*Planned.*

## Notes
Public data only (NYC Taxi & Limousine Commission trip records). Not affiliated with any employer. Built October 2026.

## Changelog
- Day 1: project scaffold and changelog.
