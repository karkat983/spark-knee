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
*Planned.*

## What I learned
*Planned.*

## Notes
Public data only (NYC Taxi & Limousine Commission trip records). Not affiliated with any employer. Built October 2026.

## Changelog
- Day 1: project scaffold and changelog.
