# Releases

## v0.1-job (2026-10-09)

The benchmarked job and its instrumentation, before any size sweep.

- Job: clean -> hourly zone aggregation -> zone-name join -> top zones per borough (window);
  output written as Parquet.
- Join strategy switch (auto / broadcast / shuffle) with plans saved via `--explain`.
- Spark event logs (plain JSON lines) and `src/eventlog.py` for shuffle, spill, GC, peak
  memory, and per-stage task counts and durations.
- Data: January-June 2024 yellow-taxi trips (20.3 M rows), checksums in docs/data.md;
  schema check and per-file casting for type drift.
- 44 tests passing.
