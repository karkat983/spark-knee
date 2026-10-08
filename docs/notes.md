# Working notes

## 2026-10-09: first end-to-end run (1x)

`python -m src.job` on `yellow_tripdata_2024-01.parquet` (2,964,624 trips), local mode
`local[*]` on an Apple M3 (8 cores, 24 GB), driver memory 4 g, 32 shuffle partitions,
PySpark 4.2.0, OpenJDK 17.0.20.

| Run | Output rows | Runtime (s) |
|-----|-------------|-------------|
| 1 | 5,131 | 2.86 |
| 2 | 5,131 | 3.24 |
| 3 | 5,131 | 2.76 |

Runtime covers building and collecting the job only (`collect()`), not JVM start-up or session
creation. 5,131 rows = (pickup zone, hour) pairs that had at least one clean trip; the maximum
possible is 265 zones x 24 hours = 6,360.

At ~3 s per month the 1x job is small. That is expected: the knee should appear only once data
no longer fits comfortably in memory, which is what the 16x sizes are for.

### Time zone check

Spark reads the TLC timestamps as `timestamp_ntz` (no time zone). With the session time zone
set to UTC, `hour()` returns the New York wall-clock hour: the three busiest pickup hours are
18:00, 17:00 and 16:00 (212,788 / 206,257 / 190,201 trips), the evening rush, as expected.
Note that `collect()` turns timestamps into Python datetimes in the *driver's* local zone, so
printed values can look shifted; computations inside Spark are not affected.
