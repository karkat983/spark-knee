# The benchmarked job

`src/job.py` answers two questions about NYC yellow-taxi trips:

1. **Hourly zone stats:** for every (pickup zone, hour of day), how many trips, average distance
   and duration, and total revenue? Joined with the zone lookup to add borough and zone name.
2. **Top zones per borough:** the 5 highest-revenue pickup zones in each borough.

## Stages and why each one shuffles

```
read Parquet (8 of 19 columns)  ->  clean (narrow filter)  ->  partial aggregate per task
                                                                   |
                                                    SHUFFLE 1: hash by (zone, hour)
                                                                   v
                                                         final aggregate (32 partitions)
                                                                   |
                                    join zone names (broadcast: no shuffle | merge: SHUFFLE 2)
                                                                   |
                                                         cache, write hourly stats
                                                                   |
                                     re-aggregate per zone, SHUFFLE 3: hash by borough
                                                                   v
                                         window row_number() over borough -> write top zones
```

- **Read and clean** are narrow: each task reads one slice of a file and drops bad rows
  without talking to other tasks. Cost grows linearly with data.
- **Shuffle 1** is the expensive one. Every task partially aggregates its rows (map-side
  combine), then sends one record per (zone, hour) it saw to the partition that owns that key.
  The volume depends on distinct keys per task, not on total rows, which is why it stays small
  at 1x (472 KB written over 8 tasks; see docs/notes.md).
- **The join** is against a 265-row table. With `join: auto` or `broadcast`, Spark copies the
  zone table to every task (BroadcastHashJoin) and no shuffle happens; `join: shuffle` forces a
  SortMergeJoin with two more exchanges. The plans are saved under `results/plans/`.
- **Shuffle 3** groups by borough for the window function, a small shuffle with very skewed
  keys (Manhattan dominates), which is useful for the skew experiments.

## Why this job can bend the runtime curve

The aggregation's per-task hash maps and the shuffle buffers both live in executor memory.
While they fit, cost is linear in data size. As data grows, three things can make it
super-linear: hash maps spill to disk, shuffle blocks spill, and with few partitions each task
handles more data than fits. The sweep varies size and partition count to find where that
happens, and the event-log metrics (spill bytes, shuffle bytes, GC time) explain why.

## Adaptive query execution

Spark 4 enables AQE by default. It coalesces small shuffle partitions after shuffle 1, so a
1x run with 32 partitions wrote a single output file. The sweep runs with AQE both on and off
so that the partition-count setting means what it says.
