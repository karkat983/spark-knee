"""Summarise a Spark event log (JSON lines) into the metrics the sweep records.

Spark writes one SparkListenerTaskEnd event per task with its "Task Metrics"; summing them
gives the job's shuffle volume and spill, which is what explains a runtime knee.
"""
import json
import pathlib
from collections.abc import Iterable

METRICS = [
    "tasks", "stages", "executor_run_time_ms", "gc_time_ms",
    "shuffle_read_bytes", "shuffle_write_bytes", "memory_spilled_bytes", "disk_spilled_bytes",
    "peak_execution_memory_bytes", "input_bytes", "input_records",
]


def read_events(path: pathlib.Path | str) -> Iterable[dict]:
    with open(path) as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def _add_task(out: dict, m: dict) -> None:
    """Accumulate one task's metrics into `out` (keys from METRICS)."""
    read = m.get("Shuffle Read Metrics", {})
    write = m.get("Shuffle Write Metrics", {})
    inp = m.get("Input Metrics", {})
    out["tasks"] += 1
    out["executor_run_time_ms"] += m.get("Executor Run Time", 0)
    out["gc_time_ms"] += m.get("JVM GC Time", 0)
    out["shuffle_read_bytes"] += read.get("Remote Bytes Read", 0) + read.get("Local Bytes Read", 0)
    out["shuffle_write_bytes"] += write.get("Shuffle Bytes Written", 0)
    out["memory_spilled_bytes"] += m.get("Memory Bytes Spilled", 0)
    out["disk_spilled_bytes"] += m.get("Disk Bytes Spilled", 0)
    peak = m.get("Peak Execution Memory", 0)
    out["peak_execution_memory_bytes"] = max(out["peak_execution_memory_bytes"], peak)
    out["input_bytes"] += inp.get("Bytes Read", 0)
    out["input_records"] += inp.get("Records Read", 0)


def _task_ends(events: Iterable[dict]) -> Iterable[dict]:
    for e in events:
        if e.get("Event") == "SparkListenerTaskEnd" and "Task Metrics" in e:
            yield e


def by_stage(events: Iterable[dict]) -> dict[tuple[int, int], dict]:
    """Metrics per (stage ID, attempt), so the expensive stage of a job can be found."""
    stages: dict[tuple[int, int], dict] = {}
    for e in _task_ends(events):
        key = (e.get("Stage ID"), e.get("Stage Attempt ID", 0))
        _add_task(stages.setdefault(key, dict.fromkeys(METRICS, 0)), e["Task Metrics"])
    for metrics in stages.values():
        metrics["stages"] = 1
    return stages


def summarise(events: Iterable[dict]) -> dict:
    """Job totals: sums over every task (peak memory is the max over tasks)."""
    out = dict.fromkeys(METRICS, 0)
    stages = set()
    for e in _task_ends(events):
        stages.add((e.get("Stage ID"), e.get("Stage Attempt ID", 0)))
        _add_task(out, e["Task Metrics"])
    out["stages"] = len(stages)
    return out


def by_job_group(events: list[dict]) -> dict[str, dict]:
    """Task metrics summed per Spark job group (sc.setJobGroup), so one application that does
    several things (e.g. builds several dataset sizes) can be broken down per thing."""
    stage_group: dict[tuple[int, int], str] = {}
    for e in events:
        if e.get("Event") == "SparkListenerStageSubmitted":
            info = e["Stage Info"]
            group = (e.get("Properties") or {}).get("spark.jobGroup.id")
            if group:
                stage_group[(info["Stage ID"], info.get("Stage Attempt ID", 0))] = group
    groups: dict[str, dict] = {}
    for key, metrics in by_stage(events).items():
        group = stage_group.get(key)
        if group is None:
            continue
        acc = groups.setdefault(group, dict.fromkeys(METRICS, 0))
        for k, v in metrics.items():
            acc[k] = max(acc[k], v) if k == "peak_execution_memory_bytes" else acc[k] + v
    return groups


def stage_table(events: list[dict]) -> list[dict]:
    """One row per completed stage: planned task count, duration, and its task metrics."""
    metrics = by_stage(events)
    rows = []
    for e in events:
        if e.get("Event") != "SparkListenerStageCompleted":
            continue
        info = e["Stage Info"]
        key = (info["Stage ID"], info.get("Stage Attempt ID", 0))
        m = metrics.get(key, dict.fromkeys(METRICS, 0))
        rows.append({
            "stage": info["Stage ID"],
            "attempt": info.get("Stage Attempt ID", 0),
            "name": info.get("Stage Name", ""),
            "planned_tasks": info.get("Number of Tasks", 0),
            "duration_ms": info.get("Completion Time", 0) - info.get("Submission Time", 0),
            **{k: m[k] for k in ("tasks", "shuffle_read_bytes", "shuffle_write_bytes",
                                 "memory_spilled_bytes", "disk_spilled_bytes")},
        })
    return sorted(rows, key=lambda r: (r["stage"], r["attempt"]))


def summarise_file(path: pathlib.Path | str) -> dict:
    return summarise(read_events(path))


def main() -> None:
    import sys

    for path in sys.argv[1:]:
        events = list(read_events(path))
        print(path)
        for k, v in summarise(events).items():
            print(f"  {k:<28} {v:>14,}")
        print(f"  {'stage':>5} {'tasks':>6} {'ms':>7} {'shuffle_w':>11} {'shuffle_r':>11}  name")
        for r in stage_table(events):
            print(f"  {r['stage']:>5} {r['tasks']:>6} {r['duration_ms']:>7} "
                  f"{r['shuffle_write_bytes']:>11,} {r['shuffle_read_bytes']:>11,}  {r['name'][:40]}")


if __name__ == "__main__":
    main()
