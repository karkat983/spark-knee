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


def summarise(events: Iterable[dict]) -> dict:
    out = dict.fromkeys(METRICS, 0)
    stages = set()
    for e in events:
        if e.get("Event") != "SparkListenerTaskEnd" or "Task Metrics" not in e:
            continue
        m = e["Task Metrics"]
        read = m.get("Shuffle Read Metrics", {})
        write = m.get("Shuffle Write Metrics", {})
        inp = m.get("Input Metrics", {})
        stages.add((e.get("Stage ID"), e.get("Stage Attempt ID", 0)))
        out["tasks"] += 1
        out["executor_run_time_ms"] += m.get("Executor Run Time", 0)
        out["gc_time_ms"] += m.get("JVM GC Time", 0)
        out["shuffle_read_bytes"] += read.get("Remote Bytes Read", 0) + read.get("Local Bytes Read", 0)
        out["shuffle_write_bytes"] += write.get("Shuffle Bytes Written", 0)
        out["memory_spilled_bytes"] += m.get("Memory Bytes Spilled", 0)
        out["disk_spilled_bytes"] += m.get("Disk Bytes Spilled", 0)
        out["peak_execution_memory_bytes"] = max(
            out["peak_execution_memory_bytes"], m.get("Peak Execution Memory", 0)
        )
        out["input_bytes"] += inp.get("Bytes Read", 0)
        out["input_records"] += inp.get("Records Read", 0)
    out["stages"] = len(stages)
    return out


def summarise_file(path: pathlib.Path | str) -> dict:
    return summarise(read_events(path))


def main() -> None:
    import sys

    for path in sys.argv[1:]:
        print(path)
        for k, v in summarise_file(path).items():
            print(f"  {k:<28} {v:>14,}")


if __name__ == "__main__":
    main()
