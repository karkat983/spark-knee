import pathlib

import pytest

from src.eventlog import (
    METRICS,
    by_job_group,
    by_stage,
    read_events,
    stage_table,
    summarise,
    summarise_file,
)

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "eventlog_small.jsonl"


def test_sums_task_metrics():
    m = summarise_file(FIXTURE)
    assert m["tasks"] == 3                       # the metric-less task is skipped
    assert m["stages"] == 2
    assert m["executor_run_time_ms"] == 520
    assert m["gc_time_ms"] == 32
    assert m["shuffle_write_bytes"] == 2500
    assert m["shuffle_read_bytes"] == 2500       # remote 400 + local 2100
    assert m["memory_spilled_bytes"] == 8192
    assert m["disk_spilled_bytes"] == 1024
    assert m["input_bytes"] == 11000
    assert m["input_records"] == 110


def test_peak_memory_is_max_not_sum():
    assert summarise_file(FIXTURE)["peak_execution_memory_bytes"] == 9000


def test_empty_log_gives_zeros():
    assert summarise([]) == dict.fromkeys(METRICS, 0)


@pytest.mark.parametrize("key", METRICS)
def test_every_metric_present(key):
    assert key in summarise_file(FIXTURE)


def test_by_stage_splits_and_sums_back_to_total():
    stages = by_stage(read_events(FIXTURE))
    assert set(stages) == {(0, 0), (1, 0)}
    assert stages[(0, 0)]["tasks"] == 2
    assert stages[(0, 0)]["shuffle_write_bytes"] == 2500
    assert stages[(1, 0)]["shuffle_read_bytes"] == 2500
    total = summarise_file(FIXTURE)
    for key in ("tasks", "executor_run_time_ms", "shuffle_write_bytes", "disk_spilled_bytes"):
        assert sum(m[key] for m in stages.values()) == total[key]


def test_stage_table_has_task_counts_and_durations():
    rows = stage_table(list(read_events(FIXTURE)))
    assert [(r["stage"], r["name"], r["planned_tasks"], r["tasks"], r["duration_ms"]) for r in rows] == [
        (0, "scan", 2, 2, 250),
        (1, "aggregate", 2, 1, 400),      # one task ended without metrics (failed)
    ]


def test_by_job_group_splits_one_application():
    def submitted(stage, group):
        return {"Event": "SparkListenerStageSubmitted", "Stage Info": {"Stage ID": stage},
                "Properties": {"spark.jobGroup.id": group}}

    events = list(read_events(FIXTURE)) + [submitted(0, "1x"), submitted(1, "2x")]
    groups = by_job_group(events)
    assert groups["1x"]["tasks"] == 2 and groups["1x"]["shuffle_write_bytes"] == 2500
    assert groups["2x"]["tasks"] == 1 and groups["2x"]["disk_spilled_bytes"] == 1024
