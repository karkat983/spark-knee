import pathlib

import pytest

from src.eventlog import METRICS, summarise, summarise_file

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
