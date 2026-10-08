from datetime import datetime

import pytest

from src.sizes import SHIFT_DAYS, shift_timestamps, unit_plan

MONTHS = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]


def test_unit_plan_uses_real_months_first_then_copies():
    assert unit_plan(4, MONTHS) == [("2024-01", 0), ("2024-02", 0), ("2024-03", 0), ("2024-04", 0)]
    plan16 = unit_plan(16, MONTHS)
    assert plan16[:6] == [(m, 0) for m in MONTHS]
    assert plan16[6:8] == [("2024-01", 1), ("2024-02", 1)]
    assert plan16[-1] == ("2024-04", 2)
    assert len(set(plan16)) == 16                       # no unit is used twice


def test_unit_plan_rejects_zero():
    with pytest.raises(ValueError):
        unit_plan(0, MONTHS)


def test_shift_keeps_weekday_hour_and_duration(spark):
    df = spark.createDataFrame(
        [(datetime(2024, 1, 15, 8, 30), datetime(2024, 1, 15, 8, 50))],
        "tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp",
    )
    (row,) = shift_timestamps(df, 2).collect()
    pickup, dropoff = row
    assert (pickup - datetime(2024, 1, 15, 8, 30)).days == 2 * SHIFT_DAYS
    assert pickup.weekday() == datetime(2024, 1, 15).weekday()
    assert pickup.hour == 8
    assert (dropoff - pickup).seconds == 20 * 60


@pytest.mark.parametrize("units, files", [(0.25, 1), (0.5, 2), (1, 4), (2, 8), (16, 64)])
def test_file_count_grows_with_size(units, files):
    from src.make_sizes import file_count

    assert file_count({"sizes": {"files_per_unit": 4}}, units) == files
