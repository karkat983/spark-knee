from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.data import load_trips


def write_month(path, passenger_type, pickup_unit="us"):
    table = pa.table({
        "tpep_pickup_datetime": pa.array([datetime(2024, 1, 1, 8)], pa.timestamp(pickup_unit)),
        "tpep_dropoff_datetime": pa.array([datetime(2024, 1, 1, 8, 20)], pa.timestamp("us")),
        "PULocationID": pa.array([1], pa.int32()),
        "DOLocationID": pa.array([2], pa.int64()),
        "passenger_count": pa.array([2], passenger_type),
        "trip_distance": pa.array([3.5], pa.float64()),
        "fare_amount": pa.array([12], pa.int64()),          # an integer fare must cast too
        "total_amount": pa.array([15.0], pa.float64()),
        "VendorID": pa.array([1], pa.int32()),             # extra columns are ignored
    })
    pq.write_table(table, path)
    return str(path)


def test_months_with_different_physical_types_union_cleanly(spark, tmp_path):
    a = write_month(tmp_path / "a.parquet", pa.int64())
    b = write_month(tmp_path / "b.parquet", pa.float64(), pickup_unit="ms")
    df = load_trips(spark, [a, b])
    assert dict(df.dtypes) == {
        "pickup_ts": "timestamp", "dropoff_ts": "timestamp", "pickup_zone": "int", "dropoff_zone": "int",
        "passengers": "double", "distance_mi": "double", "fare": "double", "total": "double",
    }
    rows = df.collect()
    assert len(rows) == 2
    assert {r["passengers"] for r in rows} == {2.0}
    assert {r["fare"] for r in rows} == {12.0}


def test_no_files_is_an_error(spark):
    with pytest.raises(ValueError):
        load_trips(spark, [])
