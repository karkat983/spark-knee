import pyarrow as pa
import pyarrow.parquet as pq

from src.schema_check import REQUIRED, check_file, column_types


def write(path, overrides=None, drop=()):
    fields = {
        "tpep_pickup_datetime": pa.timestamp("us"), "tpep_dropoff_datetime": pa.timestamp("us"),
        "PULocationID": pa.int32(), "DOLocationID": pa.int32(), "passenger_count": pa.int64(),
        "trip_distance": pa.float64(), "fare_amount": pa.float64(), "total_amount": pa.float64(),
    }
    fields.update(overrides or {})
    schema = pa.schema([(n, t) for n, t in fields.items() if n not in drop])
    pq.write_table(schema.empty_table(), path)
    return path


def test_good_file_has_no_problems(tmp_path):
    assert check_file(write(tmp_path / "a.parquet")) == []


def test_missing_and_wrong_type_are_reported(tmp_path):
    path = write(tmp_path / "b.parquet", overrides={"PULocationID": pa.string()}, drop=("fare_amount",))
    problems = check_file(path)
    assert "missing column fare_amount" in problems
    assert any(p.startswith("PULocationID is string") for p in problems)


def test_drift_report_lists_distinct_types(tmp_path):
    a = write(tmp_path / "a.parquet")
    b = write(tmp_path / "b.parquet", overrides={"passenger_count": pa.float64()})
    types = column_types([a, b])
    assert types["passenger_count"] == {"int64", "double"}
    assert all(len(types[c]) == 1 for c in REQUIRED if c != "passenger_count")
