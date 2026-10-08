from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.data import RAW_TYPES
from src.make_sizes import build_size

ROWS_PER_MONTH = {"2024-01": 3, "2024-02": 5}


@pytest.fixture
def cfg(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    for month, n in ROWS_PER_MONTH.items():
        m = int(month[-2:])
        pickups = [datetime(2024, m, 1, 8, i) for i in range(n)]
        dropoffs = [datetime(2024, m, 1, 9, i) for i in range(n)]
        pq.write_table(pa.table({
            "VendorID": pa.array([1] * n, pa.int32()),
            "tpep_pickup_datetime": pa.array(pickups, pa.timestamp("us")),
            "tpep_dropoff_datetime": pa.array(dropoffs, pa.timestamp("us")),
            "PULocationID": pa.array([1] * n, pa.int32()),
            "DOLocationID": pa.array([2] * n, pa.int32()),
            "passenger_count": pa.array([1] * n, pa.int64()),
            "trip_distance": pa.array([1.5] * n, pa.float64()),
            "fare_amount": pa.array([10.0] * n, pa.float64()),
            "total_amount": pa.array([12.0] * n, pa.float64()),
        }), raw / f"yellow_tripdata_{month}.parquet")
    return {"data": {"raw_dir": str(raw)}, "sizes": {"real_months": list(ROWS_PER_MONTH)}}


def test_replication_preserves_schema(spark, cfg):
    df = build_size(spark, cfg, 3)
    assert dict(df.dtypes) == RAW_TYPES
