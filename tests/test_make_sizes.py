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


@pytest.mark.parametrize("units, expected", [(1, 3), (2, 8), (3, 11), (4, 16)])
def test_row_counts_add_up_per_unit(spark, cfg, units, expected):
    # units cycle through the real months: Jan(3), Feb(5), Jan copy(3), Feb copy(5)
    assert build_size(spark, cfg, units).count() == expected


def test_copies_do_not_duplicate_rows(spark, cfg):
    df = build_size(spark, cfg, 4)
    assert df.distinct().count() == df.count()


def test_fractional_size_samples_the_first_month(spark, cfg):
    cfg["sizes"]["sample_seed"] = 1
    df = build_size(spark, cfg, 0.5)
    assert dict(df.dtypes) == RAW_TYPES
    assert 0 <= df.count() <= ROWS_PER_MONTH["2024-01"]
    assert df.count() == build_size(spark, cfg, 0.5).count()      # seeded: same sample


def test_fractional_size_above_one_is_rejected(spark, cfg):
    with pytest.raises(ValueError):
        build_size(spark, cfg, 1.5)


def test_disk_check_refuses_when_space_is_short(cfg, tmp_path, monkeypatch):
    import shutil

    from src import make_sizes

    cfg["sizes"]["out_dir"] = str(tmp_path / "sized")      # absolute, so resolve() keeps it
    need = make_sizes.bytes_needed(cfg, 16)
    monkeypatch.setattr(shutil, "disk_usage", lambda p: shutil._ntuple_diskusage(10**12, 0, need - 1))
    with pytest.raises(SystemExit, match="needs"):
        make_sizes.check_disk(cfg, 16)
    monkeypatch.setattr(shutil, "disk_usage", lambda p: shutil._ntuple_diskusage(10**12, 0, need + 1))
    make_sizes.check_disk(cfg, 16)


def good_sizes_cfg(cfg):
    cfg["sizes"].update({"multiples": [0.5, 1, 2], "files_per_unit": 4,
                         "skew": {"zone": 132, "target_share": 0.5}})
    return cfg


def test_valid_size_settings_pass(cfg):
    from src.make_sizes import validate_sizes_config

    validate_sizes_config(good_sizes_cfg(cfg))


@pytest.mark.parametrize("patch", [
    {"multiples": [2, 1]},
    {"multiples": [1, 1.5]},
    {"multiples": [0, 1]},
    {"files_per_unit": 0},
    {"real_months": ["2024-01", "2031-01"]},
    {"skew": {"zone": 132, "target_share": 1.5}},
])
def test_bad_size_settings_fail(cfg, patch):
    from src.make_sizes import validate_sizes_config

    cfg = good_sizes_cfg(cfg)
    cfg["sizes"].update(patch)
    with pytest.raises(ValueError):
        validate_sizes_config(cfg)


def test_committed_config_is_valid():
    from src.config import load_config
    from src.make_sizes import validate_sizes_config

    cfg = load_config()
    try:
        validate_sizes_config(cfg)
    except ValueError as e:
        if "raw files missing" not in str(e):     # CI has no downloads; everything else must pass
            raise
