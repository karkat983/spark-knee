"""Write the sized datasets to data/sized/size=<N>x/ (Parquet, the job's 8 columns only).

    python -m src.make_sizes                 # sizes from config.yaml
    python -m src.make_sizes --sizes 1 2 4
"""
import argparse
import time

from pyspark.sql import DataFrame, SparkSession

from src.config import load_config, resolve
from src.data import normalise_raw
from src.logs import get_logger
from src.sizes import shift_timestamps, unit_plan
from src.spark_session import build_session

log = get_logger("make_sizes")


def month_path(cfg: dict, month: str) -> str:
    return str(resolve(cfg["data"]["raw_dir"]) / f"yellow_tripdata_{month}.parquet")


def build_size(spark: SparkSession, cfg: dict, units: float) -> DataFrame:
    """Union of `units` month-units: real months first, then shifted copies.

    Fractional sizes below 1 are seeded random samples of the first real month.
    """
    if units < 1:
        first = normalise_raw(spark.read.parquet(month_path(cfg, cfg["sizes"]["real_months"][0])))
        return first.sample(fraction=units, seed=cfg["sizes"].get("sample_seed", 42))
    if units != int(units):
        raise ValueError("sizes above 1x must be whole numbers of months")
    frames = [
        shift_timestamps(normalise_raw(spark.read.parquet(month_path(cfg, month))), copy)
        for month, copy in unit_plan(int(units), cfg["sizes"]["real_months"])
    ]
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out


def file_count(cfg: dict, units: float) -> int:
    """Files per size grow with the size, so every file holds about the same amount of data
    (~1/files_per_unit of a month). Equal file sizes keep the read stage's task sizes constant."""
    return max(1, round(cfg["sizes"]["files_per_unit"] * units))


def size_dir(cfg: dict, label: str):
    return resolve(cfg["sizes"]["out_dir"]) / f"size={label}x"


def main() -> None:
    cfg = load_config()
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sizes", type=float, nargs="+", default=cfg["sizes"]["multiples"])
    args = parser.parse_args()

    spark = build_session(cfg["spark"], app_name="spark-knee-sizes")
    for units in args.sizes:
        start = time.perf_counter()
        out = size_dir(cfg, f"{units:g}")
        df = build_size(spark, cfg, units).repartition(file_count(cfg, units))
        df.write.mode("overwrite").parquet(str(out))
        log.info("size %gx -> %s in %.1fs", units, out, time.perf_counter() - start)
    spark.stop()


if __name__ == "__main__":
    main()
