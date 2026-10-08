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


def build_size(spark: SparkSession, cfg: dict, units: int) -> DataFrame:
    """Union of `units` month-units: real months first, then shifted copies."""
    frames = [
        shift_timestamps(normalise_raw(spark.read.parquet(month_path(cfg, month))), copy)
        for month, copy in unit_plan(units, cfg["sizes"]["real_months"])
    ]
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out


def size_dir(cfg: dict, label: str):
    return resolve(cfg["sizes"]["out_dir"]) / f"size={label}x"


def main() -> None:
    cfg = load_config()
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sizes", type=int, nargs="+", default=cfg["sizes"]["multiples"])
    args = parser.parse_args()

    spark = build_session(cfg["spark"], app_name="spark-knee-sizes")
    for units in args.sizes:
        start = time.perf_counter()
        out = size_dir(cfg, str(units))
        build_size(spark, cfg, units).write.mode("overwrite").parquet(str(out))
        log.info("size %dx -> %s in %.1fs", units, out, time.perf_counter() - start)
    spark.stop()


if __name__ == "__main__":
    main()
