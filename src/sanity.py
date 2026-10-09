"""Check that the job's output is consistent with its input at every size.

Two invariants:
1. Total trips in the hourly stats equals the number of rows that survive cleaning, and
2. a dataset made of k shifted copies of one month gives exactly k times every (zone, hour)
   count, because shifting by whole 52-week years keeps each trip's zone and hour.

    python -m src.sanity data/sized/size=1x
"""
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import load_config
from src.data import load_trips, load_zones
from src.job import build_job, clean_trips


def total_trips(stats: DataFrame) -> int:
    return int(stats.agg(F.sum("trips")).first()[0] or 0)


def check_totals(trips: DataFrame, zones: DataFrame, job_cfg: dict) -> tuple[int, int]:
    """(cleaned input rows, total trips in output); they must be equal."""
    cleaned = clean_trips(trips, job_cfg["max_trip_hours"], job_cfg["max_distance_miles"]).count()
    return cleaned, total_trips(build_job(trips, zones, job_cfg))


def scale_factors(base: DataFrame, scaled: DataFrame) -> set[float]:
    """Distinct ratios scaled.trips / base.trips over all (zone, hour) keys (one value if uniform)."""
    joined = base.select("pickup_zone", "hour", F.col("trips").alias("b")).join(
        scaled.select("pickup_zone", "hour", F.col("trips").alias("s")), ["pickup_zone", "hour"], "full"
    )
    return {r.ratio for r in joined.select((F.col("s") / F.col("b")).alias("ratio")).distinct().collect()}


def main() -> None:
    from src.config import zones_path
    from src.spark_session import build_session

    cfg = load_config()
    spark = build_session(cfg["spark"])
    zones = load_zones(spark, zones_path(cfg))
    for path in sys.argv[1:]:
        cleaned, out = check_totals(load_trips(spark, [path]), zones, cfg["job"])
        print(f"{'OK ' if cleaned == out else 'BAD'} {path}: cleaned rows {cleaned:,}, output trips {out:,}")
    spark.stop()


if __name__ == "__main__":
    main()
