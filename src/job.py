"""The benchmarked job: clean trips, aggregate by pickup zone and hour, join zone names.

The aggregation forces a shuffle on (zone, hour) and the join adds a second
one, which is where the runtime curve is expected to bend as data grows.

    python -m src.job                     # months from config.yaml
    python -m src.job --partitions 128
"""
import argparse
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import load_config, trip_paths, zones_path
from src.data import load_trips, load_zones
from src.spark_session import build_session


def clean_trips(trips: DataFrame, max_trip_hours: float, max_distance_miles: float) -> DataFrame:
    """Drop records that are clearly not real trips."""
    duration_s = F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")
    return trips.where(
        F.col("pickup_ts").isNotNull()
        & F.col("dropoff_ts").isNotNull()
        & F.col("pickup_zone").isNotNull()
        & (duration_s > 0)
        & (duration_s <= max_trip_hours * 3600)
        & (F.col("distance_mi") > 0)
        & (F.col("distance_mi") <= max_distance_miles)
        & (F.col("fare") >= 0)
        & (F.col("total") > 0)
    )


def hourly_zone_stats(trips: DataFrame) -> DataFrame:
    """Trips, average distance/duration and revenue per (pickup zone, hour of day)."""
    duration_min = (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0
    return (
        trips.withColumn("hour", F.hour("pickup_ts"))
        .groupBy("pickup_zone", "hour")
        .agg(
            F.count("*").alias("trips"),
            F.avg("distance_mi").alias("avg_distance_mi"),
            F.avg(duration_min).alias("avg_duration_min"),
            F.sum("total").alias("revenue"),
        )
    )


def with_zone_names(stats: DataFrame, zones: DataFrame) -> DataFrame:
    """Attach borough and zone name; unknown zone IDs are kept with null names."""
    z = zones.select(
        F.col("LocationID").alias("pickup_zone"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone_name"),
    )
    return stats.join(z, on="pickup_zone", how="left")


def build_job(trips: DataFrame, zones: DataFrame, job_cfg: dict) -> DataFrame:
    cleaned = clean_trips(trips, job_cfg["max_trip_hours"], job_cfg["max_distance_miles"])
    return with_zone_names(hourly_zone_stats(cleaned), zones)


def main() -> None:
    cfg = load_config()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--months", nargs="+", default=cfg["data"]["months"])
    parser.add_argument("--partitions", type=int, default=cfg["spark"]["shuffle_partitions"])
    args = parser.parse_args()

    spark = build_session(cfg["spark"], shuffle_partitions=args.partitions)
    trips = load_trips(spark, trip_paths(cfg, args.months))
    zones = load_zones(spark, zones_path(cfg))

    start = time.perf_counter()
    rows = build_job(trips, zones, cfg["job"]).collect()   # output is small (zones x 24 hours)
    elapsed = time.perf_counter() - start

    print(f"months={','.join(args.months)} partitions={args.partitions} "
          f"rows={len(rows)} runtime_s={elapsed:.2f}")
    spark.stop()


if __name__ == "__main__":
    main()
