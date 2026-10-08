"""The benchmarked job: clean trips, aggregate by pickup zone and hour, join zone names.

The aggregation forces a shuffle on (zone, hour) and the join adds a second
one, which is where the runtime curve is expected to bend as data grows.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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
