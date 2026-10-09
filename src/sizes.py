"""Build sized copies of the trip data: 0.25x ... 16x, where 1x = one month (docs/decisions/001).

Up to six units come from the six real months. Beyond that, extra units are copies of real
months with every timestamp shifted forward by whole multiples of 364 days, which keeps the
weekday and hour of day of every trip unchanged (the job aggregates by hour of day).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SHIFT_DAYS = 364        # 52 weeks: same weekday, same hour


def shift_timestamps(df: DataFrame, copy: int) -> DataFrame:
    """Return df with both trip timestamps moved forward by `copy` x 364 days."""
    if copy == 0:
        return df
    delta = F.expr(f"INTERVAL {copy * SHIFT_DAYS} DAYS")
    return df.withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime") + delta).withColumn(
        "tpep_dropoff_datetime", F.col("tpep_dropoff_datetime") + delta
    )


def unit_plan(units: int, real_months: list[str]) -> list[tuple[str, int]]:
    """Which (month, copy number) makes up each unit of an N-unit dataset.

    copy 0 is the real month; copy k is that month shifted by k x 364 days.
    """
    if units < 1:
        raise ValueError("units must be >= 1")
    return [(real_months[i % len(real_months)], i // len(real_months)) for i in range(units)]


def copies_for_share(hot_share: float, target_share: float) -> int:
    """How many extra copies of the hot zone's rows lift its share from hot_share to target_share.

    With k extra copies the share is (1 + k) h / (1 + k h); solve for k and round up.
    """
    import math

    if not 0 < hot_share < target_share < 1:
        raise ValueError("need 0 < hot_share < target_share < 1")
    k = (target_share - hot_share) / (hot_share * (1 - target_share))
    return math.ceil(k - 1e-9)


def amplify_zone(df: DataFrame, zone: int, target_share: float) -> DataFrame:
    """Skew variant: add shifted copies of one pickup zone's rows until it holds ~target_share.

    Copies use the same whole-year shifts as replication, so no two rows are identical.
    """
    total = df.count()
    hot = df.where(F.col("PULocationID") == zone)
    k = copies_for_share(hot.count() / total, target_share)
    out = df
    for copy in range(1, k + 1):
        out = out.unionByName(shift_timestamps(hot, copy))
    return out
