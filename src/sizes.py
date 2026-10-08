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
