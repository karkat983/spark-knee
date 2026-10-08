from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

from src.job import build_job, clean_trips, hourly_zone_stats, with_zone_names

TRIP_SCHEMA = StructType([
    StructField("pickup_ts", TimestampType()),
    StructField("dropoff_ts", TimestampType()),
    StructField("pickup_zone", IntegerType()),
    StructField("dropoff_zone", IntegerType()),
    StructField("passengers", DoubleType()),
    StructField("distance_mi", DoubleType()),
    StructField("fare", DoubleType()),
    StructField("total", DoubleType()),
])
ZONE_SCHEMA = StructType([
    StructField("LocationID", IntegerType()),
    StructField("Borough", StringType()),
    StructField("Zone", StringType()),
    StructField("service_zone", StringType()),
])
JOB_CFG = {"max_trip_hours": 6, "max_distance_miles": 100}
# Timezone-aware so the test does not depend on the machine's local time zone.
T0 = datetime(2024, 1, 15, 8, 0, tzinfo=timezone.utc)


def trip(minutes=10, zone=1, distance=2.0, fare=10.0, total=12.0, start=T0):
    return (start, start + timedelta(minutes=minutes), zone, 2, 1.0, distance, fare, total)


def trips_df(spark, rows):
    return spark.createDataFrame(rows, TRIP_SCHEMA)


def zones_df(spark):
    return spark.createDataFrame(
        [(1, "Manhattan", "Alphabet City", "Yellow Zone"), (2, "Queens", "Astoria", "Boro Zone")],
        ZONE_SCHEMA,
    )


def test_clean_trips_keeps_valid_and_drops_bad_rows(spark):
    rows = [
        trip(),                         # valid
        trip(distance=0.0),             # no distance
        trip(minutes=-5),               # dropoff before pickup
        trip(minutes=7 * 60),           # longer than max_trip_hours
        trip(distance=500.0),           # longer than max_distance_miles
        trip(fare=-1.0),                # negative fare
        trip(total=0.0),                # no payment
        trip(zone=None),                # unknown pickup zone
    ]
    kept = clean_trips(trips_df(spark, rows), **JOB_CFG).collect()
    assert len(kept) == 1
    assert kept[0]["distance_mi"] == 2.0


def test_hourly_zone_stats_groups_by_zone_and_hour(spark):
    rows = [
        trip(zone=1, distance=2.0, total=10.0, minutes=10),
        trip(zone=1, distance=4.0, total=20.0, minutes=20),
        trip(zone=1, start=T0 + timedelta(hours=1)),
        trip(zone=2),
    ]
    stats = {(r["pickup_zone"], r["hour"]): r for r in hourly_zone_stats(trips_df(spark, rows)).collect()}
    assert set(stats) == {(1, 8), (1, 9), (2, 8)}
    first = stats[(1, 8)]
    assert first["trips"] == 2
    assert first["avg_distance_mi"] == pytest.approx(3.0)
    assert first["avg_duration_min"] == pytest.approx(15.0)
    assert first["revenue"] == pytest.approx(30.0)


def test_zone_names_join_keeps_unknown_zones(spark):
    stats = hourly_zone_stats(trips_df(spark, [trip(zone=1), trip(zone=99)]))
    named = {r["pickup_zone"]: r for r in with_zone_names(stats, zones_df(spark)).collect()}
    assert named[1]["borough"] == "Manhattan"
    assert named[1]["zone_name"] == "Alphabet City"
    assert named[99]["borough"] is None


def test_build_job_end_to_end(spark):
    rows = [trip(zone=1), trip(zone=2), trip(zone=2, distance=0.0)]
    out = build_job(trips_df(spark, rows), zones_df(spark), JOB_CFG).collect()
    assert sorted((r["borough"], r["trips"]) for r in out) == [("Manhattan", 1), ("Queens", 1)]


@pytest.mark.parametrize("minutes, kept", [(0, False), (1, True), (6 * 60, True), (6 * 60 + 1, False)])
def test_duration_bounds_are_exclusive_of_zero_and_inclusive_of_max(spark, minutes, kept):
    out = clean_trips(trips_df(spark, [trip(minutes=minutes)]), **JOB_CFG).count()
    assert out == (1 if kept else 0)


@pytest.mark.parametrize("distance, kept", [(0.01, True), (100.0, True), (100.01, False), (-1.0, False)])
def test_distance_bounds(spark, distance, kept):
    out = clean_trips(trips_df(spark, [trip(distance=distance)]), **JOB_CFG).count()
    assert out == (1 if kept else 0)


def test_zero_fare_is_kept_but_zero_total_is_not(spark):
    # A $0 metered fare with surcharges is a real (if odd) trip; a $0 total means nothing was charged.
    rows = [trip(fare=0.0, total=3.5), trip(fare=0.0, total=0.0)]
    assert clean_trips(trips_df(spark, rows), **JOB_CFG).count() == 1


def test_null_dropoff_is_dropped(spark):
    row = list(trip())
    row[1] = None
    assert clean_trips(trips_df(spark, [tuple(row)]), **JOB_CFG).count() == 0
