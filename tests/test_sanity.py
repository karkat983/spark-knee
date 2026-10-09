from datetime import datetime, timedelta

from src.job import hourly_zone_stats
from src.sanity import check_totals, scale_factors, total_trips
from src.sizes import shift_timestamps
from tests.test_job import JOB_CFG, trip, trips_df, zones_df

RAW_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


def test_output_total_equals_cleaned_input(spark):
    rows = [trip(zone=1), trip(zone=2), trip(zone=2, distance=0.0), trip(zone=1, minutes=-3)]
    cleaned, out = check_totals(trips_df(spark, rows), zones_df(spark), JOB_CFG)
    assert cleaned == out == 2


def test_k_shifted_copies_scale_every_key_by_exactly_k(spark):
    base_rows = [trip(zone=z, start=datetime(2024, 1, 15, h, 5)) for z in (1, 2) for h in (8, 9, 17)]
    base = trips_df(spark, base_rows)
    # shift_timestamps works on raw column names; rename around it
    raw = base.withColumnRenamed("pickup_ts", RAW_COLS[0]).withColumnRenamed("dropoff_ts", RAW_COLS[1])
    copies = raw
    for k in (1, 2):
        copies = copies.unionByName(shift_timestamps(raw, k))
    tripled = copies.withColumnRenamed(RAW_COLS[0], "pickup_ts").withColumnRenamed(RAW_COLS[1], "dropoff_ts")
    s1, s3 = hourly_zone_stats(base), hourly_zone_stats(tripled)
    assert scale_factors(s1, s3) == {3.0}
    assert total_trips(s3) == 3 * total_trips(s1)


def test_scale_factors_detects_a_missing_key(spark):
    s1 = hourly_zone_stats(trips_df(spark, [trip(zone=1), trip(zone=2)]))
    later = datetime(2024, 1, 15, 8) + timedelta(days=364)
    s2 = hourly_zone_stats(trips_df(spark, [trip(zone=1, start=later)]))
    assert None in scale_factors(s1, s2)        # zone 2 vanished: ratio is null, not 1
