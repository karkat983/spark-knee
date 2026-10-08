"""Profile the input: null rates per column and how many rows each cleaning rule drops.

    python -m src.profile
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import load_config, trip_paths
from src.data import load_trips
from src.spark_session import build_session


def null_rates(trips: DataFrame) -> dict[str, float]:
    total = trips.count()
    counts = trips.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in trips.columns]).first()
    return {c: counts[c] / total for c in trips.columns}


def rule_counts(trips: DataFrame, max_trip_hours: float, max_distance_miles: float) -> dict[str, int]:
    """Rows failing each cleaning rule on its own (a row can fail several rules)."""
    duration_s = F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")
    rules = {
        "missing timestamp or zone": F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull()
        | F.col("pickup_zone").isNull(),
        "duration <= 0": duration_s <= 0,
        f"duration > {max_trip_hours} h": duration_s > max_trip_hours * 3600,
        "distance <= 0": F.col("distance_mi") <= 0,
        f"distance > {max_distance_miles} mi": F.col("distance_mi") > max_distance_miles,
        "fare < 0": F.col("fare") < 0,
        "total <= 0": F.col("total") <= 0,
    }
    row = trips.select([F.sum(cond.cast("int")).alias(name) for name, cond in rules.items()]).first()
    return {name: int(row[name] or 0) for name in rules}


def main() -> None:
    cfg = load_config()
    spark = build_session(cfg["spark"])
    trips = load_trips(spark, trip_paths(cfg))
    total = trips.count()
    print(f"rows: {total:,}")
    print("\nnull rate per column:")
    for col, rate in null_rates(trips).items():
        print(f"  {col:<14} {rate:7.3%}")
    print("\nrows failing each rule:")
    for name, n in rule_counts(trips, cfg["job"]["max_trip_hours"], cfg["job"]["max_distance_miles"]).items():
        print(f"  {name:<24} {n:>9,}  ({n / total:.2%})")
    from src.job import clean_trips
    kept = clean_trips(trips, cfg["job"]["max_trip_hours"], cfg["job"]["max_distance_miles"]).count()
    print(f"\nkept after all rules: {kept:,} ({kept / total:.2%})")
    spark.stop()


if __name__ == "__main__":
    main()
