"""Load TLC trips and the zone lookup with explicit column types.

Only the columns the job needs are read. Types are cast explicitly because
TLC files from different months do not always agree (e.g. int vs double).
"""
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

ZONE_SCHEMA = StructType([
    StructField("LocationID", IntegerType(), nullable=False),
    StructField("Borough", StringType()),
    StructField("Zone", StringType()),
    StructField("service_zone", StringType()),
])


# The 8 source columns the job reads, with the type each is normalised to.
RAW_TYPES = {
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "PULocationID": "int",
    "DOLocationID": "int",
    "passenger_count": "double",
    "trip_distance": "double",
    "fare_amount": "double",
    "total_amount": "double",
}


def normalise_raw(df: DataFrame) -> DataFrame:
    """Keep only the job's source columns, under their original names, with uniform types."""
    return df.select([F.col(c).cast(t).alias(c) for c, t in RAW_TYPES.items()])


def select_trip_columns(df: DataFrame) -> DataFrame:
    """Keep and cast the columns the job uses, with short snake_case names."""
    return df.select(
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("PULocationID").cast("int").alias("pickup_zone"),
        F.col("DOLocationID").cast("int").alias("dropoff_zone"),
        F.col("passenger_count").cast("double").alias("passengers"),
        F.col("trip_distance").cast("double").alias("distance_mi"),
        F.col("fare_amount").cast("double").alias("fare"),
        F.col("total_amount").cast("double").alias("total"),
    )


def load_trips(spark: SparkSession, paths: Sequence[str]) -> DataFrame:
    """Read each file on its own, cast it to the job schema, then union them.

    Reading all files in one `spark.read.parquet(...)` call makes Spark take one file's schema
    for all of them, which fails or mis-reads when TLC changed a column's physical type
    between releases (e.g. passenger_count int64 in some years, double in others).
    """
    frames = [select_trip_columns(spark.read.parquet(str(p))) for p in paths]
    if not frames:
        raise ValueError("no input files")
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out


def load_zones(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(str(path), header=True, schema=ZONE_SCHEMA)
