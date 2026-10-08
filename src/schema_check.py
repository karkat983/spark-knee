"""Check that every input month has the columns the job needs, with compatible types.

    python -m src.schema_check            # months from config.yaml
    python -m src.schema_check data/raw/yellow_tripdata_2023-*.parquet

Reads only Parquet footers (no data), so it is instant even for many files.
"""
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from src.config import load_config, trip_paths

# Column -> the kind of type the job can cast from.
REQUIRED = {
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "PULocationID": "integer",
    "DOLocationID": "integer",
    "passenger_count": "number",
    "trip_distance": "number",
    "fare_amount": "number",
    "total_amount": "number",
}


def kind_ok(arrow_type: pa.DataType, kind: str) -> bool:
    if kind == "timestamp":
        return pa.types.is_timestamp(arrow_type)
    if kind == "integer":
        return pa.types.is_integer(arrow_type)
    return pa.types.is_integer(arrow_type) or pa.types.is_floating(arrow_type)


def check_file(path) -> list[str]:
    """Problems found in one file's schema (empty list = fine)."""
    schema = pq.read_schema(path)
    problems = []
    for column, kind in REQUIRED.items():
        if column not in schema.names:
            problems.append(f"missing column {column}")
        elif not kind_ok(schema.field(column).type, kind):
            problems.append(f"{column} is {schema.field(column).type}, expected {kind}")
    return problems


def column_types(paths) -> dict[str, set[str]]:
    """For each required column, the distinct physical types seen across files (drift report)."""
    seen: dict[str, set[str]] = {c: set() for c in REQUIRED}
    for path in paths:
        schema = pq.read_schema(path)
        for column in REQUIRED:
            if column in schema.names:
                seen[column].add(str(schema.field(column).type))
    return seen


def main() -> None:
    paths = sys.argv[1:] or [str(p) for p in trip_paths(load_config(), None)]
    bad = 0
    for path in paths:
        problems = check_file(path)
        bad += bool(problems)
        print(f"{'OK ' if not problems else 'BAD'} {path}" + "".join(f"\n    {p}" for p in problems))
    for column, types in column_types(paths).items():
        if len(types) > 1:
            print(f"type drift in {column}: {sorted(types)} (cast in src/data.py)")
    raise SystemExit(1 if bad else 0)


if __name__ == "__main__":
    main()
