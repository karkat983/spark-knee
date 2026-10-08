# Data

## NYC TLC yellow-taxi trip records

| Field | Value |
|-------|-------|
| Publisher | NYC Taxi & Limousine Commission, [Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| Licence / terms | Public NYC Open Data; free to use |
| Downloaded | 2026-10-09 via `scripts/download_tlc.py` |

| File | Bytes | Rows | SHA-256 |
|------|-------|------|---------|
| `yellow_tripdata_2024-01.parquet` | 49,961,641 | 2,964,624 | `c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510` |
| `yellow_tripdata_2024-02.parquet` | 50,349,284 | 3,007,526 | `c76c43c18c6c6664080dd920baab4928988d5786a6b65980792ca7cd796f9f20` |
| `yellow_tripdata_2024-03.parquet` | 60,078,280 | 3,582,628 | `2d4cdc8fb96726cdd3803b13b02d2e61e71d45720aff0ebc693a8bdd1f249823` |
| `yellow_tripdata_2024-04.parquet` | 59,133,625 | 3,514,289 | `ce37f736c7cf9e92164c850ef2b502a8f7bb590fc1804b6edbcb80293da01b89` |
| `yellow_tripdata_2024-05.parquet` | 62,553,128 | 3,723,833 | `1974230be2b5a3ea92e47b5eb77f32dbf1de896d81ad8df4ab6faba52749ad23` |
| `yellow_tripdata_2024-06.parquet` | 59,859,922 | 3,539,193 | `677cf14c8347f745b583f012fbdba072334c6c4efa17bfd6a64369f2ba30329c` |
| `taxi_zone_lookup.csv` | 12,331 | 265 | `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed` |

January-June 2024: **20,332,093 trips**, 342 MB of Parquet. Months 2-6 were downloaded with
`python scripts/download_tlc.py --months 2024-02 2024-03 2024-04 2024-05 2024-06`.

The January 2024 file is Parquet with 3 row groups and 19 columns. Timestamps are stored
without a time zone (`timestamp[us]`), i.e. as New York wall-clock time; the job runs Spark with
`spark.sql.session.timeZone=UTC` so that `hour()` returns that wall-clock hour unchanged.

**1x = this one month (~2.96 M trips, ~50 MB compressed).** Larger sizes are built from more
months and replication (see the sized-datasets phase of the plan).

## Data dictionary (columns the job uses)

Descriptions paraphrase the TLC
[yellow-taxi data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).
The job renames columns to short snake_case names in `src/data.py`.

| Source column | Job column | Type in job | Meaning |
|---------------|------------|-------------|---------|
| `tpep_pickup_datetime` | `pickup_ts` | timestamp | When the meter was engaged (NY wall clock) |
| `tpep_dropoff_datetime` | `dropoff_ts` | timestamp | When the meter was disengaged |
| `PULocationID` | `pickup_zone` | int | TLC taxi zone (1-265) where the meter was engaged |
| `DOLocationID` | `dropoff_zone` | int | TLC taxi zone where the meter was disengaged |
| `passenger_count` | `passengers` | double | Driver-entered passenger count (often missing) |
| `trip_distance` | `distance_mi` | double | Trip distance in miles reported by the meter |
| `fare_amount` | `fare` | double | Time-and-distance fare from the meter, USD |
| `total_amount` | `total` | double | Total charged to the passenger, USD (excludes cash tips) |

Zone lookup (`taxi_zone_lookup.csv`): `LocationID`, `Borough`, `Zone`, `service_zone`.
IDs 264 and 265 are "Unknown"/"Outside of NYC" placeholders.

The other 11 trip columns (vendor, rate code, payment type, fee breakdown) are not read.
