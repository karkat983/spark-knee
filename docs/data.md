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
| `taxi_zone_lookup.csv` | 12,331 | 265 | `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed` |

The January 2024 file is Parquet with 3 row groups and 19 columns. Timestamps are stored
without a time zone (`timestamp[us]`), i.e. as New York wall-clock time; the job runs Spark with
`spark.sql.session.timeZone=UTC` so that `hour()` returns that wall-clock hour unchanged.

**1x = this one month (~2.96 M trips, ~50 MB compressed).** Larger sizes are built from more
months and replication (see the sized-datasets phase of the plan).
