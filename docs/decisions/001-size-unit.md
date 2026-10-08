# ADR 001: 1x = one month of yellow-taxi trips

**Status:** accepted (2026-10-09)

## Context

The experiment measures runtime as data grows, so "size" needs a unit that is real, easy to
explain, and can be multiplied without changing what the data looks like.

## Decision

- **1x = January 2024**: 2,964,624 trips, ~50 MB of Parquet (8 job columns are read).
- Sizes are powers of two from 1x to 16x, plus 0.25x and 0.5x at the low end.
- Up to 6x, sizes use real, distinct months (January-June 2024, 20.3 M trips in total).
- Beyond the six real months, extra units are copies of real months with every timestamp
  shifted forward by whole multiples of 364 days (52 weeks), implemented in `src/sizes.py`.
- 0.25x and 0.5x are seeded random samples of January.

## Why a month

- A month is a natural partition of TLC data, so 1x, 2x, 4x are just more real files.
- Monthly volumes differ by up to ~25% (Jan 2.96 M vs May 3.72 M trips). Sizes are therefore
  reported in **rows and bytes**, not only as multiples, and the fit uses rows on the x-axis.

## Consequences

- Up to 4x the data is entirely real; 8x and 16x contain replicated trips. Replication keeps
  the distribution of zones and hours intact but adds no new distinct (zone, hour) keys, so the
  aggregation's output size stays the same while its input grows. That is the regime where
  shuffle volume is bounded by key cardinality, which is part of what the experiment examines.
