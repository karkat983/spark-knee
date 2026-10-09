"""Write the sized datasets to data/sized/size=<N>x/ (Parquet, the job's 8 columns only).

    python -m src.make_sizes                 # sizes from config.yaml
    python -m src.make_sizes --sizes 1 2 4
    python -m src.make_sizes --dry-run       # print what each size is made of, write nothing
    python -m src.make_sizes --files-per-unit 8 --out-dir /tmp/sized
"""
import argparse
import hashlib
import json
import os
import shutil
import time

from pyspark.sql import DataFrame, SparkSession

from src.config import load_config, resolve
from src.data import normalise_raw
from src.logs import get_logger
from src.sizes import amplify_zone, shift_timestamps, unit_plan
from src.spark_session import build_session

log = get_logger("make_sizes")


def validate_sizes_config(cfg: dict) -> None:
    """Fail fast on settings that would build wrong or misleading datasets."""
    sizes = cfg["sizes"]
    multiples = sizes["multiples"]
    if multiples != sorted(multiples) or len(set(multiples)) != len(multiples):
        raise ValueError("sizes.multiples must be strictly increasing")
    if any(m <= 0 or (m >= 1 and m != int(m)) for m in multiples):
        raise ValueError("sizes.multiples: fractions only below 1x, whole months above")
    if sizes["files_per_unit"] < 1:
        raise ValueError("sizes.files_per_unit must be >= 1")
    if not sizes["real_months"]:
        raise ValueError("sizes.real_months must list at least one month")
    missing = [m for m in sizes["real_months"] if not os.path.exists(month_path(cfg, m))]
    if missing:
        raise ValueError(f"raw files missing for months {missing} (python scripts/download_tlc.py)")
    skew = sizes.get("skew")
    if skew and not 0 < skew["target_share"] < 1:
        raise ValueError("sizes.skew.target_share must be between 0 and 1")


def month_path(cfg: dict, month: str) -> str:
    return str(resolve(cfg["data"]["raw_dir"]) / f"yellow_tripdata_{month}.parquet")


def build_size(spark: SparkSession, cfg: dict, units: float) -> DataFrame:
    """Union of `units` month-units: real months first, then shifted copies.

    Fractional sizes below 1 are seeded random samples of the first real month.
    """
    if units < 1:
        first = normalise_raw(spark.read.parquet(month_path(cfg, cfg["sizes"]["real_months"][0])))
        return first.sample(fraction=units, seed=cfg["sizes"].get("sample_seed", 42))
    if units != int(units):
        raise ValueError("sizes above 1x must be whole numbers of months")
    frames = [
        shift_timestamps(normalise_raw(spark.read.parquet(month_path(cfg, month))), copy)
        for month, copy in unit_plan(int(units), cfg["sizes"]["real_months"])
    ]
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out


# Re-written Parquet is ~1.4x the size of the TLC originals (round-robin rows compress less);
# 2x leaves headroom for Spark's temporary files during the write.
SPACE_FACTOR = 2.0


def bytes_needed(cfg: dict, units: float) -> int:
    first = month_path(cfg, cfg["sizes"]["real_months"][0])
    return int(os.path.getsize(first) * units * SPACE_FACTOR)


def check_disk(cfg: dict, units: float) -> None:
    """Refuse to start a build that would fill the disk."""
    out_root = resolve(cfg["sizes"]["out_dir"])
    out_root.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(out_root).free
    need = bytes_needed(cfg, units)
    if need > free:
        raise SystemExit(f"size {units:g}x needs ~{need / 1e9:.1f} GB but only {free / 1e9:.1f} GB is free")


def file_count(cfg: dict, units: float) -> int:
    """Files per size grow with the size, so every file holds about the same amount of data
    (~1/files_per_unit of a month). Equal file sizes keep the read stage's task sizes constant."""
    return max(1, round(cfg["sizes"]["files_per_unit"] * units))


def describe_plan(cfg: dict, units: float) -> str:
    """Human-readable recipe of one size, e.g. '2024-01, 2024-02, 2024-01+364d'."""
    months = cfg["sizes"]["real_months"]
    if units < 1:
        return f"{units:.0%} sample of {months[0]}"
    return ", ".join(m if c == 0 else f"{m}+{c * 364}d" for m, c in unit_plan(int(units), months))


def writer(df: DataFrame, cfg: dict):
    """DataFrameWriter with the configured Parquet compression (and, if set, row-group size)."""
    w = df.write.mode("overwrite").option("compression", cfg["sizes"].get("compression", "snappy"))
    block = cfg["sizes"].get("row_group_mb")
    if block:
        w = w.option("parquet.block.size", str(int(block * 1024 * 1024)))
    return w


def build_fingerprint(cfg: dict, units: float) -> str:
    """Hash of everything that determines a size's contents: recipe, input files, write options."""
    months = cfg["sizes"]["real_months"][: max(1, int(units))]
    inputs = {}
    for m in months:
        path = month_path(cfg, m)
        inputs[m] = [os.path.getsize(path), int(os.path.getmtime(path))]
    keys = ("files_per_unit", "compression", "row_group_mb", "sample_seed")
    spec = {"recipe": describe_plan(cfg, units), "inputs": inputs,
            "params": {k: cfg["sizes"].get(k) for k in keys}}
    return hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()


MANIFEST = "_manifest.json"


def is_current(out, fingerprint: str) -> bool:
    manifest = out / MANIFEST
    return manifest.exists() and json.loads(manifest.read_text()).get("fingerprint") == fingerprint


def size_dir(cfg: dict, label: str):
    return resolve(cfg["sizes"]["out_dir"]) / f"size={label}x"


def main() -> None:
    cfg = load_config()
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sizes", type=float, nargs="+", default=cfg["sizes"]["multiples"])
    parser.add_argument("--skew", action="store_true",
                        help="also write a skewed copy of each size (hot zone from config)")
    parser.add_argument("--files-per-unit", type=int, help="override sizes.files_per_unit")
    parser.add_argument("--out-dir", help="override sizes.out_dir")
    parser.add_argument("--dry-run", action="store_true", help="print the plan, build nothing")
    parser.add_argument("--force", action="store_true", help="rebuild even if a size is up to date")
    args = parser.parse_args()
    if args.files_per_unit:
        cfg["sizes"]["files_per_unit"] = args.files_per_unit
    if args.out_dir:
        cfg["sizes"]["out_dir"] = args.out_dir
    if args.dry_run:
        for units in args.sizes:
            print(f"{units:g}x -> {describe_plan(cfg, units)} ({file_count(cfg, units)} files)")
        return

    validate_sizes_config(cfg)
    event_dir = resolve(cfg["spark"]["event_log_dir"]) / "sizes"
    event_dir.mkdir(parents=True, exist_ok=True)
    spark = build_session(cfg["spark"], app_name="spark-knee-sizes", event_log_dir=event_dir.as_uri())
    seconds: dict[str, float] = {}
    for units in args.sizes:
        out = size_dir(cfg, f"{units:g}")
        fingerprint = build_fingerprint(cfg, units)
        if not args.force and is_current(out, fingerprint):
            log.info("size %gx is up to date, skipped (--force to rebuild)", units)
            continue
        check_disk(cfg, units)
        spark.sparkContext.setJobGroup(f"{units:g}x", f"build size {units:g}x")
        start = time.perf_counter()
        df = build_size(spark, cfg, units).repartition(file_count(cfg, units))
        writer(df, cfg).parquet(str(out))
        manifest = {"fingerprint": fingerprint, "recipe": describe_plan(cfg, units)}
        (out / MANIFEST).write_text(json.dumps(manifest))
        seconds[f"{units:g}x"] = time.perf_counter() - start
        log.info("size %gx -> %s in %.1fs", units, out, seconds[f"{units:g}x"])
        if args.skew:
            skew = cfg["sizes"]["skew"]
            skewed = amplify_zone(spark.read.parquet(str(out)), skew["zone"], skew["target_share"])
            skew_out = resolve(skew["out_dir"]) / f"size={units:g}x"
            writer(skewed.repartition(file_count(cfg, units)), cfg).parquet(str(skew_out))
            log.info("skewed %gx -> %s", units, skew_out)
    app_id = spark.sparkContext.applicationId
    spark.stop()
    if seconds:
        write_build_profile(event_dir / app_id, seconds)


def write_build_profile(event_log, seconds: dict[str, float]) -> None:
    """results/size_build.csv: time, peak task memory, spill and GC for building each size."""
    import csv

    from src.eventlog import by_job_group, read_events

    groups = by_job_group(list(read_events(event_log)))
    out = resolve("results/size_build.csv")
    fields = ["size", "seconds", "tasks", "peak_execution_mb", "spill_disk_mb", "gc_seconds"]
    rows = {}
    if out.exists():                                   # keep sizes not rebuilt this time
        with open(out, newline="") as f:
            rows = {r["size"]: r for r in csv.DictReader(f)}
    for label, secs in seconds.items():
        g = groups.get(label, {})
        rows[label] = {"size": label, "seconds": round(secs, 1), "tasks": g.get("tasks", 0),
                       "peak_execution_mb": round(g.get("peak_execution_memory_bytes", 0) / 2**20, 1),
                       "spill_disk_mb": round(g.get("disk_spilled_bytes", 0) / 2**20, 1),
                       "gc_seconds": round(g.get("gc_time_ms", 0) / 1000, 2)}
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(sorted(rows.values(), key=lambda r: float(r["size"].rstrip("x"))))
    log.info("build profile -> %s", out)


if __name__ == "__main__":
    main()
