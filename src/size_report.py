"""Rows, files and bytes of every sized dataset -> results/sizes.csv.

    python -m src.size_report

Reads Parquet footers only, so it takes well under a second.
"""
import csv
import pathlib
import re

import pyarrow.parquet as pq

from src.config import load_config, resolve

FIELDS = ["size", "rows", "files", "bytes", "rows_vs_1x"]


def size_of(label: str) -> float:
    return float(re.fullmatch(r"size=([\d.]+)x", label).group(1))


def describe(size_dir: pathlib.Path) -> dict:
    files = sorted(size_dir.glob("*.parquet"))
    return {
        "size": size_of(size_dir.name),
        "rows": sum(pq.ParquetFile(f).metadata.num_rows for f in files),
        "files": len(files),
        "bytes": sum(f.stat().st_size for f in files),
    }


def report(root: pathlib.Path) -> list[dict]:
    rows = sorted((describe(d) for d in root.glob("size=*x") if d.is_dir()), key=lambda r: r["size"])
    base = next((r["rows"] for r in rows if r["size"] == 1), None)
    for r in rows:
        r["rows_vs_1x"] = round(r["rows"] / base, 3) if base else ""
    return rows


def main() -> None:
    cfg = load_config()
    rows = report(resolve(cfg["sizes"]["out_dir"]))
    out = resolve("results/sizes.csv")
    out.parent.mkdir(exist_ok=True)
    with open(out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    for r in rows:
        mb = r["bytes"] / 1e6
        ratio = r["rows_vs_1x"]
        print(f"{r['size']:>6}x {r['rows']:>12,} rows {r['files']:>4} files {mb:>9.1f} MB  x{ratio}")


if __name__ == "__main__":
    main()
