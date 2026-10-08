"""Download NYC TLC yellow-taxi trip files and the taxi-zone lookup into data/raw/.

Files are large (~50 MB per month) and gitignored.

    python scripts/download_tlc.py                 # months from config.yaml
    python scripts/download_tlc.py --months 2024-01 2024-02
"""
import argparse
import hashlib
import pathlib
import urllib.request

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent


def download(url: str, dest: pathlib.Path) -> str:
    """Stream url to dest (skipping if present) and return the file's SHA-256."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        tmp = dest.with_suffix(dest.suffix + ".part")
        with urllib.request.urlopen(url) as resp, open(tmp, "wb") as out:
            while chunk := resp.read(1 << 20):
                out.write(chunk)
        tmp.replace(dest)
    return hashlib.sha256(dest.read_bytes()).hexdigest()


def main() -> None:
    cfg = yaml.safe_load((ROOT / "config.yaml").read_text())["data"]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--months", nargs="+", default=cfg["months"])
    args = parser.parse_args()

    raw = ROOT / cfg["raw_dir"]
    targets = [(cfg["zones_url"], raw / "taxi_zone_lookup.csv")]
    for month in args.months:
        name = f"yellow_tripdata_{month}.parquet"
        targets.append((f"{cfg['base_url']}/{name}", raw / name))

    for url, dest in targets:
        digest = download(url, dest)
        print(f"{dest.name:<36} {dest.stat().st_size:>12,} bytes  sha256 {digest[:16]}")


if __name__ == "__main__":
    main()
