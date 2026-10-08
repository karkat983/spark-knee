"""Download NYC TLC yellow-taxi trip files and the taxi-zone lookup into data/raw/.

Files are large (~50 MB per month) and gitignored.

    python scripts/download_tlc.py                 # months from config.yaml
    python scripts/download_tlc.py --months 2024-01 2024-02
"""
import argparse
import hashlib
import pathlib
import sys
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from src.config import load_config, trip_paths, zones_path  # noqa: E402


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
    cfg = load_config()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--months", nargs="+", default=cfg["data"]["months"])
    args = parser.parse_args()

    targets = [(cfg["data"]["zones_url"], zones_path(cfg))]
    for path in trip_paths(cfg, args.months):
        targets.append((f"{cfg['data']['base_url']}/{path.name}", path))

    for url, dest in targets:
        digest = download(url, dest)
        print(f"{dest.name:<36} {dest.stat().st_size:>12,} bytes  sha256 {digest[:16]}")


if __name__ == "__main__":
    main()
