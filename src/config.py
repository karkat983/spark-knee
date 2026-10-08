"""Load config.yaml and resolve paths relative to the repo root."""
import pathlib
from typing import Any

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "config.yaml"


def load_config(path: pathlib.Path | str = DEFAULT_CONFIG) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


def resolve(relative: str) -> pathlib.Path:
    """Return a repo-relative path from the config as an absolute path."""
    return ROOT / relative


def trip_paths(cfg: dict[str, Any], months: list[str] | None = None) -> list[pathlib.Path]:
    raw = resolve(cfg["data"]["raw_dir"])
    return [raw / f"yellow_tripdata_{m}.parquet" for m in (months or cfg["data"]["months"])]


def zones_path(cfg: dict[str, Any]) -> pathlib.Path:
    return resolve(cfg["data"]["raw_dir"]) / "taxi_zone_lookup.csv"
