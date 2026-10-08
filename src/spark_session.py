"""Build a local SparkSession from config, one place for every tuning knob."""
import os
import sys
from typing import Any

from pyspark.sql import SparkSession


def build_session(
    spark_cfg: dict[str, Any],
    app_name: str = "spark-knee",
    shuffle_partitions: int | None = None,
    event_log_dir: str | None = None,
) -> SparkSession:
    """Create (or reuse) a SparkSession.

    shuffle_partitions overrides the config value so sweeps can vary it per run.
    event_log_dir, when set, turns on Spark event logs for shuffle/spill metrics.
    """
    # Workers must run the same Python minor version as the driver; without this
    # Spark launches whatever "python3" is first on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    partitions = shuffle_partitions or spark_cfg["shuffle_partitions"]
    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_cfg["master"])
        .config("spark.driver.memory", spark_cfg["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if event_log_dir:
        # Spark 4 defaults to zstd-compressed, rolling event logs; plain single-file JSON lines
        # are easier to parse and small enough at these sizes.
        builder = (
            builder.config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", event_log_dir)
            .config("spark.eventLog.compress", "false")
            .config("spark.eventLog.rolling.enabled", "false")
        )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel(spark_cfg.get("log_level", "WARN"))
    return session
