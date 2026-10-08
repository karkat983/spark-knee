"""Python-side logging for scripts: one format, level from the SPARK_KNEE_LOG env var.

Loggers live under the "spark_knee" namespace with their own handler, so they print even if
another library has already configured the root logger.
"""
import logging
import os

FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
ROOT = "spark_knee"


def get_logger(name: str) -> logging.Logger:
    root = logging.getLogger(ROOT)
    if not any(h.get_name() == ROOT for h in root.handlers):     # add our handler exactly once
        handler = logging.StreamHandler()
        handler.set_name(ROOT)
        handler.setFormatter(logging.Formatter(FORMAT, datefmt="%H:%M:%S"))
        root.addHandler(handler)
        root.setLevel(os.environ.get("SPARK_KNEE_LOG", "INFO").upper())
        root.propagate = False
    logging.getLogger("py4j").setLevel(logging.WARNING)   # py4j logs every JVM call at INFO
    return logging.getLogger(f"{ROOT}.{name}" if not name.startswith(ROOT) else name)
