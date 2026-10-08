import logging

from src.logs import get_logger


def test_logger_is_namespaced_and_has_one_handler():
    a = get_logger("job")
    b = get_logger("sweep")
    assert a.name == "spark_knee.job"
    assert b.name == "spark_knee.sweep"
    assert len(logging.getLogger("spark_knee").handlers) == 1


def test_info_is_enabled_by_default_and_not_propagated():
    log = get_logger("job")
    assert log.isEnabledFor(logging.INFO)
    assert logging.getLogger("spark_knee").propagate is False
