# Common tasks. Run `make help` for a list. Needs JAVA_HOME pointing at JDK 17+ (docs/setup.md).
PYTHON ?= .venv/bin/python

.PHONY: help setup data job profile lint test clean

help:      ## list targets
	@grep -E '^[a-z]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-8s %s\n", $$1, $$2}'

setup:     ## create .venv (Python 3.12) and install dependencies
	python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt

data:      ## download the months in config.yaml plus the zone lookup
	$(PYTHON) scripts/download_tlc.py

job:       ## run the job once at 1x and print its runtime
	$(PYTHON) -m src.job

profile:   ## null rates and cleaning-rule counts
	$(PYTHON) -m src.profile

lint:      ## ruff lint
	$(PYTHON) -m ruff check .

test:      ## unit tests (local Spark, tiny in-memory data)
	$(PYTHON) -m pytest -q

clean:     ## delete job outputs and event logs
	rm -rf data/out data/events spark-warehouse
