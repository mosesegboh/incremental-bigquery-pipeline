PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PIPELINE_FLAGS ?=

.PHONY: run run-fast test reset reset-local gcp-test

run:
	PYTHON=$(PYTHON) bash scripts/gcp_bigquery_test.sh all

run-fast:
	$(PYTHON) main/task_1_data_cleaner.py --no-production-layers $(PIPELINE_FLAGS)

test:
	$(PYTHON) -m pytest -q

reset:
	PYTHON=$(PYTHON) bash scripts/gcp_bigquery_test.sh reset

reset-local:
	$(PYTHON) main/task_1_data_cleaner.py --reset-processing-dir --reset-only $(PIPELINE_FLAGS)

gcp-test:
	PYTHON=$(PYTHON) bash scripts/gcp_bigquery_test.sh all
