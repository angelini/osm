TMP_DIR := /tmp/osm-root

PYTHON_REQUIREMENTS := python/requirements.txt
PYTHON_VENV := python/venv
PYTHON := $(PYTHON_VENV)/bin/python
PIP := $(PYTHON_VENV)/bin/pip

DATA_CSV := data/csv
DATA_PARQUET := data/parquet
DATA_EXAMPLE := data/example

CSV_FILES := $(wildcard $(DATA_CSV)/*.csv)
PARQUET_FILES := $(patsubst $(DATA_CSV)/%.csv,$(DATA_PARQUET)/%.parquet,$(CSV_FILES))

export RUST_BACKTRACE=1

.PHONY: setup-python build-example setup-example run

setup-python:
	python -m venv $(PYTHON_VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r $(PYTHON_REQUIREMENTS)

$(DATA_PARQUET)/%.parquet: $(DATA_CSV)/%.csv
	$(PYTHON) python/main.py $(notdir $(CSV_FILES))

build-example: $(PARQUET_FILES)
	rm -rf $(DATA_EXAMPLE)
	for part in 2020-01 2020-02 2020-03 ; do \
		mkdir -p $(DATA_EXAMPLE)/nyc_taxis/date=$$part; \
		cp $(DATA_PARQUET)/yellow_tripdata_$$part.parquet $(DATA_EXAMPLE)/nyc_taxis/date=$$part/001.parquet; \
	done

setup-example:
	rm -rf $(TMP_DIR)
	mkdir -p $(TMP_DIR)
	cp -r $(DATA_EXAMPLE) $(TMP_DIR)/example

run: setup-example
	cargo run
