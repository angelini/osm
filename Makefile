PYTHON=python/venv/bin/python
TMP_DIR=/tmp/osm-root

DATA_CSV=data/csv
DATA_PARQUET=data/parquet
DATA_EXAMPLE=data/example

export RUST_BACKTRACE=1

.PHONY: setup-python setup-example run convert-csv

setup-python:

setup-example:
	rm -rf $(TMP_DIR)
	mkdir -p $(TMP_DIR)
	cp -r $(DATA_EXAMPLE) $(TMP_DIR)/example

run: setup-example
	cargo run

convert-csv:
	$(PYTHON) python/main.py yellow_tripdata_2020-01.csv yellow_tripdata_2020-02.csv yellow_tripdata_2020-03.csv

build-example:
	rm -rf $(DATA_EXAMPLE)
	for part in 2020-01 2020-02 2020-03 ; do \
		mkdir -p $(DATA_EXAMPLE)/nyc_taxis/date=$$part; \
		cp $(DATA_PARQUET)/yellow_tripdata_$$part.parquet $(DATA_EXAMPLE)/nyc_taxis/date=$$part/001.parquet; \
	done
