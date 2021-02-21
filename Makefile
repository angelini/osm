PYTHON=python/venv/bin/python
TMP_DIR=/tmp/osm-root

.PHONY: setup-python setup-example run convert-csv

setup-python:

setup-example:
	rm -rf $(TMP_DIR)
	mkdir -p $(TMP_DIR)
	cp -r example $(TMP_DIR)/example

run: setup-example
	cargo run

convert-csv:
	$(PYTHON) python/main.py yellow_tripdata_2020-01.csv yellow_tripdata_2020-02.csv yellow_tripdata_2020-03.csv
