TMP_DIR=/tmp/osm-root

.PHONY: setup-example run

setup-example:
	rm -rf $(TMP_DIR)
	mkdir -p $(TMP_DIR)
	cp -r example $(TMP_DIR)/example

run: setup-example
	cargo run
