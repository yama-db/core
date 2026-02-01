# Makefile for GSI GCP POIs

TABLE_NAME := stg_gsi_gcp_pois

ZIP_FILES := $(addprefix archive/, $(shell cat ZIP_FILES))

all: $(ZIP_FILES)
	@echo "All required GCP ZIP files are present."
	@echo "You can run 'make regist' to generate and register the data."

$(ZIP_FILES):
	@echo "Please download the required GCP ZIP files from GSI and place them in archive/ directory."
	@exit 1

dist/gsi_gcp.csv: work/.extracted
	python3 xml2csv.py work/FG-GML-*.xml > $@

work/.extracted:
	./extract.sh
	touch $@

regist: dist/gsi_gcp.csv
	python3 ../regist_pois.py --truncate $< $(TABLE_NAME)

unify:
	python3 unify_gcp.py $(TABLE_NAME)

clean:
	rm -rf dist/* work/*

.PHONY: all regist unify clean
