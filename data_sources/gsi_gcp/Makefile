# Makefile for GSI GCP POIs
.DELETE_ON_ERROR:

TABLE_NAME := stg_gsi_gcp_pois

all: dist/gsi_gcp.csv
	@echo "All required raw files are present."

dist/gsi_gcp.csv: xml2csv.py work/.extracted
	python3 xml2csv.py work/FG-GML-*.xml > $@

work/.extracted:
	./extract.sh
	touch $@

import: dist/gsi_gcp.csv
	python3 ../import_pois.py --truncate $< $(TABLE_NAME)

unify:
	python3 unify_gcp.py $(TABLE_NAME)

clean:
	rm -rf work/.extracted work/*

.PHONY: all import unify clean
