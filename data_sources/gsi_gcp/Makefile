# Makefile for GSI GCP POIs

TABLE_NAME := stg_gsi_gcp_pois

FG_GML_FILES := $(shell cat FG-GML-file-list.txt)

all: dist/gsi_gcp.csv
	@echo "All required raw files are present."

dist/gsi_gcp.csv: xml2csv.py work/.extracted
	python3 xml2csv.py work/FG-GML-*.xml > $@

work/.extracted: $(FG_GML_FILES)
	./extract.sh
	touch $@

$(FG_GML_FILES):
	@echo "Please download '$(@F)'."
	@exit 1

import: dist/gsi_gcp.csv
	python3 ../import_pois.py --truncate $< $(TABLE_NAME)

unify:
	python3 unify_gcp.py $(TABLE_NAME)

clean:
	rm -rf work/.extracted work/*

.PHONY: all import unify clean
