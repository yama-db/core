# Makefile for GSI GCP POIs
.DELETE_ON_ERROR:

TABLE_NAME := stg_gsi_gcp_pois

all: dist/gsi_gcp.tsv
	@echo "All required raw files are present."

dist:
	mkdir -p $@

dist/gsi_gcp.tsv: xml2tsv.py work/.extracted | dist
	python3 xml2tsv.py work/FG-GML-*.xml > $@

work/.extracted:
	./extract_xml.sh
	touch $@

import: dist/gsi_gcp.tsv import_gcp.py
	python3 import_gcp.py --truncate $(TABLE_NAME) $<

unify: unify_gcp.py
	python3 unify_gcp.py --truncate $(TABLE_NAME)

clean:
	rm -rf work/.extracted work/*

.PHONY: all import unify clean
