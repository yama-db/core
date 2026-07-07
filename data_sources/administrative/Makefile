# Makefile for Administrative Boundaries
.DELETE_ON_ERROR:

VERSION := 20260101

TABLE_REGIONS := administrative_regions
TABLE_BOUNDARIES := administrative_boundaries

all: dist/regions.csv work/N03-${VERSION}.geojson

dist work raw:
	mkdir -p $@

dist/regions.csv: raw/AdminiBoundary_CD.xlsx raw/query.csv gen_regions_csv.py | dist
	python3 gen_regions_csv.py $< $(word 2, $^) > $@

raw/AdminiBoundary_CD.xlsx:
	@echo "Please download '$(@F)'."
	@exit 1

raw/query.csv: query_jis_code.py | raw
	python3 query_jis_code.py > $@

work/N03-${VERSION}.geojson: raw/N03-${VERSION}_GML.zip | work
	unzip $< $(@F) -d $(@D)
	touch $@

raw/N03-${VERSION}_GML.zip:
	@echo "Please download '$(@F)'."
	@exit 1

import: dist/regions.csv work/N03-${VERSION}.geojson
	python3 ../import_csv.py --truncate $(TABLE_REGIONS) dist/regions.csv
	python3 import_boundaries.py --truncate $(TABLE_BOUNDARIES) work/N03-${VERSION}.geojson

unify: ;

clean:
	rm -rf work/*

.PHONY: all import unify clean
