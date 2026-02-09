# Makefile for Administrative Boundaries
.DELETE_ON_ERROR:

REVISION := 20250101

TABLE_REGIONS := administrative_regions
TABLE_BOUNDARIES := administrative_boundaries

all: dist/regions.csv work/N03-$(REVISION).geojson

dist/regions.csv: raw/AdminiBoundary_CD.xlsx raw/query.csv
	python3 gen_regions_csv.py $^ > $@

raw/AdminiBoundary_CD.xlsx:
	@echo "Please download '$(@F)'."
	@exit 1

raw/query.csv:
	python3 query_jis_code.py > $@

work/N03-$(REVISION).geojson: raw/N03-$(REVISION)_GML.zip
	unzip $< $(@F) -d $(@D)

raw/N03-$(REVISION)_GML.zip:
	@echo "Please download '$(@F)'."
	@exit 1

import: dist/regions.csv work/N03-$(REVISION).geojson
	python3 ../import_csv.py --truncate dist/regions.csv $(TABLE_REGIONS)
	python3 import_boundaries.py --truncate work/N03-$(REVISION).geojson $(TABLE_BOUNDARIES)

unify: ;

clean:
	rm -rf work/*

.PHONY: all import unify clean
