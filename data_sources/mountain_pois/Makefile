# Makefile for unified_pois data source
.DELETE_ON_ERROR:

all: dist/mountain_pois.csv

dist:
	mkdir -p $@

# Auxiliary file generated alongside the main unified POI data
AUX_FILE := dist/poi_hierarchies.csv
# Additional hierarchies to be registered
ADDITION := raw/poi_hierarchies_addition.csv

dist/mountain_pois.csv: gen_mountain_pois.py raw/parent_pois.csv | dist
	python3 gen_mountain_pois.py $(word 2,$^) $@ $(AUX_FILE)

import: dist/mountain_pois.csv import_mountain_pois.py
	python3 import_mountain_pois.py --truncate mountain_pois $<
	python3 ../import_csv.py --truncate poi_hierarchies $(AUX_FILE)

unify: ;

clean: ;

.PHONY: all import unify clean
