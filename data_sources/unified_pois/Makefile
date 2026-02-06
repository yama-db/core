# Makefile for unified_pois data source

all: dist/unified_pois.csv

# Auxiliary file generated alongside the main unified POI data
AUX_FILE := dist/poi_hierarchies.csv
# Additional hierarchies to be registered
ADDITION := raw/poi_hierarchies_addition.csv

dist/unified_pois.csv:
	python3 gen_unified_pois.py $@ $(AUX_FILE)

import: dist/unified_pois.csv
	python3 import_unified_pois.py --truncate $< unified_pois
	python3 ../import_csv.py --truncate $(AUX_FILE) poi_hierarchies
	python3 ../import_csv.py $(ADDITION) poi_hierarchies

unify: ;

clean: ;

.PHONY: all import unify clean