# Makefile for unified_pois data source

all: dist/unified_pois.csv

# Auxiliary file generated alongside the main unified POI data
AUX_FILE := dist/poi_hierarchies.csv

dist/unified_pois.csv:
	python3 gen_unified_pois.py $@ $(AUX_FILE)
#	patch -b < unified_pois.csv.patch

regist: dist/unified_pois.csv
	python3 regist_unified_pois.py --truncate $< unified_pois
	python3 ../regist_csv.py --truncate $(AUX_FILE) poi_hierarchies

unify: ;

clean: ;

.PHONY: all regist unify clean
