# Makefile for GSI VTEXP POIs

TABLE_NAME := stg_gsi_vtexp_pois

all: dist/gsi_vtexp.csv

dist/gsi_vtexp.csv: extract_features.py archive/.downloaded
	python3 extract_features.py archive/tiles/ > $@

archive/.downloaded:
	@echo "Please run './get_tiles.sh' to download VTEXP tiles."
	@echo "It may take a long time and a large amount of disk space."
	@exit 1

import: dist/gsi_vtexp.csv
	python3 ../import_pois.py --truncate $< $(TABLE_NAME)

unify:
	python3 ../unify_pois.py $(TABLE_NAME)

clean: ;

.PHONY: all clean import unify
