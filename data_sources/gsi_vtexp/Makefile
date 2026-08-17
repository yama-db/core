# Makefile for GSI VTEXP POIs
.DELETE_ON_ERROR:

TABLE_NAME := stg_gsi_vtexp_pois

all: dist/gsi_vtexp.tsv

dist archive:
	mkdir -p $@

dist/gsi_vtexp.tsv: extract_features.py raw/gsi_vtexp_corrections.csv archive/.downloaded | dist
	python3 extract_features.py archive/tiles/ raw/gsi_vtexp_corrections.csv > $@

extract_features.py: ../../shared/extract_aliases.py

archive/.downloaded: | archive
	@echo "Please run './get_tiles.sh' to download VTEXP tiles."
	@echo "It may take a long time and a large amount of disk space."
	@exit 1

import: dist/gsi_vtexp.tsv
	python3 ../import_pois.py --truncate $(TABLE_NAME) $<

unify:
	python3 ../unify_pois.py --truncate $(TABLE_NAME)

clean: ;

.PHONY: all clean import unify
