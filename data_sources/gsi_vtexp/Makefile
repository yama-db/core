# Makefile for GSI VTEXP POIs

TABLE_NAME := stg_gsi_vtexp_pois

all: archive/mokuroku.csv.gz
	@echo "All required VTEXP files are present."
	@echo "You can run 'make regist' to generate and register the data."

archive/mokuroku.csv.gz:
	@echo "Please download mokuroku.csv.gz from GSI VTEXP site and place it in archive/ directory."
	@echo "run './get_tiles.sh' to download GSI VTEXP tiles."
	@exit 1

dist/gsi_vtexp.csv: extract_features.py
	python3 extract_features.py archive/tiles/ > $@

regist: dist/gsi_vtexp.csv
	python3 ../regist_pois.py --truncate $< $(TABLE_NAME)

unify:
	python3 ../unify_pois.py $(TABLE_NAME)

clean:
	rm -f dist/*

.PHONY: all clean regist unify
