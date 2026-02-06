# Makefile for processing geographic data

SOURCE := yamareco
TABLE_NAME := stg_$(SOURCE)_pois

all: dist/$(SOURCE).csv

dist/$(SOURCE).csv: archive/$(SOURCE).tsv ../tsv2csv.py
	python3 ../tsv2csv.py $(SOURCE) $< > $@

archive/$(SOURCE).tsv:
	@echo "please run the crawler to generate '$@'."
	@exit 1

import: dist/$(SOURCE).csv
	python3 ../import_pois.py --truncate $< $(TABLE_NAME)

unify:
	python3 ../unify_pois.py $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
