# Makefile for processing geographic data
.DELETE_ON_ERROR:

SOURCE := legacy
TABLE_NAME := stg_$(SOURCE)_pois

all: dist/$(SOURCE).csv

dist/$(SOURCE).csv: dump.py
	python3 dump.py > $@

import: dist/$(SOURCE).csv
	python3 ../import_pois.py --truncate $< $(TABLE_NAME)

unify:
	python3 ../unify_pois.py $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
