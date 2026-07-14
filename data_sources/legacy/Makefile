# Makefile for processing geographic data
.DELETE_ON_ERROR:

SOURCE := legacy
TABLE_NAME := stg_$(SOURCE)_pois

all: dist/$(SOURCE).tsv

dist:
	mkdir -p $@

dist/$(SOURCE).tsv: dump.py | dist
	python3 dump.py > $@

import: dist/$(SOURCE).tsv
	python3 ../import_pois.py --truncate $(TABLE_NAME) $<

unify:
	python3 ../unify_pois.py --truncate $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
