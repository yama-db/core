# Makefile for processing geographic data
.DELETE_ON_ERROR:

SOURCE := yamareco
TABLE_NAME := stg_$(SOURCE)_pois

all: dist/$(SOURCE).tsv

dist/$(SOURCE).tsv: archive/$(SOURCE).tsv ../yama2tsv.py ../../shared/extract_aliases.py
	python3 ../yama2tsv.py $< > $@

archive/$(SOURCE).tsv:
	@echo "please run the crawler to generate '$@'."
	@exit 1

import: dist/$(SOURCE).tsv
	python3 ../import_pois.py --truncate $(TABLE_NAME) $<

unify:
	python3 ../unify_pois.py --truncate $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
