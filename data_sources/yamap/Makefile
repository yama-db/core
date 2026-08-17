# Makefile for processing geographic data
.DELETE_ON_ERROR:

SOURCE := yamap
TABLE_NAME := stg_$(SOURCE)_pois

all: dist/$(SOURCE).tsv

dist work:
	mkdir -p $@

dist/$(SOURCE).tsv: work/$(SOURCE)_fixed.tsv ../yama2tsv.py | dist
	python3 ../yama2tsv.py $< > $@

../yama2tsv.py: ../../shared/extract_aliases.py

work/$(SOURCE).tsv: archive/$(SOURCE).tsv ../tsv_region.py | work
	python3 ../tsv_region.py $< > $@

work/$(SOURCE)_fixed.tsv: work/$(SOURCE).tsv error_collections.sed
	sed -f $(word 2, $^) $< > $@

archive/$(SOURCE).tsv:
	@echo "please run the crawler to generate '$@'."
	@exit 1

import: dist/$(SOURCE).tsv
	python3 ../import_pois.py --truncate $(TABLE_NAME) $<

unify:
	python3 ../unify_pois.py --truncate $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
