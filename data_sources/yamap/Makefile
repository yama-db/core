# Makefile for processing geographic data
.DELETE_ON_ERROR:

SOURCE := yamap
TABLE_NAME := stg_$(SOURCE)_pois

all: dist/$(SOURCE).tsv

dist work:
	mkdir -p $@

dist/$(SOURCE).tsv: work/$(SOURCE).tsv ../yama2tsv.py | dist
	python3 ../yama2tsv.py $< > $@

../yama2tsv.py: ../../shared/extract_aliases.py

work/$(SOURCE).tsv: archive/$(SOURCE).tsv | work
	sed -e 's/悪沢岳（東岳）/東岳（悪沢岳）/g' $< > $@

archive/$(SOURCE).tsv:
	@echo "please run the crawler to generate '$@'."
	@exit 1

import: dist/$(SOURCE).tsv
	python3 ../import_pois.py --truncate $(TABLE_NAME) $<

unify:
	python3 ../unify_pois.py --truncate $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
