# Makefile for Wikidata POIs
.DELETE_ON_ERROR:

TABLE_NAME := stg_wikidata_pois

all: archive/wikidata.csv archive/wikipedia.csv dist/wikidata.tsv

archive dist:
	mkdir -p $@

archive/wikidata.csv: query_wikidata.py | archive
	python3 query_wikidata.py > $@

archive/wikipedia.csv: archive/wikidata.csv query_wikipedia.py | archive
	python3 query_wikipedia.py $< > $@

dist/wikidata.tsv: archive/wikidata.csv archive/wikipedia.csv convert_wikidata.py | dist
	python3 convert_wikidata.py archive/wikidata.csv archive/wikipedia.csv | \
	sed -e '/,西沢渓谷,/d' -e '/,大鰐テレビ中継局,/d' > $@

import: dist/wikidata.tsv ../import_pois.py
	python3 ../import_pois.py --truncate $(TABLE_NAME) $<

unify: ../unify_pois.py
	python3 ../unify_pois.py --truncate $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
