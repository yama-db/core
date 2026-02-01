# Makefile for Wikidata POIs

TABLE_NAME := stg_wikidata_pois

all: archive/wikidata.csv archive/wikipedia.csv
	@echo "All required raw files are present."
	@echo "You can run 'make regist' to generate and register the data."

archive/wikidata.csv:
	python3 query_wikidata.py > $@

archive/wikipedia.csv: archive/wikidata.csv
	python3 query_wikipedia.py $< > $@

dist/wikidata.csv: convert_wikidata.py
	python3 convert_wikidata.py archive/wikidata.csv archive/wikipedia.csv | \
	sed -e '/,西沢渓谷,/d' -e '/,大鰐テレビ中継局,/d' > $@

regist: dist/wikidata.csv
	python3 ../regist_pois.py --truncate $< $(TABLE_NAME)

unify:
	python3 ../unify_pois.py $(TABLE_NAME)

clean:
	rm -f dist/*

.PHONY: all regist unify clean
