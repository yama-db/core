TABLE_NAME := stg_wikidata_pois

all: dist/wikidata.csv

archive/wikidata.csv:
	python3 query_wikidata.py > $@

archive/wikipedia.csv: archive/wikidata.csv
	python3 query_wikipedia.py $< > $@

dist/wikidata.csv: archive/wikidata.csv archive/wikipedia.csv
	python3 convert_wikidata.py $^ | \
	sed -e '/,西沢渓谷,/d' -e '/,大鰐テレビ中継局,/d' > $@

regist: dist/wikidata.csv
	python3 ../regist_pois.py --truncate $< $(TABLE_NAME)
	python3 ../regist_aliases.py $< $(TABLE_NAME)

unify:
	python3 ../unify_pois.py $(TABLE_NAME)

clean:
	rm -f dist/*

.PHONY: all regist unify clean
