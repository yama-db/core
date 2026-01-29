# Makefile for processing geographic data

SOURCE := yamap
TABLE_NAME := stg_$(SOURCE)_pois

all: dist/$(SOURCE).csv

dist/$(SOURCE).csv: ../../crawler/dist/$(SOURCE).tsv
	python3 ../tsv2csv.py $(SOURCE) < $< > $@

regist: dist/$(SOURCE).csv
	python3 ../regist_pois.py --truncate $< $(TABLE_NAME)
	python3 ../regist_aliases.py $< $(TABLE_NAME)

unify:
	python3 ../unify_pois.py $(TABLE_NAME)

clean:
	rm -f dist/*

.PHONY: all regist unify clean
