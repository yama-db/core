# Makefile for processing geographic data

SOURCE := yamareco
TABLE_NAME := stg_$(SOURCE)_pois

all: dist/$(SOURCE).csv
	@echo "All required raw files are present."
	@echo "You can run 'make regist' to generate and register the data."

dist/$(SOURCE).csv: ../../crawler/dist/$(SOURCE).tsv ../tsv2csv.py
	python3 ../tsv2csv.py $(SOURCE) < $< > $@

regist: dist/$(SOURCE).csv
	python3 ../regist_pois.py --truncate $< $(TABLE_NAME)

unify:
	python3 ../unify_pois.py $(TABLE_NAME)

clean:
	rm -f dist/*

.PHONY: all clean unify
