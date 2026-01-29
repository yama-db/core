all: dist/gsi_vtexp.csv

TABLE_NAME := stg_gsi_vtexp_pois

dist/gsi_vtexp.csv:
	python3 extract_features.py archive/tiles/ > $@

regist: dist/gsi_vtexp.csv
	python3 ../regist_pois.py --truncate $< $(TABLE_NAME)
	python3 ../regist_aliases.py $< $(TABLE_NAME)

unify:
	python3 ../unify_pois.py $(TABLE_NAME)

clean:
	rm -f dist/*

.PHONY: all clean regist unify
