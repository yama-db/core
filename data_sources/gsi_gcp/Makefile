TABLE_NAME := stg_gsi_gcp_pois

all: dist/gsi_gcp.csv

dist/gsi_gcp.csv: work/.extracted
	python3 xml2csv.py work/FG-GML-*.xml > $@

work/.extracted:
	./extract.sh
	touch $@

regist: dist/gsi_gcp.csv
	python3 ../regist_pois.py --truncate $< $(TABLE_NAME)

unify:
	python3 unify_gcp.py $(TABLE_NAME)

clean:
	rm -rf dist/* work/*

.PHONY: all regist unify clean
