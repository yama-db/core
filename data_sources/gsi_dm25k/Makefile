# Makefile for processing geographic data

all: private/gsi_dm25k.csv

TABLE_NAME := stg_gsi_dm25k_pois
SED_CMD1 := $(shell printf 's/,\uE662牛,/,\uE6EF牛,/')
SED_CMD2 := $(shell printf 's/,大工\uE67A浜,/,大工\uE62A浜,/') 

private/gsi_dm25k.csv: archive/tyuuki.csv
	iconv -f CP932 -t UTF-8 $< | \
	sed -e "${SED_CMD1}" -e "${SED_CMD2}" | \
	python3 convert_tyuuki.py > $@

regist: private/gsi_dm25k.csv
	python3 ../regist_pois.py --truncate $< $(TABLE_NAME)
	python3 ../regist_aliases.py $< $(TABLE_NAME)

unify:
	python3 ../unify_pois.py $(TABLE_NAME)

clean:
	rm -f dist/*

.PHONY: all regist unify clean
