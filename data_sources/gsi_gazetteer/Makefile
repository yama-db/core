# Makefile for processing geographic data

all: dist/tyuuki.csv

TABLE_NAME := stg_gsi_gazetteer_pois
SED_CMD1 := $(shell printf 's/,\uE662牛,/,\uE6EF牛,/')
SED_CMD2 := $(shell printf 's/,大工\uE67A浜,/,大工\uE62A浜,/') 

dist/tyuuki.csv: private_data/tyuuki.csv
	iconv -f CP932 -t UTF-8 $< | \
	sed -e "${SED_CMD1}" -e "${SED_CMD2}" | \
	../convert_tyuuki.py > $@

register: dist/tyuuki.csv
	../regist_pois.py --truncate $< $(TABLE_NAME)
	../regist_aliases.py $< $(TABLE_NAME)

clean:
	rm -f dist/*

.PHONY: all clean
