# Makefile for processing geographic data

TABLE_NAME := stg_gsi_dm25k_pois
SED_CMD1 := $(shell printf 's/,\uE662牛,/,\uE6EF牛,/')
SED_CMD2 := $(shell printf 's/,大工\uE67A浜,/,大工\uE62A浜,/') 

all: private/gsi_dm25k.csv

private/gsi_dm25k.csv: archive/tyuuki.csv convert_tyuuki.py
	iconv -f CP932 -t UTF-8 $< | \
	sed -e "${SED_CMD1}" -e "${SED_CMD2}" | \
	python3 convert_tyuuki.py > $@

archive/tyuuki.csv:
	@echo "'$(@F)' is a content part of GSI DM25K data, not publicly distributable."
	@exit 1

import: private/gsi_dm25k.csv
	python3 ../import_pois.py --truncate $< $(TABLE_NAME)
	python3 import_aliases.py $< $(TABLE_NAME)

unify:
	python3 ../unify_pois.py $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
