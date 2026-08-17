# Makefile for processing geographic data
.DELETE_ON_ERROR:

TABLE_NAME := stg_gsi_dm25k_pois
SED_CMD1 := $(shell printf '/^523372,/s/,\uE662牛,/,\uE6EF牛,/')
SED_CMD2 := $(shell printf '/^513354,/s/,大工\uE67A浜,/,大工\uE62A浜,/')

all: private/gsi_dm25k.tsv private/gsi_dm25k_aliases.tsv

work private:
	mkdir -p $@

work/tyuuki.csv: archive/tyuuki.csv error_collections.sed | work
	iconv -f CP932 -t UTF-8 $< | \
	sed -e "${SED_CMD1}" -e "${SED_CMD2}" -f error_collections.sed > $@

private/gsi_dm25k.tsv: work/tyuuki.csv convert_tyuuki.py | private
	python3 convert_tyuuki.py $< > $@

private/gsi_dm25k_aliases.tsv: work/tyuuki.csv convert_tyuuki.py | private
	python3 convert_tyuuki.py --aliases $<> $@

archive/tyuuki.csv:
	@echo "'$(@F)' is a content part of GSI DM25K data, not publicly distributable."
	@exit 1

import: private/gsi_dm25k.tsv private/gsi_dm25k_aliases.tsv
	python3 ../import_pois.py --truncate $(TABLE_NAME) $(word 1, $^)
	python3 import_aliases.py $(TABLE_NAME) $(word 2, $^)

unify:
	python3 ../unify_pois.py --truncate $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
