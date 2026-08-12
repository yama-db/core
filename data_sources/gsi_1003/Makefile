# Makefile for processing geographic data
.DELETE_ON_ERROR:

SOURCE := gsi_1003
TABLE_NAME := stg_$(SOURCE)_pois
VERSION := 20260331

all: dist/$(SOURCE).tsv

dist work:
	mkdir -p $@

dist/$(SOURCE).tsv: archive/1003zan$(VERSION).geojson geojson2tsv.py | dist
	python3 geojson2tsv.py $< > $@

geojson2tsv.py: ../../shared/extract_aliases.py

archive/1003zan$(VERSION).geojson:
	wget -O $(basename $@) https://www.gsi.go.jp/KOKUJYOHO/MOUNTAIN/1003zan$(VERSION).zip
	unzip $(basename $@) -d archive

import: dist/$(SOURCE).tsv
	python3 ../import_pois.py --truncate $(TABLE_NAME) $<

unify:
	python3 ../unify_pois.py --truncate $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
