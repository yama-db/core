# Makefile for book data source
.DELETE_ON_ERROR:

TABLE_NAME = stg_book_web_pois

all: ;

import:
	python3 ../import_csv.py --truncate information_sources raw/information_sources.csv
	python3 import_book.py --truncate $(TABLE_NAME) raw/[0-9]*.csv

unify:
	python3 unify_book_web.py --truncate $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
