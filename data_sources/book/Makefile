# Makefile for book data source
.DELETE_ON_ERROR:

TABLE_NAME := stg_book_web_pois

all: ;

import:
	python3 ../import_csv.py --truncate information_sources raw/information_sources.csv
	@for csv_file in raw/[0-9]*.csv; do \
		source_id=$$(echo $$csv_file | sed -E 's|raw/([0-9]+)_.*\.csv|\1|'); \
		python3 import_book.py --truncate $(TABLE_NAME) $$source_id $$csv_file; \
	done

unify:
	python3 ../unify_pois.py --truncate $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
