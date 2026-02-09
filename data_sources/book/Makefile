# Makefile for book data source
.DELETE_ON_ERROR:

TABLE_NAMES = information_sources digital_service_details book_details

all: ;

import:
	@for table in $(TABLE_NAMES); do \
		python3 ../import_csv.py --truncate raw/$$table.csv $$table; \
	done
	@for csv_file in raw/[0-9]*.csv; do \
		source_id=$$(echo $$csv_file | sed -E 's|raw/([0-9]+)_.*\.csv|\1|'); \
		python3 import_book.py --truncate $$source_id $$csv_file; \
	done

unify:
	python3 ../unify_pois.py stg_book_pois

clean: ;

.PHONY: all import unify clean
