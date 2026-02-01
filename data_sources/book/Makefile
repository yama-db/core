# Makefile for book data source

TABLE_NAMES = information_sources digital_service_details book_details

all: raw/information_sources.csv raw/digital_service_details.csv raw/book_details.csv
	@echo "All required raw files are present."
	@echo "Also, you need to prepare csv files named as <source_id>_*.csv for each book data source in the raw/ directory."
	@echo "You can run 'make regist' to generate and register the data."

raw/information_sources.csv:
	@echo "Please prepare $@."
	@exit 1

raw/digital_service_details.csv:
	@echo "Please prepare $@."
	@exit 1

raw/book_details.csv:
	@echo "Please prepare $@."
	@exit 1

regist:
	@for table in $(TABLE_NAMES); do \
		python3 ../regist_csv.py --truncate raw/$$table.csv $$table; \
	done
	@for csv_file in raw/[0-9]*.csv; do \
		source_id=$$(echo $$csv_file | sed -E 's|raw/([0-9]+)_.*\.csv|\1|'); \
		python3 regist_book.py --truncate $$source_id $$csv_file; \
	done

unify:
	python3 ../unify_pois.py stg_book_pois

clean: ;

.PHONY: all regist unify clean
