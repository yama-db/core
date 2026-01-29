all:
	for table in information_sources digital_service_details book_details; do \
		python3 ../regist_csv.py --truncate csv/$$table.csv $$table; \
	done

regist:
	for csv_file in csv/[0-9]*.csv; do \
		source_id=$$(echo $$csv_file | sed -E 's|csv/([0-9]+)_.*\.csv|\1|'); \
		python3 regist_book.py --truncate $$source_id $$csv_file; \
	done

unify:
	python3 ../unify_pois.py stg_book_pois

clean:

.PHONY: all regist unify clean
