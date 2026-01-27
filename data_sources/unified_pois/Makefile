all: dist/unified_pois.csv

dist/unified_pois.csv:
	python3 gen_unified_pois.py > $@

regist: dist/unified_pois.csv
	mysql --defaults-file=../../.my.cnf < init_poi_categories.sql
	python3 regist_unified_pois.py --truncate $< unified_pois

clean:
	rm -f dist/unified_pois.csv

.PHONY: all regist clean
