all: dist/gsi_gcp.csv

TABLE_NAME := stg_gsi_gcp_pois

dist/gsi_gcp.csv:
	../xml2csv.py workspace/FG-GML-*.xml > $@

register: dist/gsi_gcp.csv
	../regist_pois.py --truncate $< $(TABLE_NAME)

clean:
	rm -f dist/*

.PHONY: all clean