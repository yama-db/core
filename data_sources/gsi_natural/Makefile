all: dist/gsi_natural_features.csv

TABLE_NAME := stg_gsi_natural_pois

dist/gsi_natural_features.csv:
	../extract_features.py private_data/tiles > $@

register: dist/gsi_natural_features.csv
	../regist_pois.py --truncate $< $(TABLE_NAME)

clean:
	rm -f dist/gsi_natural_features.csv

.PHONY: all clean