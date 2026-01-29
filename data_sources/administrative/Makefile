REVISION := 20250101

TABLE_REGIONS := administrative_regions
TABLE_BOUNDARIES := administrative_boundaries

all: dist/regions.csv

raw/N03-$(REVISION)_GML.zip:
	@echo "Please download N03-$(REVISION)_GML.zip from https://nlftp.mlit.go.jp/ksj/ and place it in raw/ directory."
	@exit 1

raw/AdminiBoundary_CD.xlsx:
	@echo "Please download https://nlftp.mlit.go.jp/ksj/gml/codelist/AdminiBoundary_CD.xlsx and place it in raw/ directory."
	@exit 1

raw/query.csv:
	python3 query_jis_code.py > $@

dist/regions.csv: raw/AdminiBoundary_CD.xlsx raw/query.csv
	python3 gen_regions_csv.py $^ > $@

work/N03-$(REVISION).geojson: raw/N03-$(REVISION)_GML.zip
	unzip $< $(@F) -d $(@D)

regist: dist/regions.csv work/N03-$(REVISION).geojson
	python3 ../regist_csv.py --truncate $< $(TABLE_REGIONS)
	python3 regist_boundaries.py --truncate work/N03-$(REVISION).geojson $(TABLE_BOUNDARIES)

clean:
	rm -f dist/*

.PHONY: all regist clean
