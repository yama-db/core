REVISION := 20250101

TABLE_REGIONS := administrative_regions
TABLE_BOUNDARIES := administrative_boundaries

all: raw/N03-$(REVISION)_GML.zip raw/AdminiBoundary_CD.xlsx raw/query.csv
	@echo "All required raw files are present."
	@echo "You can run 'make regist' to generate and register the data."

raw/N03-$(REVISION)_GML.zip:
	@echo "Please download N03-$(REVISION)_GML.zip from https://nlftp.mlit.go.jp/ksj/ and place it in raw/ directory."
	@exit 1

raw/AdminiBoundary_CD.xlsx:
	@echo "Please download https://nlftp.mlit.go.jp/ksj/gml/codelist/AdminiBoundary_CD.xlsx and place it in raw/ directory."
	@exit 1

raw/query.csv:
	@echo "Please run 'python3 query_jis_code.py' to generate raw/query.csv."
	@exit 1

dist/regions.csv: raw/AdminiBoundary_CD.xlsx raw/query.csv
	python3 gen_regions_csv.py $^ > $@

work/N03-$(REVISION).geojson: raw/N03-$(REVISION)_GML.zip
	unzip $< $(@F) -d $(@D)

regist: dist/regions.csv work/N03-$(REVISION).geojson
	python3 ../regist_csv.py --truncate dist/regions.csv $(TABLE_REGIONS)
	python3 regist_boundaries.py --truncate work/N03-$(REVISION).geojson $(TABLE_BOUNDARIES)

unify: ;

clean:
	rm -f dist/* work/*

.PHONY: all regist unify clean
