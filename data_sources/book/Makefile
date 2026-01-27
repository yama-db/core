all: done/digital_service_details done/book_details done/information_sources

done/digital_service_details: csv/digital_service_details.csv done/information_sources
	python3 ../regist_csv.py --truncate $< ${@F}
	touch $@

done/book_details: csv/book_details.csv done/information_sources
	python3 ../regist_csv.py --truncate $< ${@F}
	touch $@

done/information_sources: csv/information_sources.csv
	python3 ../regist_csv.py --truncate $< ${@F}
	touch $@

regist: done/digital_service_details done/book_details done/information_sources	
	python3 regist_book.py --truncate 9 csv/09_kyoto339.csv
	python3 regist_book.py --truncate 10 csv/10_shizuoka100.csv
	python3 regist_book.py --truncate 11 csv/11_ishikawa.csv
	python3 regist_book.py --truncate 12 csv/12_oita.csv
	python3 regist_book.py --truncate 13 csv/13_oita300.csv
	python3 regist_book.py --truncate 14 csv/14_oita250.csv
	python3 regist_book.py --truncate 18 csv/18_boso.csv
	python3 regist_book.py --truncate 20 csv/20_suzuka300.csv

unify:
	python3 ../unify_pois.py stg_book_pois

clean:
	rm -f done/*

.PHONY: all register unify clean