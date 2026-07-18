# Makefile for processing geographic data
.DELETE_ON_ERROR:

SOURCE := yamareco
TABLE_NAME := stg_$(SOURCE)_pois

all: dist/$(SOURCE).tsv

work:
	mkdir -p $@

dist/$(SOURCE).tsv: work/$(SOURCE).tsv ../yama2tsv.py ../../shared/extract_aliases.py
	python3 ../yama2tsv.py $< > $@

work/$(SOURCE).tsv: archive/$(SOURCE).tsv | work
	sed -e '/^74728\t/s,猟師ノ 尾根ノ頭,猟師ノ尾根ノ頭,'\
	 -e '/^6337\t/s,１むね,いちみね,'\
	 -e '/^6338\t/s,２むね,にみね,' $< > $@

archive/$(SOURCE).tsv:
	@echo "please run the crawler to generate '$@'."
	@exit 1

import: dist/$(SOURCE).tsv
	python3 ../import_pois.py --truncate $(TABLE_NAME) $<

unify:
	python3 ../unify_pois.py --truncate $(TABLE_NAME)

clean: ;

.PHONY: all import unify clean
