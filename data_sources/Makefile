# Makefile for data_sources

POIDIRS := gsi_vtexp gsi_dm25k yamap yamareco wikidata legacy book

all:
	@for dir in $(POIDIRS); do \
		$(MAKE) -C $$dir $@; \
	done

import:
	@for dir in $(POIDIRS); do \
		$(MAKE) -C $$dir $@; \
	done

unify:
	@for dir in $(POIDIRS); do \
		$(MAKE) -C $$dir $@; \
	done \
	./import_poi_names.sh

clean:
	@for dir in $(POIDIRS); do \
		$(MAKE) -C $$dir $@; \
	done

.PHONY: all import unify clean
