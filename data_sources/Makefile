# Makefile for data_sources

POIDIRS := gsi_1003 gsi_vtexp gsi_dm25k yamap yamareco wikidata legacy book_web

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
	done

clean:
	@for dir in $(POIDIRS); do \
		$(MAKE) -C $$dir $@; \
	done

.PHONY: all import unify clean
