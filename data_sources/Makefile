# Makefile for data_sources

POIDIRS := gsi_vtexp gsi_dm25k yamap yamareco wikidata book

regist:
	@for dir in $(POIDIRS); do \
		$(MAKE) -C $$dir $@; \
	done

unify:
	@for dir in $(POIDIRS); do \
		$(MAKE) -C $$dir $@; \
	done

.PHONY: all regist unify clean
