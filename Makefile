MODULE_big = oai_fdw
OBJS = oai_fdw.o

EXTENSION = oai_fdw
DATA = oai_fdw--1.0.sql \
	   oai_fdw--1.0--1.1.sql \
	   oai_fdw--1.1--1.2.sql \
	   oai_fdw--1.2--1.3.sql \
	   oai_fdw--1.3--1.4.sql \
	   oai_fdw--1.4--1.5.sql \
	   oai_fdw--1.5--1.6.sql \
	   oai_fdw--1.6--1.7.sql \
	   oai_fdw--1.7--1.8.sql \
	   oai_fdw--1.8--1.9.sql \
	   oai_fdw--1.9--1.10.sql \
	   oai_fdw--1.10--1.11.sql \
	   oai_fdw--1.11--1.12.sql \
	   oai_fdw--1.12--1.13.sql \
	   oai_fdw--1.13--1.14.sql \
	   oai_fdw--1.14.sql

REGRESS = create-extension upgrade create_server import_foreign_schema create_foreign_table select_statements exceptions functions harvest

CURL_CONFIG = curl-config
PG_CONFIG = pg_config

CFLAGS += $(shell $(CURL_CONFIG) --cflags)
LIBS += $(shell $(CURL_CONFIG) --libs)
SHLIB_LINK := $(LIBS)

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)