MODULE_big = oai_fdw
OBJS = oai_fdw.o

EXTENSION = oai_fdw
DATA = oai_fdw--1.0.sql

CURL_CONFIG = curl-config
PG_CONFIG = pg_config

CFLAGS += $(shell $(CURL_CONFIG) --cflags)
LIBS += $(shell $(CURL_CONFIG) --libs)
SHLIB_LINK := $(LIBS)

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)