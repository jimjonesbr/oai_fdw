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
	   oai_fdw--1.14--1.15.sql \
	   oai_fdw--1.15.sql

REGRESS = create-extension \
		  upgrade \
		  create_server \
		  explain \
		  regressions

#
# The tests above need nothing but a PostgreSQL server, and are the ones that
# run by default - package builds, for instance, have no network access. The
# groups below need an OAI-PMH repository to talk to, so they are opt-in:
#
#   make installcheck INCLUDE_EXTERNAL_TESTS=1  live OAI-PMH repositories
#                                               (DNB and ULB Münster)
#   make installcheck INCLUDE_LOCAL_TESTS=1     the Squid proxies deployed by
#                                               scripts/squid, which forward to
#                                               the DNB repository
#   make installcheck INCLUDE_ALL_TESTS=1       all of the above
#
ifdef INCLUDE_ALL_TESTS
  INCLUDE_EXTERNAL_TESTS = 1
  INCLUDE_LOCAL_TESTS = 1
endif

ifdef INCLUDE_EXTERNAL_TESTS
  REGRESS += import_foreign_schema \
             create_foreign_table \
             select_statements \
             exceptions \
             functions \
             harvest
endif

ifdef INCLUDE_LOCAL_TESTS
  REGRESS += proxy
endif

CURL_CONFIG = curl-config
XML2_CONFIG = xml2-config
PG_CONFIG = pg_config

# SOURCE_DATE_EPOCH keeps the build date reproducible (reproducible-builds.org)
DATE_FMT = +%Y-%m-%d %H:%M:%S UTC
ifdef SOURCE_DATE_EPOCH
	BUILD_DATE = $(shell date -u -d "@$(SOURCE_DATE_EPOCH)" "$(DATE_FMT)" 2>/dev/null || date -u -r "$(SOURCE_DATE_EPOCH)" "$(DATE_FMT)")
else
	BUILD_DATE = $(shell date -u "$(DATE_FMT)")
endif

PG_CPPFLAGS += $(shell $(CURL_CONFIG) --cflags) \
			   $(shell $(XML2_CONFIG) --cflags) \
			   -DOAI_FDW_CC="\"$(CC)\"" \
			   -DOAI_FDW_BUILD_DATE="\"$(BUILD_DATE)\""
LIBS += $(shell $(CURL_CONFIG) --libs) \
		$(shell $(XML2_CONFIG) --libs)

SHLIB_LINK := $(LIBS)

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)