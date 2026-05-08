#!/bin/bash

CONTAINER_NAME=oai_pg18
NETWORK_NAME=pgnet
GRAPHDB_REPOSITORY="test"
FUSEKI_DATASET="dt"

# Build and install oai_fdw
echo -e "\n== Building and Installing oai_fdw on PostgreSQL 18 ==\n"

docker exec -itw /oai_fdw/ $CONTAINER_NAME make uninstall 2>/dev/null || true
docker exec -itw /oai_fdw/ $CONTAINER_NAME make clean
docker exec -itw /oai_fdw/ $CONTAINER_NAME make
docker exec -itw /oai_fdw/ $CONTAINER_NAME make install
docker restart $CONTAINER_NAME
docker exec -itw /oai_fdw/ -u postgres $CONTAINER_NAME psql -d postgres \
  -c "DROP EXTENSION IF EXISTS oai_fdw CASCADE; CREATE EXTENSION oai_fdw"

# SKIP_STRESS_TESTS=1   - skip long running stress tests
# SKIP_UPDATE_TESTS=1   - skip tests that update data (INSERT/DELETE/UPDATE)
# SKIP_EXTERNAL_TESTS=1 - skip tests that need external network access

docker exec -itw /oai_fdw/ $CONTAINER_NAME make PGUSER=postgres installcheck 
echo -e "\n== Tests completed ==\n"
