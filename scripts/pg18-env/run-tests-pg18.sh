#!/bin/bash

CONTAINER_NAME=oai_pg18
NETWORK_NAME=pgnet
TEST_ENV_PATH=~/git/oai_fdw/scripts

bash $TEST_ENV_PATH/squid/deploy-proxy-env.sh

# Build and install oai_fdw
echo -e "\n== Building and Installing oai_fdw on PostgreSQL 18 ==\n"

podman exec -itw /oai_fdw/ $CONTAINER_NAME make uninstall 2>/dev/null || true
podman exec -itw /oai_fdw/ $CONTAINER_NAME make clean
podman exec -itw /oai_fdw/ $CONTAINER_NAME make
podman exec -itw /oai_fdw/ $CONTAINER_NAME make install
podman restart $CONTAINER_NAME
podman exec -itw /oai_fdw/ -u postgres $CONTAINER_NAME psql -d postgres \
  -c "DROP EXTENSION IF EXISTS oai_fdw CASCADE; CREATE EXTENSION oai_fdw"

podman exec -itw /oai_fdw/ $CONTAINER_NAME make PGUSER=postgres installcheck 
echo -e "\n== Tests completed ==\n"
