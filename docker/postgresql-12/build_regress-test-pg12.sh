#!/bin/bash
CONTAINER_NAME=oai_fdw_pg12

docker stop $CONTAINER_NAME
docker restart $CONTAINER_NAME 
docker exec -u postgres $CONTAINER_NAME psql -d db -c "DROP EXTENSION IF EXISTS oai_fdw CASCADE" 
docker exec $CONTAINER_NAME make clean --silent -C /home/postgres/oai_fdw &&
docker exec $CONTAINER_NAME make --silent -j10 -C /home/postgres/oai_fdw &&
docker exec $CONTAINER_NAME make install -C /home/postgres/oai_fdw &&
docker exec $CONTAINER_NAME make PGUSER=postgres installcheck -C /home/postgres/oai_fdw
docker restart $CONTAINER_NAME 
docker exec -u postgres $CONTAINER_NAME psql -d db -c "CREATE EXTENSION oai_fdw;" 
docker exec -u postgres $CONTAINER_NAME psql -d db -c "SELECT OAI_Version();" -c "SELECT Version();"
