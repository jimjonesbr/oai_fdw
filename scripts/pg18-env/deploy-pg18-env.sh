POSTGRES18_IMAGE=pg18-oai_fdw
OAI_FDW_PATH=~/git/oai_fdw
PG_CONTAINER=oai_pg18
NETWORK_NAME=pgnet
IP_ADDRESS="172.19.42.58"

podman build -t $POSTGRES18_IMAGE .

podman stop $PG_CONTAINER
podman rm $PG_CONTAINER
podman network create --driver=bridge --subnet=172.19.42.0/24 $NETWORK_NAME
podman run -d \
  --name   $PG_CONTAINER \
  --network $NETWORK_NAME \
  --env POSTGRES_HOST_AUTH_METHOD=trust \
  --ip $IP_ADDRESS \
  --volume  $OAI_FDW_PATH:/oai_fdw:Z \
  $POSTGRES18_IMAGE -c logging_collector=on \
                    -c max_locks_per_transaction=512 &&

podman exec -itw /oai_fdw/ $PG_CONTAINER make clean &&
podman exec -itw /oai_fdw/ $PG_CONTAINER make PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config &&
podman exec -itw /oai_fdw/ $PG_CONTAINER make install