#!/bin/bash

CONTAINER_NAME=oai_fdw_pg14
BASE_IMAGE=postgres:14
BASEPATH=$(pwd)
DOCKERFILE=$BASEPATH/dockerfiles
DATUM=$(date +'%Y-%m-%d')
IMAGE_NAME=img_oai_fdw_pg14
NETWORK_BASE_IP=172.18.0
CONTAINER_IP=$NETWORK_BASE_IP.114
DOCKER_NETWORK_NAME=jimnet
DOCKER_NETWORK_SUBNET=172.18.0.0/16

rm -rf data/

docker pull $BASE_IMAGE &&
docker build --no-cache -t $IMAGE_NAME $DOCKERFILE &&

#Container running?
if [ $(docker ps | grep $CONTAINER_NAME | wc -l) -eq "1" ]; then
  docker stop $CONTAINER_NAME
  docker rm $CONTAINER_NAME
else
  if [ $(docker ps -a | grep $CONTAINER_NAME | wc -l) -eq "1" ]; then
    docker rm $CONTAINER_NAME
  fi
fi &&

docker network create --subnet=$DOCKER_NETWORK_SUBNET --driver bridge $DOCKER_NETWORK_NAME

cd ..

docker create --name $CONTAINER_NAME \
--restart=unless-stopped \
--net=$DOCKER_NETWORK_NAME --ip=$CONTAINER_IP \
-v $BASEPATH:/home/postgres:Z \
-v $(pwd):/home/postgres/oai_fdw:Z \
-e PGDATA=/home/postgres/data \
-e POSTGRES_HOST_AUTH_METHOD=trust \
-e POSTGRES_DB=db \
 $IMAGE_NAME &&

docker start $CONTAINER_NAME &&

echo -n "Waiting for container "$CONTAINER_NAME" to start...";
while true
do
  status=$(nc -z -v $CONTAINER_IP 5432 2>&1);
  if [[ $status =~ "succeeded!" ]]; then
    echo " PostgreSQL up and running!";
    break;
  else
    echo -n ".";
    sleep 0.5;
  fi
done
