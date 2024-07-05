#!/bin/bash

PROFILE=${PROFILE:-release-min}
CONTAINER=${CONTAINER:-extract-piproxy-binary}

docker build --build-arg PROFILE=$PROFILE -t piproxy .
docker create --name $CONTAINER piproxy
mkdir -p ./dist
docker cp $CONTAINER:/piproxy ./dist/piproxy-$PROFILE
docker rm $CONTAINER
