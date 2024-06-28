#!/bin/bash

PROFILE=${PROFILE:-release-min}
CONTAINER=${CONTAINER:-extract-piproxy-binary}

docker build --build-arg PROFILE=$PROFILE -t piproxy .
docker create --name $CONTAINER piproxy
mkdir -p ./dist/$PROFILE/
docker cp $CONTAINER:/piproxy ./dist/$PROFILE/
docker rm $CONTAINER
