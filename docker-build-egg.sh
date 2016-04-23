#!/bin/bash
set -x
docker build -t $USER/coronado-mysqlplugin .
mkdir -p dist
docker run --rm \
    -e USERID=$EUID \
    -v `pwd`/dist:/root/MySQLPlugin/dist \
    $USER/coronado-mysqlplugin
