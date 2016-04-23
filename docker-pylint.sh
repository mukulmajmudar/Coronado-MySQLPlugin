#!/bin/bash
set -x
docker build -t $USER/coronado-mysqlplugin .
docker run --rm --entrypoint=pylint $USER/coronado-mysqlplugin MySQLPlugin
