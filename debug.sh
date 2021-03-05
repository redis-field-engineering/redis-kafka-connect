#!/usr/bin/env bash

: ${SUSPEND:='n'}

set -e

mvn clean package
export KAFKA_DEBUG='y'
connect-standalone config/connect-avro-docker.properties config/source.properties