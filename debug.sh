#!/usr/bin/env bash

: ${SUSPEND:='n'}

set -e

./mvnw clean package
export KAFKA_DEBUG='y'
connect-standalone config/connect-avro-docker.properties config/sink-string.properties
