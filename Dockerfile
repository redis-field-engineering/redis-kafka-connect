ARG CP_VERSION=7.7.0
ARG BASE_PREFIX=confluentinc
ARG CONNECT_IMAGE=cp-kafka-connect

FROM gradle:8.10.0-jdk17 AS build
WORKDIR /app
COPY --chown=gradle:gradle . /app
RUN gradle createConfluentArchive --no-daemon

FROM $BASE_PREFIX/$CONNECT_IMAGE:$CP_VERSION

COPY --from=build /app/core/redis-kafka-connect/build/confluent/redis-redis-kafka-connect-*.zip /tmp/redis-redis-kafka-connect.zip

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:0.5.3

RUN confluent-hub install --no-prompt /tmp/redis-redis-kafka-connect.zip