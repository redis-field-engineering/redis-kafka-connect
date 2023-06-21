ARG CP_VERSION=7.2.0
ARG BASE_PREFIX=confluentinc
ARG CONNECT_IMAGE=cp-server-connect

FROM openjdk:18-jdk-slim AS build
WORKDIR /root/redis-kafka-connect
COPY . /root/redis-kafka-connect
RUN ./gradlew createConfluentArchive

FROM $BASE_PREFIX/$CONNECT_IMAGE:$CP_VERSION

COPY --from=build /root/redis-kafka-connect/core/redis-kafka-connect/build/confluent/redis-redis-kafka-connect-*.zip /tmp/redis-redis-kafka-connect.zip

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:0.5.3

RUN confluent-hub install --no-prompt /tmp/redis-redis-kafka-connect.zip