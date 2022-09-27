ARG CP_VERSION=7.2.0
ARG BASE_PREFIX=confluentinc 
ARG CONNECT_IMAGE=cp-server-connect

FROM docker.io/library/maven:3.8.6-openjdk-18 AS builder
WORKDIR /root/redis-kafka-connect
COPY . /root/redis-kafka-connect
ENV MAVEN_FAST_INSTALL="-DskipTests -Dair.check.skip-all=true -Dmaven.javadoc.skip=true -B -q -T C1"
RUN mvn package $MAVEN_FAST_INSTALL

FROM $BASE_PREFIX/$CONNECT_IMAGE:$CP_VERSION

COPY --from=builder --chown=trino:trino /root/redis-kafka-connect/target/components/packages/redis-redis-enterprise-kafka-6.*.zip /tmp/redis-redis-enterprise-kafka-6.zip

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:0.5.3

RUN confluent-hub install --no-prompt /tmp/redis-redis-enterprise-kafka-6.zip