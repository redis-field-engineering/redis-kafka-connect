ARG CP_VERSION=7.2.0
ARG BASE_PREFIX=confluentinc 
ARG CONNECT_IMAGE=cp-server-connect

FROM $BASE_PREFIX/$CONNECT_IMAGE:$CP_VERSION

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:0.5.3

COPY target/components/packages/redis-redis-enterprise-kafka-6.zip /tmp/redis-redis-enterprise-kafka.zip

RUN confluent-hub install --no-prompt /tmp/redis-redis-enterprise-kafka.zip