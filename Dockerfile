FROM confluentinc/cp-kafka-connect-base:0.0.0

COPY target/components/packages/redislabs-redis-enterprise-kafka-1.0.0-SNAPSHOT.zip /tmp/my-connector-0.0.0.zip

RUN confluent-hub install --no-prompt /tmp/my-connector-0.0.0.zip