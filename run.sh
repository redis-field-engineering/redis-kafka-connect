#!/bin/bash

set -e
(
if lsof -Pi :6379 -sTCP:LISTEN -t >/dev/null ; then
    echo "Please terminate the local redis-server on 6379"
    exit 1
fi
)

echo "Building the Redis Enterprise Kafka Connector"
(
./mvnw clean package -DskipTests
mv target/components/packages/redis-redis-enterprise-kafka-6.*.zip target/components/packages/redis-enterprise-kafka.zip
)

echo "Starting docker ."
docker-compose up -d --build

function clean_up {
    echo -e "\n\nSHUTTING DOWN\n\n"
    curl --output /dev/null -X DELETE http://localhost:8083/connectors/datagen-pageviews || true
    curl --output /dev/null -X DELETE http://localhost:8083/connectors/redis-enterprise-sink || true
    curl --output /dev/null -X DELETE http://localhost:8083/connectors/redis-enterprise-source || true
    docker-compose down
    if [ -z "$1" ]
    then
      echo -e "Bye!\n"
    else
      echo -e "$1"
    fi
}

sleep 5
echo -ne "\n\nWaiting for the systems to be ready.."
function test_systems_available {
  COUNTER=0
  until $(curl --output /dev/null --silent --head --fail http://localhost:$1); do
      printf '.'
      sleep 2
      (( COUNTER+=1 ))
      if [[ $COUNTER -gt 30 ]]; then
        MSG="\nWARNING: Could not reach configured kafka system on http://localhost:$1 \nNote: This script requires curl.\n"

          if [[ "$OSTYPE" == "darwin"* ]]; then
            MSG+="\nIf using OSX please try reconfiguring Docker and increasing RAM and CPU. Then restart and try again.\n\n"
          fi

        echo -e "$MSG"
        clean_up "$MSG"
        exit 1
      fi
  done
}

test_systems_available 8082
test_systems_available 8083

trap clean_up EXIT

echo -e "\nKafka Topics:"
curl -X GET "http://localhost:8082/topics" -w "\n"

echo -e "\nKafka Connectors:"
curl -X GET "http://localhost:8083/connectors/" -w "\n"

echo -e "\nAdding datagen pageviews:"
curl -X POST -H "Content-Type: application/json" --data '
  { "name": "datagen-pageviews",
    "config": {
      "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
      "kafka.topic": "pageviews",
      "quickstart": "pageviews",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false",
      "producer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor",
      "max.interval": 200,
      "iterations": 10000000,
      "tasks.max": "1"
}}' http://localhost:8083/connectors -w "\n"

sleep 5

echo -e "\nAdding Redis Enteprise Kafka Sink Connector for the 'pageviews' topic into the 'pageviews' stream:"
curl -X POST -H "Content-Type: application/json" --data '
  {"name": "redis-enterprise-sink",
   "config": {
     "connector.class":"com.redis.kafka.connect.RedisEnterpriseSinkConnector",
     "tasks.max":"1",
     "topics":"pageviews",
     "redis.uri":"redis://redis:6379",
     "key.converter": "org.apache.kafka.connect.storage.StringConverter",
     "value.converter": "org.apache.kafka.connect.json.JsonConverter",
     "value.converter.schemas.enable": "false"
}}' http://localhost:8083/connectors -w "\n"

sleep 2
echo -e "\nAdding Redis Enterprise Kafka Sink Connector for the 'pageviews' topic into RedisJSON:"
curl -X POST -H "Content-Type: application/json" --data '
  {"name": "redis-enterprise-sink-json",
   "config": {
     "connector.class":"com.redis.kafka.connect.RedisEnterpriseSinkConnector",
     "tasks.max":"1",
     "topics":"pageviews",
     "redis.uri":"redis://redis:6379",
     "redis.type":"JSON",
     "key.converter": "org.apache.kafka.connect.json.JsonConverter",
     "value.converter": "org.apache.kafka.connect.storage.StringConverter",
     "value.converter.schemas.enable": "false",
     "transforms": "Cast",
     "transforms.Cast.type": "org.apache.kafka.connect.transforms.Cast$Key",
     "transforms.Cast.spec": "string"
}}' http://localhost:8083/connectors -w "\n"

sleep 2
echo -e "\nAdding Redis Enterprise Kafka Source Connector for the 'mystream' stream:"
curl -X POST -H "Content-Type: application/json" --data '
  {"name": "redis-enterprise-source",
   "config": {
     "tasks.max":"1",
     "connector.class":"com.redis.kafka.connect.RedisEnterpriseSourceConnector",
     "redis.uri":"redis://redis:6379",
     "redis.stream.name":"mystream",
     "topic": "mystream"
}}' http://localhost:8083/connectors -w "\n"

sleep 2
echo -e "\nKafka Connectors: \n"
curl -X GET "http://localhost:8083/connectors/" -w "\n"

echo "Number of messages in 'pageviews' stream:"
docker-compose exec redis /usr/local/bin/redis-cli xlen pageviews

sleep 2
echo -e "\nAdding messages to Redis stream 'mystream':"
docker-compose exec redis /usr/local/bin/redis-cli "xadd" "mystream" "*" "field1" "value11" "field2" "value21"
docker-compose exec redis /usr/local/bin/redis-cli "xadd" "mystream" "*" "field1" "value12" "field2" "value22"

echo -e '''


==============================================================================================================
Examine the topics in the Kafka UI: http://localhost:9021 or http://localhost:8000/
  - The `pageviews` topic should have the generated page views.
  - The `mystream` topic should contain the Redis stream messages.
The `pageviews` stream in Redis should contain the sunk page views: redis-cli xlen pageviews

Examine the Redis database:
  - In your shell run: docker-compose exec redis /usr/local/bin/redis-cli
  - List some RedisJSON keys: SCAN 0 TYPE ReJSON-RL
  - Show the JSON value of a given key: JSON.GET pageviews:971
==============================================================================================================

Use <ctrl>-c to quit'''

read -r -d '' _ </dev/tty
