/*
 * Copyright © 2021 Redis Labs
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redislabs.kafkaconnect.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.jcustenborder.kafka.connect.utils.data.SinkOffsetState;
import com.github.jcustenborder.kafka.connect.utils.data.TopicPartitionCounter;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.redislabs.kafkaconnect.RedisEnterpriseSinkConnector;
import com.redislabs.kafkaconnect.common.RedisEnterpriseConnectorConfigException;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.batch.item.redis.support.operation.ConstantPredicate;
import org.springframework.batch.item.redis.support.operation.Hset;
import org.springframework.batch.item.redis.support.operation.Lpush;
import org.springframework.batch.item.redis.support.operation.NullValuePredicate;
import org.springframework.batch.item.redis.support.operation.Rpush;
import org.springframework.batch.item.redis.support.operation.Sadd;
import org.springframework.batch.item.redis.support.operation.Set;
import org.springframework.batch.item.redis.support.operation.Xadd;
import org.springframework.batch.item.redis.support.operation.Zadd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class RedisEnterpriseSinkTask extends SinkTask {

    private RedisClient client;
    private RedisEnterpriseSinkConfig config;
    private OperationItemWriter<SinkRecord> writer;
    private StatefulRedisConnection<String, String> connection;

    @Override
    public String version() {
        return new RedisEnterpriseSinkConnector().version();
    }

    @Override
    public void start(final Map<String, String> props) {
        config = new RedisEnterpriseSinkConfig(props);
        client = RedisClient.create(config.getRedisUri());
        connection = client.connect();
        writer = OperationItemWriter.operation(operation()).client(client).transactional(Boolean.TRUE.equals(config.isMultiexec())).build();
        writer.open(new ExecutionContext());
        final java.util.Set<TopicPartition> assignment = this.context.assignment();
        if (!assignment.isEmpty()) {
            List<SinkOffsetState> offsetStates = new ArrayList<>();
            String[] partitionKeys = assignment.stream().map(this::offsetKey).toArray(String[]::new);
            List<KeyValue<String, String>> values = connection.sync().mget(partitionKeys);
            for (KeyValue<String, String> value : values) {
                if (value.hasValue()) {
                    try {
                        offsetStates.add(ObjectMapperFactory.INSTANCE.readValue(value.getValue(), SinkOffsetState.class));
                    } catch (IOException e) {
                        throw new DataException(e);
                    }
                }
            }
            Map<TopicPartition, Long> partitionOffsets = new HashMap<>(assignment.size());
            for (SinkOffsetState state : offsetStates) {
                partitionOffsets.put(state.topicPartition(), state.offset());
                log.info("Requesting offset {} for {}", state.offset(), state.topicPartition());
            }
            for (TopicPartition topicPartition : assignment) {
                if (!partitionOffsets.containsKey(topicPartition)) {
                    partitionOffsets.put(topicPartition, 0L);
                    log.info("Requesting offset {} for {}", 0L, topicPartition);
                }
            }
            this.context.offset(partitionOffsets);
        }
    }

    private String offsetKey(TopicPartition topicPartition) {
        return String.format("__kafka.offset.%s.%s", topicPartition.topic(), topicPartition.partition());
    }

    private OperationItemWriter.RedisOperation<SinkRecord> operation() {
        switch (config.getType()) {
            case STREAM:
                return new Xadd<>(this::collectionKey, this::map);
            case HASH:
                return new Hset<>(this::key, this::map, this::isDelete);
            case STRING:
                return new Set<>(this::key, this::string, this::isDelete);
            case LIST:
                if (config.getPushDirection() == RedisEnterpriseSinkConfig.PushDirection.LEFT) {
                    return new Lpush<>(this::collectionKey, this::key, new ConstantPredicate<>(false), new NullValuePredicate<>(this::string));
                }
                return new Rpush<>(this::collectionKey, this::key, new ConstantPredicate<>(false), new NullValuePredicate<>(this::string));
            case SET:
                return new Sadd<>(this::collectionKey, this::key, new ConstantPredicate<>(false), new NullValuePredicate<>(this::string));
            case ZSET:
                return new Zadd<>(this::collectionKey, this::key, new ConstantPredicate<>(false), new NullValuePredicate<>(this::score), this::score);
            default:
                throw new RedisEnterpriseConnectorConfigException("Data structure not supported: " + config.getType());
        }
    }

    private String string(SinkRecord record) {
        return string("value", record.value());
    }

    private Double score(SinkRecord record) {
        Object value = record.value();
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return (Double) value;
        }
        throw new DataException("The value for the record must be float64 (Java double). Consider using a single message transformation to transform the data before it is written to Redis.");
    }

    private boolean isDelete(SinkRecord record) {
        return record.value() == null;
    }

    private String key(SinkRecord record) {
        return string("key", record.key());
    }

    private String string(String source, Object input) {
        if (input == null) {
            return null;
        }
        if (input instanceof String) {
            return (String) input;
        }
        throw new DataException(String.format("The %s for the record must be String or Bytes. Consider using the StringConverter if the data is stored in Kafka in the format needed in Redis. Another option is to use a single message transformation to transform the data before it is written to Redis.", source));
    }

    private String collectionKey(SinkRecord record) {
        return config.getKeyFormat().replace(RedisEnterpriseSinkConfig.TOKEN_TOPIC, record.topic());
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> map(SinkRecord record) {
        Object value = record.value();
        if (value == null) {
            return null;
        }
        if (value instanceof Struct) {
            Map<String, String> body = new LinkedHashMap<>();
            Struct struct = (Struct) value;
            for (Field field : struct.schema().fields()) {
                Object fieldValue = struct.get(field);
                body.put(field.name(), fieldValue == null ? null : fieldValue.toString());
            }
            return body;
        }
        if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            Map<String, String> body = new LinkedHashMap<>();
            map.forEach((k, v) -> body.put(String.valueOf(k), String.valueOf(v)));
            return body;
        }
        throw new ConnectException("Unsupported source value type: " + record.valueSchema().type().name());
    }

    @Override
    public void stop() {
        if (writer != null) {
            writer.close();
        }
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
            client.getResources().shutdown();
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        log.debug("Processing {} records", records.size());
        try {
            writer.write(new ArrayList<>(records));
            log.info("Wrote {} records", records.size());
        } catch (Exception e) {
            log.warn("Could not write {} records", records.size(), e);
        }
        TopicPartitionCounter counter = new TopicPartitionCounter();
        for (SinkRecord record : records) {
            counter.increment(record.topic(), record.kafkaPartition(), record.kafkaOffset());
        }
        List<SinkOffsetState> offsetData = counter.offsetStates();
        if (!offsetData.isEmpty()) {
            Map<String, String> offsets = new LinkedHashMap<>(offsetData.size());
            for (SinkOffsetState e : offsetData) {
                String key = String.format("__kafka.offset.%s.%s", e.topic(), e.partition());
                String value;
                try {
                    value = ObjectMapperFactory.INSTANCE.writeValueAsString(e);
                } catch (JsonProcessingException e1) {
                    throw new DataException(e1);
                }
                offsets.put(key, value);
                log.trace("put() - Setting offset: {}", e);
            }
            connection.sync().mset(offsets);
        }
    }


}
