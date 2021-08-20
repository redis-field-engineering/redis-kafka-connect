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
package com.redislabs.kafka.connect.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.jcustenborder.kafka.connect.utils.data.SinkOffsetState;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.redislabs.kafka.connect.RedisEnterpriseSinkConnector;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.core.convert.converter.Converter;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class RedisEnterpriseSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(RedisEnterpriseSinkTask.class);
    private static final String OFFSET_KEY_FORMAT = "com.redislabs.kafka.connect.sink.offset.%s.%s";

    private RedisClient client;
    private RedisEnterpriseSinkConfig config;
    private Charset charset;
    private OperationItemWriter<byte[], byte[], SinkRecord> writer;
    private StatefulRedisConnection<String, String> connection;

    @Override
    public String version() {
        return new RedisEnterpriseSinkConnector().version();
    }

    @Override
    public void start(final Map<String, String> props) {
        config = new RedisEnterpriseSinkConfig(props);
        client = RedisClient.create(config.getRedisURI());
        connection = client.connect();
        charset = config.getCharset();
        writer = OperationItemWriter.operation(operation()).codec(new ByteArrayCodec()).client(client).transactional(Boolean.TRUE.equals(config.isMultiexec())).build();
        writer.open(new ExecutionContext());
        final java.util.Set<TopicPartition> assignment = this.context.assignment();
        if (!assignment.isEmpty()) {
            List<SinkOffsetState> offsetStates = new ArrayList<>();
            String[] partitionKeys = assignment.stream().map(a -> offsetKey(a.topic(), a.partition())).toArray(String[]::new);
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

    private String offsetKey(String topic, Integer partition) {
        return String.format(OFFSET_KEY_FORMAT, topic, partition);
    }

    private OperationItemWriter.RedisOperation<byte[], byte[], SinkRecord> operation() {
        switch (config.getType()) {
            case STREAM:
                return new Xadd<>((Converter<SinkRecord, byte[]>) this::collectionKey, this::map);
            case HASH:
                return new Hset<>(this::key, this::map, this::isDelete);
            case STRING:
                return new Set<>(this::key, this::value, this::isDelete);
            case LIST:
                if (config.getPushDirection() == RedisEnterpriseSinkConfig.PushDirection.LEFT) {
                    return new Lpush<>(this::collectionKey, this::key, new ConstantPredicate<>(false), new NullValuePredicate<>(this::value));
                }
                return new Rpush<>(this::collectionKey, this::key, new ConstantPredicate<>(false), new NullValuePredicate<>(this::value));
            case SET:
                return new Sadd<>(this::collectionKey, this::key, new ConstantPredicate<>(false), new NullValuePredicate<>(this::value));
            case ZSET:
                return new Zadd<>(this::collectionKey, this::key, new ConstantPredicate<>(false), new NullValuePredicate<>(this::score), this::score);
            default:
                throw new ConfigException(RedisEnterpriseSinkConfig.TYPE, config.getType());
        }
    }

    private byte[] value(SinkRecord record) {
        return bytes("value", record.value());
    }

    private Double score(SinkRecord record) {
        Object value = record.value();
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        throw new DataException("The value for the record must be a number. Consider using a single message transformation to transform the data before it is written to Redis.");
    }

    private boolean isDelete(SinkRecord record) {
        return record.value() == null;
    }

    private byte[] key(SinkRecord record) {
        return bytes("key", record.key());
    }

    private byte[] bytes(String source, Object input) {
        if (input == null) {
            return null;
        }
        if (input instanceof byte[]) {
            return (byte[]) input;
        }
        if (input instanceof String) {
            return ((String) input).getBytes(charset);
        }
        throw new DataException(String.format("The %s for the record must be a string or byte array. Consider using the StringConverter or ByteArrayConverter if the data is stored in Kafka in the format needed in Redis.", source));
    }

    private byte[] collectionKey(SinkRecord record) {
        return config.getKeyFormat().replace(RedisEnterpriseSinkConfig.TOKEN_TOPIC, record.topic()).getBytes(charset);
    }

    @SuppressWarnings("unchecked")
    private Map<byte[], byte[]> map(SinkRecord record) {
        Object value = record.value();
        if (value == null) {
            return null;
        }
        if (value instanceof Struct) {
            Map<byte[], byte[]> body = new LinkedHashMap<>();
            Struct struct = (Struct) value;
            for (Field field : struct.schema().fields()) {
                Object fieldValue = struct.get(field);
                body.put(field.name().getBytes(charset), fieldValue == null ? null : fieldValue.toString().getBytes(charset));
            }
            return body;
        }
        if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            Map<byte[], byte[]> body = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : map.entrySet()) {
                body.put(e.getKey().getBytes(charset), String.valueOf(e.getValue()).getBytes(charset));
            }
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
        Map<TopicPartition, Long> data = new ConcurrentHashMap<>(100);
        for (SinkRecord record : records) {
            Preconditions.checkState(!Strings.isNullOrEmpty(record.topic()), "topic cannot be null or empty.");
            Preconditions.checkNotNull(record.kafkaPartition(), "partition cannot be null.");
            Preconditions.checkState(record.kafkaOffset() >= 0, "offset must be greater than or equal 0.");
            TopicPartition partition = new TopicPartition(record.topic(), record.kafkaPartition());
            long current = data.getOrDefault(partition, Long.MIN_VALUE);
            if (record.kafkaOffset() > current) {
                data.put(partition, record.kafkaOffset());
            }
        }
        List<SinkOffsetState> offsetData = data.entrySet().stream().map(e -> SinkOffsetState.of(e.getKey(), e.getValue())).collect(Collectors.toList());
        if (!offsetData.isEmpty()) {
            Map<String, String> offsets = new LinkedHashMap<>(offsetData.size());
            for (SinkOffsetState e : offsetData) {
                String key = offsetKey(e.topic(), e.partition());
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
