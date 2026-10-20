/*
 * Copyright © 2021 Redis
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
package com.redis.kafka.connect.sink;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.kafka.connect.common.ManifestVersionProvider;
import com.redis.kafka.connect.common.ModuleCommand;
import com.redis.kafka.connect.common.RedisConnection;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(RedisSinkTask.class);

    private static final String OFFSET_KEY_FORMAT = "com.redis.kafka.connect.sink.offset.%s.%s.%s";

    private static final String LEGACY_OFFSET_KEY_FORMAT = "com.redis.kafka.connect.sink.offset.%s.%s";

    private static final ObjectMapper objectMapper = objectMapper();

    private RedisSinkConfig config;

    private AbstractRedisClient client;
    private RedisConnection<String, String> connection;
    private Converter jsonConverter;
    private RedisSinkWriter writer;

    private static ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
        return mapper;
    }

    private static String offsetValue(SinkOffsetState state) {
        try {
            return objectMapper.writeValueAsString(state);
        } catch (JsonProcessingException e) {
            throw new DataException("Could not serialize sink offset state", e);
        }
    }

    @Override
    public String version() {
        return ManifestVersionProvider.getVersion();
    }

    @Override
    public void start(final Map<String, String> props) {
        config = new RedisSinkConfig(props);
        jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
        client = config.client();
        try {
            connection = RedisConnection.open(client, StringCodec.UTF8);
            writer = new RedisSinkWriter(client, config);
        } catch (Exception e) {
            stop();
            throw new ConnectException("Could not start Redis sink", e);
        }
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        super.open(partitions);
        if (partitions.isEmpty()) {
            return;
        }
        restoreOffsets(partitions);
    }

    private void restoreOffsets(Collection<TopicPartition> assignment) {
        Map<TopicPartition, Long> partitionOffsets = new HashMap<>(assignment.size());
        for (SinkOffsetState state : offsetStates(assignment)) {
            partitionOffsets.put(state.topicPartition(), state.offset());
            log.info("Requesting offset {} for {}", state.offset(), state.topicPartition());
        }
        if (partitionOffsets.isEmpty()) {
            return;
        }
        this.context.offset(partitionOffsets);
    }

    private Collection<SinkOffsetState> offsetStates(Collection<TopicPartition> assignment) {
        return assignment.stream().map(this::offsetState).filter(Optional::isPresent).map(Optional::get)
                .collect(Collectors.toList());
    }

    private Optional<SinkOffsetState> offsetState(TopicPartition partition) {
        String value = strings().get(offsetKey(partition));
        if (value == null) {
            value = strings().get(legacyOffsetKey(partition));
        }
        if (value == null) {
            return Optional.empty();
        }
        SinkOffsetState state = offsetState(value);
        if (!partition.equals(state.topicPartition())) {
            throw new DataException("Sink offset state does not match partition " + partition);
        }
        return Optional.of(state);
    }

    private String offsetKey(TopicPartition partition) {
        return offsetKey(partition.topic(), partition.partition());
    }

    private String offsetKey(SinkOffsetState state) {
        return offsetKey(state.topic(), state.partition());
    }

    private String offsetKey(String topic, Integer partition) {
        return String.format(OFFSET_KEY_FORMAT, config.getOffsetNamespace(), topic, partition);
    }

    private String legacyOffsetKey(TopicPartition partition) {
        return String.format(LEGACY_OFFSET_KEY_FORMAT, partition.topic(), partition.partition());
    }

    private SinkOffsetState offsetState(String value) {
        try {
            return objectMapper.readValue(value, SinkOffsetState.class);
        } catch (JsonProcessingException e) {
            throw new DataException("Could not parse sink offset state", e);
        }
    }

    @SuppressWarnings("unchecked")
    private RedisStringCommands<String, String> strings() {
        return connection.sync(RedisStringCommands.class);
    }

    private List<RedisWrite> prepare(Collection<SinkRecord> records) {
        List<RedisWrite> writes = new ArrayList<>();
        for (SinkRecord record : records) {
            RedisWrite write = command(record);
            if (write != null) {
                writes.add(write);
            }
        }
        return writes;
    }

    private RedisWrite command(SinkRecord record) {
        byte[] key = switch (config.getType()) {
            case STREAM, LIST, SET, ZSET, TIMESERIES -> collectionKey(record);
            default -> key(record);
        };
        if (record.value() == null) {
            return switch (config.getType()) {
                case HASH, JSON, STRING -> write(CommandType.DEL, key, List.of(), RedisWrite.Reply.INTEGER, -1);
                case LIST -> write(CommandType.LREM, key, List.of(number(0), member(record)), RedisWrite.Reply.INTEGER, -1);
                case SET -> write(CommandType.SREM, key, List.of(member(record)), RedisWrite.Reply.INTEGER, -1);
                case ZSET -> write(CommandType.ZREM, key, List.of(member(record)), RedisWrite.Reply.INTEGER, -1);
                case STREAM, TIMESERIES -> null;
            };
        }
        long ttl = ttl(record);
        return switch (config.getType()) {
            case STRING -> write(CommandType.SET, key, List.of(value(record)), RedisWrite.Reply.STATUS, ttl);
            case JSON -> write(ModuleCommand.JSON_SET, key, List.of(number("$"), jsonValue(record)), RedisWrite.Reply.STATUS, ttl);
            case HASH -> write(CommandType.HSET, key, fields(record), RedisWrite.Reply.INTEGER, ttl);
            case STREAM -> {
                List<byte[]> args = new ArrayList<>();
                args.add(number("*"));
                args.addAll(fields(record));
                yield write(CommandType.XADD, key, args, RedisWrite.Reply.VALUE, ttl);
            }
            case LIST -> write(CommandType.RPUSH, key, List.of(member(record)), RedisWrite.Reply.INTEGER, ttl);
            case SET -> write(CommandType.SADD, key, List.of(member(record)), RedisWrite.Reply.INTEGER, ttl);
            case ZSET -> write(CommandType.ZADD, key, List.of(number(doubleValue(record)), member(record)), RedisWrite.Reply.INTEGER, ttl);
            case TIMESERIES -> write(ModuleCommand.TS_ADD, key, List.of(number(longMember(record)), number(doubleValue(record))), RedisWrite.Reply.INTEGER, ttl);
        };
    }

    private RedisWrite write(ProtocolKeyword command, byte[] key, List<byte[]> args, RedisWrite.Reply reply, long ttl) {
        return new RedisWrite(command, key, List.copyOf(args), reply, ttl);
    }

    private byte[] number(Object value) {
        if (value == null) {
            throw new DataException("Redis command argument cannot be null");
        }
        return value.toString().getBytes(StandardCharsets.US_ASCII);
    }

    private List<byte[]> fields(SinkRecord record) {
        List<byte[]> args = new ArrayList<>();
        map(record).forEach((field, value) -> {
            if (value == null) {
                throw new DataException("Redis hash/stream fields cannot be null");
            }
            args.add(field);
            args.add(value);
        });
        if (args.isEmpty()) {
            throw new DataException("Redis hash/stream body cannot be empty");
        }
        return args;
    }

    private long ttl(SinkRecord record) {
        for (Header header : record.headers()) {
            if (RedisSinkConfigDef.KEY_TTL_CONFIG.equals(header.key())) {
                try {
                    if (header.value() != null) {
                        long ttl = Long.parseLong(header.value().toString());
                        if (ttl > 0) {
                            return ttl;
                        }
                    }
                } catch (NumberFormatException e) {
                    log.warn("Invalid TTL header for topic {} partition {} offset {}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
                }
            }
        }
        return config.getKeyTTL();
    }

    private Long longMember(SinkRecord record) {
        if (record.key() instanceof Number number) {
            return number.longValue();
        }
        throw new DataException("TimeSeries record key must be a numeric timestamp");
    }

    private Double doubleValue(SinkRecord record) {
        if (record.value() instanceof Number number && Double.isFinite(number.doubleValue())) {
            return number.doubleValue();
        }
        throw new DataException("Redis score/sample value must be a finite number");
    }

    private byte[] value(SinkRecord sinkRecord) {
        return bytes("value", sinkRecord.value());
    }

    private byte[] jsonValue(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        byte[] json;
        if (value instanceof byte[] binary) {
            json = binary;
        } else if (value instanceof String text) {
            json = text.getBytes(config.getCharset());
        } else {
            json = jsonConverter.fromConnectData(sinkRecord.topic(), sinkRecord.valueSchema(), value);
        }
        try {
            if (objectMapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(json).isMissingNode()) {
                throw new DataException("JSON record cannot be empty");
            }
        } catch (IOException e) {
            throw new DataException("Record does not contain a valid JSON document", e);
        }
        return json;
    }

    private byte[] key(SinkRecord sinkRecord) {
        if (sinkRecord.key() == null) {
            throw new DataException("Redis key cannot be null");
        }
        if (config.getKeyspace().isEmpty()) {
            return bytes("key", sinkRecord.key());
        }
        String keyspace = keyspace(sinkRecord);
        byte[] prefix = (keyspace + config.getSeparator()).getBytes(config.getCharset());
        byte[] suffix = sinkRecord.key() instanceof byte[] binary ? binary
                : String.valueOf(sinkRecord.key()).getBytes(config.getCharset());
        byte[] key = Arrays.copyOf(prefix, prefix.length + suffix.length);
        System.arraycopy(suffix, 0, key, prefix.length, suffix.length);
        return key;
    }

    private byte[] member(SinkRecord sinkRecord) {
        return bytes("key", sinkRecord.key());
    }

    private String keyspace(SinkRecord sinkRecord) {
        return config.getKeyspace().replace(RedisSinkConfigDef.TOKEN_TOPIC, sinkRecord.topic());
    }

    private byte[] bytes(String source, Object input) {
        if (input instanceof byte[]) {
            return (byte[]) input;
        }
        if (input instanceof String) {
            return ((String) input).getBytes(config.getCharset());
        }
        throw new DataException(String.format(
                "The %s for the record must be a string or byte array. Consider using the StringConverter or ByteArrayConverter if the data is stored in Kafka in the format needed in Redis.",
                source));
    }

    private byte[] collectionKey(SinkRecord sinkRecord) {
        return keyspace(sinkRecord).getBytes(config.getCharset());
    }

    @SuppressWarnings("unchecked")
    private Map<byte[], byte[]> map(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        if (value instanceof Struct) {
            Map<byte[], byte[]> body = new LinkedHashMap<>();
            Struct struct = (Struct) value;
            for (Field field : struct.schema().fields()) {
                Object fieldValue = struct.get(field);
                body.put(field.name().getBytes(config.getCharset()),
                        fieldValue == null ? null : fieldValue.toString().getBytes(config.getCharset()));
            }
            return body;
        }
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            Map<byte[], byte[]> body = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (!(e.getKey() instanceof String field)) {
                    throw new DataException("Redis hash/stream field names must be strings");
                }
                body.put(field.getBytes(config.getCharset()),
                        String.valueOf(e.getValue()).getBytes(config.getCharset()));
            }
            return body;
        }
        throw new DataException("Unsupported hash/stream value: " + (value == null ? "null" : value.getClass().getName()));
    }

    @Override
    public void stop() {
        try {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                    connection = null;
                }
            } finally {
                try {
                    if (client != null) {
                        client.shutdown();
                        client = null;
                    }
                } finally {
                    if (jsonConverter != null) {
                        try {
                            jsonConverter.close();
                        } catch (IOException e) {
                            log.warn("Could not close JSON converter", e);
                        } finally {
                            jsonConverter = null;
                        }
                    }
                }
            }
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        log.debug("Processing {} records", records.size());
        try {
            writer.write(prepare(records));
        } catch (DataException e) {
            throw e;
        } catch (RedisCommandExecutionException e) {
            String message = e.getMessage();
            if (message != null && (message.startsWith("MOVED") || message.startsWith("ASK"))) {
                if (client instanceof RedisClusterClient cluster) {
                    cluster.refreshPartitionsAsync();
                }
                throw new RetriableException("Redis cluster slot ownership changed", e);
            }
            if (message != null && (message.startsWith("READONLY") || message.startsWith("LOADING")
                    || message.startsWith("TRYAGAIN") || message.startsWith("CLUSTERDOWN"))) {
                throw new RetriableException("Redis is temporarily unable to accept writes", e);
            }
            throw new ConnectException("Redis rejected sink commands", e);
        } catch (RedisConnectionException e) {
            throw new RetriableException("Could not get connection to Redis", e);
        } catch (RedisCommandTimeoutException e) {
            throw new RetriableException("Timeout while writing sink records", e);
        } catch (Exception e) {
            throw new RetriableException("Could not write sink records", e);
        }
        log.info("Wrote {} records", records.size());
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        Map<String, String> offsets = currentOffsets.entrySet().stream().map(this::offsetState)
                .collect(Collectors.toMap(this::offsetKey, RedisSinkTask::offsetValue));
        if (offsets.isEmpty()) {
            return;
        }
        log.trace("Writing offsets: {}", offsets);
        try {
            // Individual SETs also support cluster offset keys spanning multiple slots.
            offsets.forEach((key, value) -> strings().set(key, value));
        } catch (RedisCommandTimeoutException e) {
            throw new RetriableException("Could not write offsets", e);
        }
    }

    private SinkOffsetState offsetState(Entry<TopicPartition, OffsetAndMetadata> entry) {
        return SinkOffsetState.of(entry.getKey(), entry.getValue().offset());
    }

}
