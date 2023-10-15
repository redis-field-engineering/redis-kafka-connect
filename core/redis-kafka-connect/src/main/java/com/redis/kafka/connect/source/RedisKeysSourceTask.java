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
package com.redis.kafka.connect.source;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

import com.redis.kafka.connect.common.ManifestVersionProvider;
import com.redis.spring.batch.RedisItemReader.ReaderMode;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.KeyValueItemReader;
import com.redis.spring.batch.reader.StructItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

public class RedisKeysSourceTask extends SourceTask {

    public static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;

    /**
     * The offsets that have been processed and that are to be acknowledged by the reader in
     * {@link RedisKeysSourceTask#commit()}.
     */
    private final List<Map<String, ?>> sourceOffsets = new ArrayList<>();

    private final ToStructFunction converter = new ToStructFunction();

    private final Clock clock;

    private String topic;

    private int batchSize;

    private StructItemReader<String, String> reader;

    private AbstractRedisClient client;

    public RedisKeysSourceTask() {
        this(Clock.systemDefaultZone());
    }

    public RedisKeysSourceTask(Clock clock) {
        this.clock = clock;
    }

    @Override
    public String version() {
        return ManifestVersionProvider.getVersion();
    }

    public KeyValueItemReader<String, String> getReader() {
        return reader;
    }

    @Override
    public void start(Map<String, String> props) {
        RedisKeysSourceConfig config = new RedisKeysSourceConfig(props);
        this.topic = config.getTopicName();
        this.batchSize = Math.toIntExact(config.getBatchSize());
        this.client = config.client();
        reader = new StructItemReader<>(client, StringCodec.UTF8);
        reader.setMode(ReaderMode.LIVE);
        reader.setPoolSize(config.getPoolSize());
        reader.setDatabase(config.uri().getDatabase());
        reader.setKeyPattern(config.getKeyPattern());
        reader.setChunkSize(batchSize);
        if (!config.getIdleTimeout().isNegative() && !config.getIdleTimeout().isZero()) {
            reader.setIdleTimeout(config.getIdleTimeout());
        }
        try {
            reader.open(new ExecutionContext());
        } catch (ItemStreamException e) {
            throw new RetriableException("Could not open reader", e);
        }
    }

    private void addSourceOffset(Map<String, ?> sourceOffset) {
        sourceOffsets.add(sourceOffset);
    }

    @Deprecated
    @Override
    public void commitRecord(SourceRecord sourceRecord) throws InterruptedException {
        Map<String, ?> currentOffset = sourceRecord.sourceOffset();
        if (currentOffset != null) {
            addSourceOffset(currentOffset);
        }
    }

    @Override
    public void commit() throws InterruptedException {
        // do nothing
    }

    @Override
    public void stop() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        if (client != null) {
            client.shutdown();
            client.getResources().shutdown();
            client = null;
        }
    }

    private SourceRecord convert(KeyValue<String> input) {
        Map<String, ?> partition = new HashMap<>();
        Map<String, ?> offset = new HashMap<>();
        String key = input.getKey();
        long epoch = clock.instant().toEpochMilli();
        return new SourceRecord(partition, offset, topic, null, KEY_SCHEMA, key, ToStructFunction.VALUE_SCHEMA,
                converter.apply(input), epoch);
    }

    @Override
    public List<SourceRecord> poll() {
        // TODO: return heartbeat if no records
        return reader.read(batchSize).stream().map(this::convert).collect(Collectors.toList());
    }

}
