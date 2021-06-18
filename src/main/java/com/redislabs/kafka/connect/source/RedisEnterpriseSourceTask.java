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
package com.redislabs.kafka.connect.source;

import com.redislabs.kafka.connect.RedisEnterpriseSourceConnector;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.redis.StreamItemReader;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RedisEnterpriseSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(RedisEnterpriseSourceTask.class);

    public static final String STREAM_FIELD = "stream";
    public static final String OFFSET_FIELD = "offset";

    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final String VALUE_SCHEMA_NAME = "com.redislabs.kafka.connect.EventValue";
    private static final Schema VALUE_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).name(VALUE_SCHEMA_NAME);

    private RedisClient client;
    private RedisEnterpriseSourceConfig sourceConfig;
    private StreamItemReader reader;
    private Map<String, String> offsetKey;

    @Override
    public String version() {
        return new RedisEnterpriseSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.sourceConfig = new RedisEnterpriseSourceConfig(props);
        this.client = RedisClient.create(sourceConfig.getRedisUri());
        this.offsetKey = Collections.singletonMap(STREAM_FIELD, sourceConfig.getStreamName());
        String offset = sourceConfig.getStreamOffset();
        if (context != null) {
            Map<String, Object> storedOffset = context.offsetStorageReader().offset(offsetKey);
            if (storedOffset != null) {
                Object lastRecordedOffset = storedOffset.get(OFFSET_FIELD);
                if (lastRecordedOffset != null) {
                    if (lastRecordedOffset instanceof String) {
                        log.info("Found previous offset: {}", lastRecordedOffset);
                        offset = (String) lastRecordedOffset;
                    } else {
                        throw new ConnectException("Offset position is the incorrect type");
                    }
                }
            }
        }
        XReadArgs.StreamOffset<String> streamOffset = XReadArgs.StreamOffset.from(sourceConfig.getStreamName(), offset);
        this.reader = StreamItemReader.client(client).offset(streamOffset).block(Duration.ofMillis(sourceConfig.getStreamBlock())).count(sourceConfig.getStreamCount()).build();
        this.reader.open(new ExecutionContext());
    }

    @Override
    public void stop() {
        if (reader != null) {
            reader.close();
        }
        if (client != null) {
            client.shutdown();
            client.getResources().shutdown();
        }
    }

    @Override
    public List<SourceRecord> poll() {
        log.debug("Reading from offset {}", reader.getOffset().getOffset());
        List<SourceRecord> records = new ArrayList<>();
        for (StreamMessage<String, String> message : reader.readMessages()) {
            Map<String, String> offsetValue = Collections.singletonMap(OFFSET_FIELD, message.getId());
            String topic = sourceConfig.getTopicNameFormat().replace(RedisEnterpriseSourceConfig.TOKEN_STREAM, message.getStream());
            records.add(new SourceRecord(offsetKey, offsetValue, topic, null, KEY_SCHEMA, message.getId(), VALUE_SCHEMA, message.getBody(), Instant.now().getEpochSecond()));
        }
        if (records.isEmpty()) {
            //TODO return heartbeat
            return null;
        }
        log.info("Returning {} source records", records.size());
        return records;
    }

}
