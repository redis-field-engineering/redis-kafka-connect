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

import com.redislabs.kafkaconnect.RedisEnterpriseSinkConnector;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XAddArgs;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.batch.item.redis.RedisOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class RedisEnterpriseSinkTask extends SinkTask {

    private RedisClient client;
    private RedisEnterpriseSinkConfig sinkConfig;
    private OperationItemWriter<String, String, SinkRecord> writer;

    @Override
    public String version() {
        return new RedisEnterpriseSinkConnector().version();
    }

    @Override
    public void start(final Map<String, String> props) {
        sinkConfig = new RedisEnterpriseSinkConfig(props);
        client = RedisClient.create(sinkConfig.getRedisUri());
        XAddArgs args = new XAddArgs();
        writer = OperationItemWriter.operation(RedisOperation.<SinkRecord>xadd().key(this::key).args(i -> args).body(this::body).build()).client(client).transactional(Boolean.TRUE.equals(sinkConfig.isMultiexec())).build();
        writer.open(new ExecutionContext());
    }

    private String key(SinkRecord record) {
        return sinkConfig.getStreamNameFormat().replace(RedisEnterpriseSinkConfig.TOKEN_TOPIC, record.topic());
    }

    private Map<String, String> body(SinkRecord record) {
        Object value = record.value();
        if (value instanceof Struct) {
            Map<String, String> body = new LinkedHashMap<>();
            Struct struct = (Struct) value;
            for (Field field : struct.schema().fields()) {
                Object fieldValue = struct.get(field);
                if (fieldValue == null) {
                    continue;
                }
                body.put(field.name(), fieldValue.toString());
            }
            return body;
        }
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
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
        if (client != null) {
            client.shutdown();
            client.getResources().shutdown();
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        try {
            writer.write(new ArrayList<>(records));
            log.info("Wrote {} records", records.size());
        } catch (Exception e) {
            log.warn("Could not write {} records", records.size(), e);
        }
    }


}
