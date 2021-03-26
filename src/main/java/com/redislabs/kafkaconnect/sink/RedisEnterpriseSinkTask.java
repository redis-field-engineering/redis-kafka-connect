package com.redislabs.kafkaconnect.sink;

import com.redislabs.kafkaconnect.RedisEnterpriseSinkConnector;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RedisEnterpriseSinkTask extends SinkTask {

    private RedisClient client;
    private StatefulRedisConnection<String, String> connection;
    private RedisAsyncCommands<String, String> commands;
    private RedisEnterpriseSinkConfig sinkConfig;
    private long timeout;

    @Override
    public String version() {
        return new RedisEnterpriseSinkConnector().version();
    }

    @Override
    public void start(final Map<String, String> props) {
        this.sinkConfig = new RedisEnterpriseSinkConfig(props);
        this.client = RedisClient.create(sinkConfig.getRedisUri());
        this.connection = client.connect();
        this.commands = connection.async();
        this.commands.setAutoFlushCommands(false);
        this.timeout = connection.getTimeout().toMillis();
    }

    @Override
    public void stop() {
        commands = null;
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        Map<SinkRecord, RedisFuture<?>> futures = new HashMap<>();
        for (SinkRecord record : records) {
            String stream = sinkConfig.getStreamNameFormat().replace("${topic}", record.topic());
            futures.put(record, commands.xadd(stream, body(record)));
        }
        commands.flushCommands();
        for (Map.Entry<SinkRecord, RedisFuture<?>> entry : futures.entrySet()) {
            try {
                entry.getValue().get(timeout, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                log.warn("Could not write record at offset {}", entry.getKey().kafkaOffset());
            }
        }
        log.info("Wrote {} messages to Redis", records.size());
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
            Map map = (Map) value;
            Map<String, String> body = new LinkedHashMap<>();
            map.forEach((k, v) -> body.put(String.valueOf(k), String.valueOf(v)));
            return body;
        }
        throw new ConnectException("Unsupported source value type: " + record.valueSchema().type().name());
    }

}
