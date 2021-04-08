package com.redislabs.kafkaconnect.sink;

import com.redislabs.kafkaconnect.RedisEnterpriseSinkConnector;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisOperationItemWriter;
import org.springframework.batch.item.redis.RedisTransactionItemWriter;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.RedisOperationBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class RedisEnterpriseSinkTask extends SinkTask {

    private RedisClient client;
    private RedisEnterpriseSinkConfig sinkConfig;
    private ItemWriter<SinkRecord> writer;
    private GenericObjectPool<StatefulRedisConnection<String, String>> pool;

    @Override
    public String version() {
        return new RedisEnterpriseSinkConnector().version();
    }

    @Override
    public void start(final Map<String, String> props) {
        sinkConfig = new RedisEnterpriseSinkConfig(props);
        client = RedisClient.create(sinkConfig.getRedisUri());
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(1);
        pool = ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig);
        writer = writer(pool, Boolean.TRUE.equals(sinkConfig.isMultiexec()));
        if (writer instanceof ItemStreamSupport) {
            ((ItemStreamSupport) writer).open(new ExecutionContext());
        }
    }

    private ItemWriter<SinkRecord> writer(GenericObjectPool<StatefulRedisConnection<String, String>> pool, boolean transactional) {
        XAddArgs args = new XAddArgs();
        RedisOperation<String, String, SinkRecord> xadd = RedisOperationBuilder.<String, String, SinkRecord>xadd().keyConverter(this::key).argsConverter(i -> args).bodyConverter(this::body).build();
        if (Boolean.TRUE.equals(transactional)) {
            return new RedisTransactionItemWriter<>(pool, xadd);
        }
        return new RedisOperationItemWriter<>(pool, xadd);
    }

    private String key(SinkRecord record) {
        return sinkConfig.getStreamNameFormat().replace("${topic}", record.topic());
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
        if (writer != null && writer instanceof ItemStreamSupport) {
            ((ItemStreamSupport) writer).close();
        }
        if (pool != null) {
            pool.close();
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
        try {
            writer.write(new ArrayList<>(records));
            log.info("Wrote {} records", records.size());
        } catch (Exception e) {
            log.warn("Could not write {} records", records.size(), e);
        }
    }


}
