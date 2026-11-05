package com.redis.kafka.connect.source;

import com.redis.kafka.connect.common.ManifestVersionProvider;
import io.lettuce.core.AbstractRedisClient;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class RedisKeysSourceTask extends SourceTask {
    public static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;

    private final ToStructFunction converter = new ToStructFunction();
    private final Clock clock;
    private AbstractRedisClient client;
    private volatile RedisKeyReader reader;
    private volatile boolean stopped = true;
    private String topic;

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

    public RedisKeyReader getReader() {
        return reader;
    }

    @Override
    public void start(Map<String, String> props) {
        RedisKeysSourceConfig config = new RedisKeysSourceConfig(props);
        topic = config.getTopicName();
        stopped = false;
        client = config.client();
        reader = new RedisKeyReader(client, config);
        try {
            reader.open();
        } catch (Exception e) {
            stop();
            throw new ConnectException("Could not start Redis keys source", e);
        }
    }

    @Override
    public void stop() {
        stopped = true;
        RedisKeyReader current = reader;
        reader = null;
        try {
            if (current != null) {
                current.close();
            }
        } finally {
            if (client != null) {
                client.shutdown();
                client = null;
            }
        }
    }

    private SourceRecord convert(KeySnapshot input) {
        return new SourceRecord(Map.of(), Map.of(), topic, null, KEY_SCHEMA, input.key(), ToStructFunction.VALUE_SCHEMA,
                converter.apply(input), clock.instant().toEpochMilli());
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        RedisKeyReader current = reader;
        if (current == null) {
            return List.of();
        }
        try {
            return current.poll().stream().map(this::convert).toList();
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            if (stopped) {
                return List.of();
            }
            throw e instanceof ConnectException connect ? connect : new ConnectException("Could not read from Redis", e);
        }
    }
}
