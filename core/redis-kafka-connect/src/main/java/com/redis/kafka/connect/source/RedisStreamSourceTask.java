package com.redis.kafka.connect.source;

import com.redis.kafka.connect.common.ManifestVersionProvider;
import com.redis.kafka.connect.common.RedisConnection;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.ClaimedMessages;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class RedisStreamSourceTask extends SourceTask {
    public static final String TASK_ID = "task.id";
    public static final String OFFSET_FIELD = "offset";

    private final Clock clock;
    private final List<String> delivered = new ArrayList<>();
    private AbstractRedisClient client;
    private volatile RedisConnection<String, String> connection;
    private RedisStreamSourceConfig config;
    private StreamMessageConverter converter;
    private Consumer<String> consumer;
    private String resumeOffset;
    private String pendingCursor = "0-0";
    private String claimCursor = "0-0";
    private boolean recovering = true;
    private volatile boolean stopped;

    public RedisStreamSourceTask() {
        this(Clock.systemDefaultZone());
    }

    public RedisStreamSourceTask(Clock clock) {
        this.clock = clock;
    }

    @Override
    public String version() {
        return ManifestVersionProvider.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new RedisStreamSourceConfig(props);
        converter = new StreamMessageConverter(clock, config);
        String taskId = props.getOrDefault(TASK_ID, "0");
        consumer = Consumer.from(config.getStreamConsumerGroup(),
                config.getStreamConsumerName().replace(RedisStreamSourceConfigDef.TOKEN_TASK, taskId));
        resumeOffset = config.getStreamOffset();
        if (context != null) {
            Map<String, Object> offset = context.offsetStorageReader().offset(config.sourcePartition());
            if (offset == null) {
                offset = context.offsetStorageReader().offset(Collections.emptyMap());
            }
            if (offset != null && offset.containsKey(OFFSET_FIELD)) {
                Object value = offset.get(OFFSET_FIELD);
                if (!(value instanceof String)) {
                    throw new ConnectException("Stored Redis stream offset must be a string");
                }
                resumeOffset = (String) value;
                if (!resumeOffset.matches("[0-9]+-[0-9]+")) {
                    throw new ConnectException("Stored Redis stream offset is not a stream ID");
                }
            }
        }
        recovering = true;
        stopped = false;
        pendingCursor = "0-0";
        claimCursor = "0-0";
        synchronized (delivered) {
            delivered.clear();
        }
        client = config.client();
        try {
            connection = RedisConnection.open(client, StringCodec.UTF8);
            try {
                commands().xgroupCreate(StreamOffset.from(config.getStreamName(), resumeOffset), consumer.getGroup(),
                        XGroupCreateArgs.Builder.mkstream(true));
            } catch (RedisBusyException e) {
                // An existing consumer group owns its position; never reset it during a restart.
            }
        } catch (Exception e) {
            stop();
            throw new ConnectException("Could not start Redis stream source", e);
        }
    }

    @SuppressWarnings("unchecked")
    private RedisStreamCommands<String, String> commands() {
        return connection.sync(RedisStreamCommands.class);
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (!atMostOnce() && record.sourceOffset() != null) {
            Object id = record.sourceOffset().get(OFFSET_FIELD);
            if (!(id instanceof String)) {
                throw new ConnectException("Redis stream record has no valid offset");
            }
            synchronized (delivered) {
                delivered.add((String) id);
            }
        }
    }

    @Override
    public void commit() {
        if (connection == null || atMostOnce()) {
            return;
        }
        List<String> batch;
        synchronized (delivered) {
            batch = new ArrayList<>(delivered);
        }
        if (batch.isEmpty()) {
            return;
        }
        try {
            commands().xack(config.getStreamName(), consumer.getGroup(), batch.toArray(String[]::new));
            synchronized (delivered) {
                // Do not clear delivery callbacks that arrived while XACK was in flight.
                delivered.subList(0, batch.size()).clear();
            }
        } catch (Exception e) {
            throw new ConnectException("Could not acknowledge Redis stream records", e);
        }
    }

    @Override
    public List<SourceRecord> poll() {
        if (stopped) {
            return List.of();
        }
        try {
            List<StreamMessage<String, String>> messages = read();
            if (atMostOnce() && !messages.isEmpty()) {
                ack(messages);
            }
            return messages.stream().map(converter).toList();
        } catch (RedisCommandTimeoutException e) {
            if (stopped) {
                return List.of();
            }
            throw new RetriableException("Timeout while reading Redis stream", e);
        } catch (Exception e) {
            if (stopped) {
                return List.of();
            }
            throw new ConnectException("Could not read Redis stream", e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<StreamMessage<String, String>> read() {
        RedisStreamCommands<String, String> commands = commands();
        XReadArgs pendingArgs = XReadArgs.Builder.count(config.getBatchSize());
        if (recovering && !stopped) {
            List<StreamMessage<String, String>> pending = commands.xreadgroup(consumer, pendingArgs,
                    StreamOffset.from(config.getStreamName(), pendingCursor));
            if (pending.isEmpty()) {
                recovering = false;
            } else {
                pendingCursor = pending.get(pending.size() - 1).getId();
                List<StreamMessage<String, String>> replay = new ArrayList<>();
                List<StreamMessage<String, String>> completed = new ArrayList<>();
                for (StreamMessage<String, String> message : pending) {
                    if (atMostOnce() || message.getBody() == null) {
                        completed.add(message);
                    } else {
                        replay.add(message);
                    }
                }
                ack(completed);
                // Recover at most one page per poll, even when every entry is deleted or auto-acked.
                return replay;
            }
        }
        if (stopped) {
            return List.of();
        }
        long idle = config.getLong(RedisStreamSourceConfigDef.STREAM_CLAIM_IDLE_CONFIG);
        if (idle > 0) {
            ClaimedMessages<String, String> claimed = commands.xautoclaim(config.getStreamName(),
                    XAutoClaimArgs.Builder.xautoclaim(consumer, idle, claimCursor).count(config.getBatchSize()));
            claimCursor = claimed.getId();
            if (!claimed.getMessages().isEmpty()) {
                return claimed.getMessages();
            }
        }
        XReadArgs args = XReadArgs.Builder.count(config.getBatchSize());
        // Redis BLOCK 0 means forever. Preserve a bounded poll even when configured with zero.
        if (config.getStreamBlock() > 0) {
            args.block(Math.min(config.getStreamBlock(), config.uri().getTimeout().toMillis() / 2));
        }
        return commands.xreadgroup(consumer, args, StreamOffset.lastConsumed(config.getStreamName()));
    }

    private boolean atMostOnce() {
        return RedisStreamSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE.equals(config.getStreamDelivery());
    }

    private void ack(List<StreamMessage<String, String>> messages) {
        if (!messages.isEmpty()) {
            commands().xack(config.getStreamName(), consumer.getGroup(),
                    messages.stream().map(StreamMessage::getId).toArray(String[]::new));
        }
    }

    @Override
    public void stop() {
        stopped = true;
        try {
            RedisConnection<String, String> current = connection;
            connection = null;
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
}
