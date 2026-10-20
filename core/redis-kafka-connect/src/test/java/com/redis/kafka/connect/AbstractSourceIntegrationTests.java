package com.redis.kafka.connect;

import com.redis.kafka.connect.common.RedisConfigDef;
import com.redis.kafka.connect.source.KeySnapshot;
import com.redis.kafka.connect.source.RedisKeysSourceConfig;
import com.redis.kafka.connect.source.RedisKeysSourceConfigDef;
import com.redis.kafka.connect.source.RedisKeysSourceTask;
import com.redis.kafka.connect.source.RedisStreamSourceConfig;
import com.redis.kafka.connect.source.RedisStreamSourceConfigDef;
import com.redis.kafka.connect.source.RedisStreamSourceTask;
import com.redis.kafka.connect.source.ToStructFunction;
import io.lettuce.core.KillArgs;
import io.lettuce.core.XClaimArgs;
import io.lettuce.core.Consumer;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.models.stream.PendingMessages;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract class AbstractSourceIntegrationTests extends AbstractRedisIntegrationTests {

    private static final Logger log = LoggerFactory.getLogger(AbstractSourceIntegrationTests.class);

    private RedisStreamSourceTask streamSourceTask;

    private RedisKeysSourceTask keysSourceTask;

    private final List<SourceTask> tasks = new ArrayList<>();

    private final ExecutorService generatorExecutor = Executors.newSingleThreadExecutor();

    @BeforeEach
    public void setupTasks() {
        streamSourceTask = new RedisStreamSourceTask();
        keysSourceTask = new RedisKeysSourceTask();
        tasks.add(streamSourceTask);
        tasks.add(keysSourceTask);
    }

    @AfterEach
    public void teardownTasks() {
        tasks.forEach(SourceTask::stop);
        tasks.clear();
    }

    @AfterAll
    void shutdownGeneratorExecutor() {
        generatorExecutor.shutdownNow();
    }

    // Used to initialize a task with a previous connect offset (as though records
    // had been committed).
    void initializeTask(String id) {
        SourceTaskContext taskContext = mock(SourceTaskContext.class);
        OffsetStorageReader offsets = mock(OffsetStorageReader.class);
        when(offsets.offset(anyMap())).thenReturn(Map.of(RedisStreamSourceTask.OFFSET_FIELD, id));
        when(taskContext.offsetStorageReader()).thenReturn(offsets);
        streamSourceTask.initialize(taskContext);
    }

    private void startTask(SourceTask task, String... props) {
        Map<String, String> config = map(props);
        config.put(RedisConfigDef.URI_CONFIG, getRedisServer().getRedisURI());
        task.start(config);
    }

    private void startStreamSourceTask(String... props) {
        startTask(streamSourceTask, props);
    }

    private void startKeysSourceTask(String... props) {
        startTask(keysSourceTask, props);
    }

    protected Map<String, String> map(String... args) {
        Objects.requireNonNull(args, "Args cannot be null");
        if (args.length % 2 != 0) { throw new IllegalArgumentException("Args length is not a multiple of 2"); }
        Map<String, String> body = new LinkedHashMap<>();
        for (int index = 0; index < args.length / 2; index++) {
            body.put(args[index * 2], args[index * 2 + 1]);
        }
        return body;
    }

    @Test
    void pollStreamAtMostOnce() throws InterruptedException {
        String stream = "stream1";
        String topicPrefix = "testprefix-";
        startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG, topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM,
                RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, stream, RedisStreamSourceConfigDef.STREAM_DELIVERY_CONFIG,
                RedisStreamSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        String id1 = redisConnection.sync().xadd(stream, body);
        String id2 = redisConnection.sync().xadd(stream, body);
        String id3 = redisConnection.sync().xadd(stream, body);
        List<SourceRecord> sourceRecords = new ArrayList<>();
        Awaitility.await().until(() -> sourceRecords.addAll(streamSourceTask.poll()));
        Assertions.assertEquals(3, sourceRecords.size());
        assertEquals(id1, body, stream, topicPrefix + stream, sourceRecords.get(0));
        assertEquals(id2, body, stream, topicPrefix + stream, sourceRecords.get(1));
        assertEquals(id3, body, stream, topicPrefix + stream, sourceRecords.get(2));
        PendingMessages pendingMsgs = redisConnection.sync()
                .xpending(stream, RedisStreamSourceConfigDef.STREAM_CONSUMER_GROUP_DEFAULT);
        Assertions.assertEquals(0, pendingMsgs.getCount(), "pending messages");
    }

    @Test
    void pollStreamAtLeastOnce() throws InterruptedException {
        String stream = "stream1";
        String topicPrefix = "testprefix-";
        startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG, topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM,
                RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, stream);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        String id1 = redisConnection.sync().xadd(stream, body);
        String id2 = redisConnection.sync().xadd(stream, body);
        String id3 = redisConnection.sync().xadd(stream, body);
        List<SourceRecord> sourceRecords = new ArrayList<>();
        Awaitility.await().until(() -> sourceRecords.addAll(streamSourceTask.poll()));
        Assertions.assertEquals(3, sourceRecords.size());
        assertEquals(id1, body, stream, topicPrefix + stream, sourceRecords.get(0));
        assertEquals(id2, body, stream, topicPrefix + stream, sourceRecords.get(1));
        assertEquals(id3, body, stream, topicPrefix + stream, sourceRecords.get(2));
        PendingMessages pendingMsgsBeforeCommit = redisConnection.sync()
                .xpending(stream, RedisStreamSourceConfigDef.STREAM_CONSUMER_GROUP_DEFAULT);
        Assertions.assertEquals(3, pendingMsgsBeforeCommit.getCount(), "pending messages before commit");
        streamSourceTask.commitRecord(sourceRecords.get(0), new RecordMetadata(null, 0, 0, 0, 0, 0));
        streamSourceTask.commitRecord(sourceRecords.get(1), new RecordMetadata(null, 0, 0, 0, 0, 0));
        streamSourceTask.commit();
        PendingMessages pendingMsgsAfterCommit = redisConnection.sync()
                .xpending(stream, RedisStreamSourceConfigDef.STREAM_CONSUMER_GROUP_DEFAULT);
        Assertions.assertEquals(1, pendingMsgsAfterCommit.getCount(), "pending messages after commit");
    }

    @Test
    void pollStreamAtLeastOnceRecover() throws InterruptedException {
        String stream = "stream1";
        String topicPrefix = "testprefix-";
        startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG, topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM,
                RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, stream);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        redisConnection.sync().xadd(stream, body);
        redisConnection.sync().xadd(stream, body);
        redisConnection.sync().xadd(stream, body);
        List<SourceRecord> sourceRecords = new ArrayList<>();
        Awaitility.await().until(() -> sourceRecords.addAll(streamSourceTask.poll()));
        Assertions.assertEquals(3, sourceRecords.size());

        List<SourceRecord> recoveredRecords = new ArrayList<>();
        redisConnection.sync().xadd(stream, body);
        redisConnection.sync().xadd(stream, body);
        redisConnection.sync().xadd(stream, body);

        // create a new task, same config
        setupTasks();
        startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG, topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM,
                RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, stream);

        Awaitility.await().until(() -> recoveredRecords.addAll(streamSourceTask.poll()));
        Awaitility.await().until(() -> !recoveredRecords.addAll(streamSourceTask.poll()));

        Assertions.assertEquals(6, recoveredRecords.size());
    }

    @Test
    void pollStreamAtLeastOnceRecoverUncommitted() throws InterruptedException {
        String stream = "stream1";
        String topicPrefix = "testprefix-";
        startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG, topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM,
                RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, stream);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        redisConnection.sync().xadd(stream, body);
        redisConnection.sync().xadd(stream, body);
        String id3 = redisConnection.sync().xadd(stream, body);
        List<SourceRecord> sourceRecords = new ArrayList<>();
        Awaitility.await().until(() -> sourceRecords.addAll(streamSourceTask.poll()));
        Assertions.assertEquals(3, sourceRecords.size());
        streamSourceTask.commitRecord(sourceRecords.get(0), new RecordMetadata(null, 0, 0, 0, 0, 0));
        streamSourceTask.commitRecord(sourceRecords.get(1), new RecordMetadata(null, 0, 0, 0, 0, 0));
        streamSourceTask.commit();

        List<SourceRecord> recoveredRecords = new ArrayList<>();
        String id4 = redisConnection.sync().xadd(stream, body);
        String id5 = redisConnection.sync().xadd(stream, body);
        String id6 = redisConnection.sync().xadd(stream, body);

        // create a new task, same config
        setupTasks();
        startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG, topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM,
                RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, stream);

        // Wait until task.poll() doesn't return any more records
        Awaitility.await().until(() -> recoveredRecords.addAll(streamSourceTask.poll()));
        Awaitility.await().until(() -> !recoveredRecords.addAll(streamSourceTask.poll()));
        List<String> recoveredIds = recoveredRecords.stream().map(SourceRecord::key).map(String::valueOf)
                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.<String> asList(id3, id4, id5, id6), recoveredIds, "recoveredIds");
    }

    @Test
    void pollStreamAtLeastOnceRecoverFromOffset() throws Exception {
        String stream = "stream1";
        String topicPrefix = "testprefix-";
        startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG, topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM,
                RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, stream);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        String id1 = redisConnection.sync().xadd(stream, body);
        log.info("ID1: " + id1);
        String id2 = redisConnection.sync().xadd(stream, body);
        log.info("ID2: " + id2);
        String id3 = redisConnection.sync().xadd(stream, body);
        log.info("ID3: " + id3);
        List<SourceRecord> records = new ArrayList<>();
        Awaitility.await().until(() -> records.addAll(streamSourceTask.poll()));
        Assertions.assertEquals(3, records.size());

        List<SourceRecord> recoveredRecords = new ArrayList<>();
        String id4 = redisConnection.sync().xadd(stream, body);
        log.info("ID4: " + id4);
        String id5 = redisConnection.sync().xadd(stream, body);
        log.info("ID5: " + id5);
        String id6 = redisConnection.sync().xadd(stream, body);
        log.info("ID6: " + id6);

        // create a new task, same config
        setupTasks();
        // Kafka committed these records, but Redis did not receive their acknowledgments.
        // Pending entries replay: a checkpoint cannot prove that a lower claimed ID was delivered.
        initializeTask(id3);
        startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG, topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM,
                RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, stream);

        // Wait until task.poll() doesn't return any more records
        Awaitility.await().until(() -> recoveredRecords.addAll(streamSourceTask.poll()));
        Awaitility.await().until(() -> !recoveredRecords.addAll(streamSourceTask.poll()));

        List<String> recoveredIds = recoveredRecords.stream().map(SourceRecord::key).map(String::valueOf)
                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.<String> asList(id1, id2, id3, id4, id5, id6), recoveredIds, "pending messages replay even when Kafka checkpoint is ahead");
    }

    @Test
    void pollStreamAtMostOnceRecover() throws InterruptedException {
        String stream = "stream1";
        String topicPrefix = "testprefix-";
        startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG, topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM,
                RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, stream, RedisStreamSourceConfigDef.STREAM_DELIVERY_CONFIG,
                RedisStreamSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        redisConnection.sync().xadd(stream, body);
        redisConnection.sync().xadd(stream, body);
        redisConnection.sync().xadd(stream, body);
        List<SourceRecord> sourceRecords = new ArrayList<>();
        Awaitility.await().until(() -> sourceRecords.addAll(streamSourceTask.poll()));
        Assertions.assertEquals(3, sourceRecords.size());

        List<SourceRecord> recoveredRecords = new ArrayList<>();
        String id4 = redisConnection.sync().xadd(stream, body);
        String id5 = redisConnection.sync().xadd(stream, body);
        String id6 = redisConnection.sync().xadd(stream, body);

        // create a new task, same config
        setupTasks();
        startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG, topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM,
                RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, stream, RedisStreamSourceConfigDef.STREAM_DELIVERY_CONFIG,
                RedisStreamSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE);

        // Wait until task.poll() doesn't return any more records
        Awaitility.await().until(() -> recoveredRecords.addAll(streamSourceTask.poll()));
        Awaitility.await().until(() -> !recoveredRecords.addAll(streamSourceTask.poll()));
        List<String> recoveredIds = recoveredRecords.stream().map(SourceRecord::key).map(String::valueOf)
                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");
    }

    @Test
    void pollStreamRecoverAtLeastOnceToAtMostOnce() throws InterruptedException {
        String stream = "stream1";
        String topicPrefix = "testprefix-";
        startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG, topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM,
                RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, stream, RedisStreamSourceConfigDef.STREAM_DELIVERY_CONFIG,
                RedisStreamSourceConfig.STREAM_DELIVERY_AT_LEAST_ONCE);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        redisConnection.sync().xadd(stream, body);
        redisConnection.sync().xadd(stream, body);
        redisConnection.sync().xadd(stream, body);
        List<SourceRecord> sourceRecords = new ArrayList<>();
        Awaitility.await().until(() -> sourceRecords.addAll(streamSourceTask.poll()));
        Assertions.assertEquals(3, sourceRecords.size());

        List<SourceRecord> recoveredRecords = new ArrayList<>();
        String id4 = redisConnection.sync().xadd(stream, body);
        String id5 = redisConnection.sync().xadd(stream, body);
        String id6 = redisConnection.sync().xadd(stream, body);

        // create a new task, same config except AT_MOST_ONCE
        setupTasks();
        startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG, topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM,
                RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, stream, RedisStreamSourceConfigDef.STREAM_DELIVERY_CONFIG,
                RedisStreamSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE);

        // Wait until task.poll() doesn't return any more records
        Awaitility.await().until(() -> recoveredRecords.addAll(streamSourceTask.poll()));
        Awaitility.await().until(() -> !recoveredRecords.addAll(streamSourceTask.poll()));
        List<String> recoveredIds = recoveredRecords.stream().map(SourceRecord::key).map(String::valueOf)
                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");

        PendingMessages pending = redisConnection.sync()
                .xpending(stream, RedisStreamSourceConfigDef.STREAM_CONSUMER_GROUP_DEFAULT);
        Assertions.assertEquals(0, pending.getCount(), "pending message count");
    }

    private void assertEquals(String expectedId, Map<String, String> expectedBody, String expectedStream, String expectedTopic,
            SourceRecord record) {
        Struct struct = (Struct) record.value();
        Assertions.assertEquals(expectedId, struct.get("id"));
        Assertions.assertEquals(expectedBody, struct.get("body"));
        Assertions.assertEquals(expectedStream, struct.get("stream"));
        Assertions.assertEquals(expectedTopic, record.topic());
    }

    @Test
    void pollKeysLive(TestInfo info) throws Exception {
        generateKeys("snapshot", 100);
        enableKeyspaceNotifications();
        String topic = "mytopic";
        startKeysSourceTask(RedisKeysSourceConfigDef.TOPIC_CONFIG, topic, RedisKeysSourceConfigDef.IDLE_TIMEOUT_CONFIG, "500");
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        generatorExecutor.submit(() -> generateKeys("live", 100)).get(20, TimeUnit.SECONDS);
        awaitUntil(() -> {
            List<SourceRecord> records = keysSourceTask.poll();
            log.info("Polled {} records", records.size());
            sourceRecords.addAll(records);
            return sourceRecords.stream().map(SourceRecord::key).distinct().count() >= 200;
        });
        for (SourceRecord record : sourceRecords) {
            Assertions.assertEquals(topic, record.topic());
            Compare compare = values((Struct) record.value());
            if (compare != null) {
                Assertions.assertEquals(compare.expected, compare.actual);
            }
        }
    }

    @Test
    void pollKeysLiveOnly(TestInfo info) throws Exception {
        enableKeyspaceNotifications();
        String topic = "mytopic";
        startKeysSourceTask(RedisKeysSourceConfigDef.TOPIC_CONFIG, topic, RedisKeysSourceConfigDef.IDLE_TIMEOUT_CONFIG, "500",
                RedisKeysSourceConfigDef.MODE_CONFIG, RedisKeysSourceConfig.ReaderMode.LIVEONLY.name());
        int count = 100;
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        generatorExecutor.submit(() -> generateKeys("liveonly", count)).get(20, TimeUnit.SECONDS);
        awaitUntil(() -> {
            List<SourceRecord> records = keysSourceTask.poll();
            log.info("Polled {} records", records.size());
            sourceRecords.addAll(records);
            return sourceRecords.stream().map(SourceRecord::key).distinct().count() >= count;
        });
        for (SourceRecord record : sourceRecords) {
            Assertions.assertEquals(topic, record.topic());
            Compare compare = values((Struct) record.value());
            if (compare != null) {
                Assertions.assertEquals(compare.expected, compare.actual);
            }
        }
    }

    private static class Compare {

        private final Object expected;

        private final Object actual;

        public Compare(Object expected, Object actual) {
            this.expected = expected;
            this.actual = actual;
        }

    }

    private Compare values(Struct struct) {
        String key = struct.getString(ToStructFunction.FIELD_KEY);
        String type = struct.getString(ToStructFunction.FIELD_TYPE);
        Assertions.assertEquals(redisConnection.sync().type(key), type);
        RedisCommands<String, String> commands = redisConnection.sync();
        switch (type) {
            case KeySnapshot.TYPE_HASH:
                return compare(commands.hgetall(key), struct.getMap(ToStructFunction.FIELD_HASH));
            case KeySnapshot.TYPE_JSON:
                return compare(commands.jsonGetRaw(key).get(0), struct.getString(ToStructFunction.FIELD_JSON));
            case KeySnapshot.TYPE_LIST:
                return compare(commands.lrange(key, 0, -1), struct.getArray(ToStructFunction.FIELD_LIST));
            case KeySnapshot.TYPE_SET:
                return compare(commands.smembers(key), new HashSet<>(struct.getArray(ToStructFunction.FIELD_SET)));
            case KeySnapshot.TYPE_STRING:
                return compare(commands.get(key), struct.getString(ToStructFunction.FIELD_STRING));
            case KeySnapshot.TYPE_ZSET:
                return compare(ToStructFunction.zsetMap(commands.zrangeWithScores(key, 0, -1)),
                        struct.getMap(ToStructFunction.FIELD_ZSET));
            default:
                return null;
        }
    }

    private static Compare compare(Object expected, Object actual) {
        return new Compare(expected, actual);
    }


    @Test
    void recoversPendingMessagesInBoundedBatches() throws InterruptedException {
        startStreamSourceTask(RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, "bounded", "batch.size", "2");
        for (int index = 0; index < 7; index++) {
            redisConnection.sync().xadd("bounded", Map.of("value", Integer.toString(index)));
        }
        List<SourceRecord> first = new ArrayList<>();
        awaitUntil(() -> {
            first.addAll(streamSourceTask.poll());
            return first.size() == 7;
        });
        streamSourceTask.stop();
        setupTasks();
        startStreamSourceTask(RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, "bounded", "batch.size", "2");
        List<SourceRecord> recovered = new ArrayList<>();
        awaitUntil(() -> {
            List<SourceRecord> batch = streamSourceTask.poll();
            Assertions.assertTrue(batch.size() <= 2);
            recovered.addAll(batch);
            return recovered.size() == 7;
        });
        Assertions.assertEquals(first.stream().map(SourceRecord::key).toList(), recovered.stream().map(SourceRecord::key).toList());
    }

    @Test
    void reclaimsPendingFromRemovedConsumer() throws InterruptedException {
        startStreamSourceTask(RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, "claim", "redis.stream.consumer.name", "removed");
        String id = redisConnection.sync().xadd("claim", Map.of("value", "value"));
        Assertions.assertEquals(id, streamSourceTask.poll().get(0).key());
        streamSourceTask.stop();
        setupTasks();
        startStreamSourceTask(RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, "claim", "redis.stream.consumer.name", "replacement",
                RedisStreamSourceConfigDef.STREAM_CLAIM_IDLE_CONFIG, "1");
        List<SourceRecord> recovered = new ArrayList<>();
        awaitUntil(() -> recovered.addAll(streamSourceTask.poll()));
        Assertions.assertEquals(id, recovered.get(0).key());
        streamSourceTask.commitRecord(recovered.get(0), null);
        streamSourceTask.commit();
        Assertions.assertEquals(0, redisConnection.sync().xpending("claim", "kafka-consumer-group").getCount());
    }

    @Test
    void idleClaimDoesNotReplayInFlightRecords() throws Exception {
        String stream = "in-flight";
        startStreamSourceTask("redis.stream.name", stream, "redis.stream.block", "0",
                "redis.stream.consumer.name", "in-flight-owner",
                RedisStreamSourceConfigDef.STREAM_CLAIM_IDLE_CONFIG, "1");
        String id = redisConnection.sync().xadd(stream, Map.of("value", "first"));
        SourceRecord first = streamSourceTask.poll().get(0);
        // Make this task's own PEL entry eligible without a timing-dependent sleep.
        Consumer<String> consumer = Consumer.from("kafka-consumer-group", "in-flight-owner");
        redisConnection.sync().xclaim(stream, consumer,
                XClaimArgs.Builder.minIdleTime(0).idle(60000), id);
        Assertions.assertTrue(streamSourceTask.poll().isEmpty(), "not yet produced to Kafka");
        streamSourceTask.commitRecord(first, null);
        redisConnection.sync().xclaim(stream, consumer,
                XClaimArgs.Builder.minIdleTime(0).idle(60000), id);
        Assertions.assertTrue(streamSourceTask.poll().isEmpty(), "produced but not committed");
        String next = redisConnection.sync().xadd(stream, Map.of("value", "next"));
        Assertions.assertEquals(List.of(next), streamSourceTask.poll().stream().map(SourceRecord::key).toList());
        streamSourceTask.commit();
        Assertions.assertEquals(1, redisConnection.sync().xpending(stream, consumer.getGroup()).getCount());
        // Restart still replays records that never reached a successful XACK.
        setupTasks();
        startStreamSourceTask("redis.stream.name", stream, "redis.stream.block", "0",
                "redis.stream.consumer.name", "in-flight-owner",
                RedisStreamSourceConfigDef.STREAM_CLAIM_IDLE_CONFIG, "1");
        Assertions.assertEquals(List.of(next), streamSourceTask.poll().stream().map(SourceRecord::key).toList());
    }

    @Test
    void deletedKeyHasNoneTypeAndMissingExpiry() throws Exception {
        enableKeyspaceNotifications();
        startKeysSourceTask(RedisKeysSourceConfigDef.MODE_CONFIG, "LIVEONLY");
        redisConnection.sync().set("deleted", "value");
        redisConnection.sync().del("deleted");
        List<SourceRecord> records = new ArrayList<>();
        awaitUntil(() -> records.addAll(keysSourceTask.poll()));
        Struct value = (Struct) records.get(records.size() - 1).value();
        Assertions.assertEquals("none", value.getString("type"));
        Assertions.assertEquals(-2, value.getInt64("ttl"));
        Assertions.assertNull(value.getString("string"));
    }

    @Test
    void keyExpiryIsAnAbsoluteMillisecondTimestamp() throws Exception {
        redisConnection.sync().psetex("expiring", 60000, "value");
        startKeysSourceTask(RedisKeysSourceConfigDef.KEY_PATTERN_CONFIG, "expiring");
        List<SourceRecord> records = new ArrayList<>();
        awaitUntil(() -> records.addAll(keysSourceTask.poll()));
        long expiry = ((Struct) records.get(0).value()).getInt64("ttl");
        Assertions.assertTrue(expiry > System.currentTimeMillis() + 50000);
        Assertions.assertTrue(expiry <= System.currentTimeMillis() + 60000);
    }

    @Test
    void notificationOverflowFailsExplicitly() {
        enableKeyspaceNotifications();
        startKeysSourceTask(RedisKeysSourceConfigDef.MODE_CONFIG, "LIVEONLY",
                RedisKeysSourceConfigDef.QUEUE_CAPACITY_CONFIG, "1");
        for (int index = 0; index < 100; index++) {
            redisConnection.sync().set("overflow:" + index, "value");
        }
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            ConnectException error = Assertions.assertThrows(
                    ConnectException.class, () -> keysSourceTask.poll());
            Assertions.assertTrue(error.getMessage().contains("overflow"));
        });
    }

    @Test
    void notificationDisconnectFailsExplicitly() {
        enableKeyspaceNotifications();
        startKeysSourceTask(RedisKeysSourceConfigDef.MODE_CONFIG, "LIVEONLY");
        redisConnection.sync().clientKill(KillArgs.Builder.typePubsub());
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            ConnectException error = Assertions.assertThrows(
                    ConnectException.class, () -> keysSourceTask.poll());
            Assertions.assertTrue(error.getMessage().contains("interrupted"));
        });
    }

    @Test
    void legacySharedCheckpointDoesNotDiscardAnotherConsumersPendingRecords() throws Exception {
        startStreamSourceTask("redis.stream.name", "legacy-recovery");
        String first = redisConnection.sync().xadd("legacy-recovery", Map.of("value", "first"));
        String last = redisConnection.sync().xadd("legacy-recovery", Map.of("value", "last"));
        Assertions.assertEquals(2, streamSourceTask.poll().size());
        streamSourceTask.stop();
        setupTasks();
        SourceTaskContext taskContext = mock(SourceTaskContext.class);
        OffsetStorageReader offsets = mock(OffsetStorageReader.class);
        when(offsets.offset(anyMap())).thenAnswer(invocation -> {
            Map<?, ?> partition = invocation.getArgument(0);
            return partition.isEmpty() ? Map.of("offset", last) : null;
        });
        when(taskContext.offsetStorageReader()).thenReturn(offsets);
        streamSourceTask.initialize(taskContext);
        startStreamSourceTask("redis.stream.name", "legacy-recovery");
        Assertions.assertEquals(List.of(first, last), streamSourceTask.poll().stream().map(SourceRecord::key).toList());
    }

    @Test
    void keysStopWakesLongIdlePoll() throws Exception {
        startKeysSourceTask("mode", "LIVEONLY", "redis.keys.timeout", "60000");
        AtomicReference<Thread> polling = new AtomicReference<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<List<SourceRecord>> result = executor.submit(() -> {
                polling.set(Thread.currentThread());
                return keysSourceTask.poll();
            });
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> polling.get() != null
                    && polling.get().getState() == Thread.State.TIMED_WAITING);
            keysSourceTask.stop();
            Assertions.assertTrue(result.get(3, TimeUnit.SECONDS).isEmpty());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void streamCommitDoesNotWaitForBlockedRead() throws Exception {
        String stream = "commit-while-blocked";
        startStreamSourceTask("redis.stream.name", stream, "redis.stream.block", "30000");
        redisConnection.sync().xadd(stream, Map.of("value", "first"));
        SourceRecord first = streamSourceTask.poll().get(0);
        streamSourceTask.commitRecord(first, null);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            AtomicReference<Thread> pollingThread = new AtomicReference<>();
            Future<List<SourceRecord>> polling = executor.submit(() -> {
                pollingThread.set(Thread.currentThread());
                return streamSourceTask.poll();
            });
            // Enterprise's proxy does not expose the same blocked-client flags as Redis.
            // Wait until the task is awaiting the blocking command's reply on either server.
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> {
                Thread thread = pollingThread.get();
                return thread != null && (thread.getState() == Thread.State.WAITING
                        || thread.getState() == Thread.State.TIMED_WAITING)
                        && Arrays.stream(thread.getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("xreadgroup"));
            });
            Future<?> committing = executor.submit(() -> streamSourceTask.commit());
            committing.get(3, TimeUnit.SECONDS);
            Assertions.assertEquals(0, redisConnection.sync().xpending(stream, "kafka-consumer-group").getCount());
            Assertions.assertFalse(polling.isDone(), "acknowledgment must complete while the read remains blocked");
            streamSourceTask.stop();
            Assertions.assertTrue(polling.get(3, TimeUnit.SECONDS).isEmpty());
        } finally {
            streamSourceTask.stop();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void streamStopCancelsBlockedRead() throws Exception {
        startStreamSourceTask("redis.stream.name", "stop", "redis.stream.block", "30000");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<List<SourceRecord>> result = executor.submit(() -> streamSourceTask.poll());
            awaitUntil(() -> redisConnection.sync().clientList().contains("cmd=xreadgroup"));
            streamSourceTask.stop();
            Assertions.assertTrue(result.get(3, TimeUnit.SECONDS).isEmpty());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void keysReloadReadScriptAfterCacheFlush() throws Exception {
        redisConnection.sync().set("cached-script", "value");
        startKeysSourceTask("redis.keys.pattern", "cached-script");
        redisConnection.sync().scriptFlush();
        List<SourceRecord> records = new ArrayList<>();
        awaitUntil(() -> records.addAll(keysSourceTask.poll()));
        Assertions.assertEquals("value", ((Struct) records.get(0).value()).getString("string"));
    }

    @Test
    void claimedMessageBelowScopedCheckpointIsNotDiscarded() throws Exception {
        String stream = "claimed-lower";
        String group = "kafka-consumer-group";
        startStreamSourceTask("redis.stream.name", stream, "redis.stream.consumer.name", "removed");
        String lower = redisConnection.sync().xadd(stream, Map.of("value", "lower"));
        Assertions.assertEquals(lower, streamSourceTask.poll().get(0).key());
        streamSourceTask.stop();
        String higher = redisConnection.sync().xadd(stream, Map.of("value", "higher"));
        Consumer<String> replacement = Consumer.from(group, "replacement");
        redisConnection.sync().xreadgroup(replacement, StreamOffset.lastConsumed(stream));
        redisConnection.sync().xack(stream, group, higher);
        redisConnection.sync().xclaim(stream, replacement, 0, lower);
        setupTasks();
        initializeTask(higher);
        startStreamSourceTask("redis.stream.name", stream, "redis.stream.consumer.name", "replacement");
        Assertions.assertEquals(lower, streamSourceTask.poll().get(0).key());
    }

    @Test
    void liveUpdatesAndDeletesAreCapturedWhileSnapshotIsInProgress() throws Exception {
        enableKeyspaceNotifications();
        for (int index = 0; index < 20; index++) {
            redisConnection.sync().set("paging:" + index, "original");
        }
        startKeysSourceTask("batch.size", "2", "redis.keys.pattern", "paging:*");
        List<SourceRecord> first = new ArrayList<>();
        awaitUntil(() -> first.addAll(keysSourceTask.poll()));
        String alreadyRead = first.get(0).key().toString();
        String deleted = "paging:0".equals(alreadyRead) ? "paging:1" : "paging:0";
        redisConnection.sync().set(alreadyRead, "updated");
        redisConnection.sync().set("paging:new", "new");
        redisConnection.sync().del(deleted);
        List<SourceRecord> records = new ArrayList<>();
        awaitUntil(() -> {
            List<SourceRecord> batch = keysSourceTask.poll();
            Assertions.assertTrue(batch.size() <= 2);
            records.addAll(batch);
            boolean update = records.stream().anyMatch(record -> alreadyRead.equals(record.key())
                    && "updated".equals(((Struct) record.value()).getString("string")));
            boolean added = records.stream().anyMatch(record -> "paging:new".equals(record.key()));
            boolean removed = records.stream().anyMatch(record -> deleted.equals(record.key())
                    && "none".equals(((Struct) record.value()).getString("type")));
            return update && added && removed;
        });
    }
}
