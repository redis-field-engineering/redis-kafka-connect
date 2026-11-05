package com.redis.kafka.connect;

import com.redis.kafka.connect.common.RedisConnection;
import com.redis.kafka.connect.sink.RedisSinkConfig;
import com.redis.kafka.connect.sink.RedisSinkTask;
import com.redis.kafka.connect.source.RedisKeysSourceTask;
import com.redis.kafka.connect.source.RedisStreamSourceTask;
import com.redis.testcontainers.RedisContainer;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.utility.MountableFile;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RedisTlsIntegrationTests {
    private final RedisContainer tls = server(false, "server");
    private final RedisContainer mutualTls = server(true, "server");
    private final RedisContainer wrongHost = server(false, "wrong-host");

    private static RedisContainer server(boolean mutual, String certificate) {
        return RedisContainerFactory.redis()
                .withCopyFileToContainer(MountableFile.forClasspathResource("tls/ca.crt", 0644), "/tls/ca.crt")
                .withCopyFileToContainer(MountableFile.forClasspathResource("tls/" + certificate + ".crt", 0644), "/tls/server.crt")
                .withCopyFileToContainer(MountableFile.forClasspathResource("tls/" + certificate + ".key", 0644), "/tls/server.key")
                .withCommand("redis-server", "--port", "0", "--tls-port", "6379",
                        "--tls-cert-file", "/tls/server.crt", "--tls-key-file", "/tls/server.key",
                        "--tls-ca-cert-file", "/tls/ca.crt", "--tls-auth-clients", mutual ? "yes" : "no",
                        "--requirepass", "test-password", "--notify-keyspace-events", "KEA");
    }

    @BeforeAll
    void startServers() {
        tls.start();
        mutualTls.start();
        wrongHost.start();
    }

    @AfterAll
    void stopServers() {
        wrongHost.stop();
        mutualTls.stop();
        tls.stop();
    }

    private String resource(String name) throws Exception {
        return Path.of(getClass().getResource("/tls/" + name).toURI()).toString();
    }

    private Map<String, String> props(RedisContainer server, boolean mutual) throws Exception {
        Map<String, String> result = new HashMap<>();
        result.put("redis.uri", RedisURI.Builder.redis(server.getHost(), server.getRedisPort()).withSsl(true).build().toURI().toString());
        result.put("redis.username", "default");
        result.put("redis.password", "test-password");
        result.put("redis.cacert", resource("ca.crt"));
        result.put("redis.timeout", "2");
        if (mutual) {
            result.put("redis.key.file", resource("client.key"));
            result.put("redis.key.cert", resource("client.crt"));
        }
        return result;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void allTasksMoveDataOverTls(boolean mutual) throws Exception {
        Map<String, String> config = props(mutual ? mutualTls : tls, mutual);
        RedisSinkTask sink = new RedisSinkTask();
        RedisKeysSourceTask keys = new RedisKeysSourceTask();
        RedisStreamSourceTask stream = new RedisStreamSourceTask();
        try (AbstractRedisClient client = new RedisSinkConfig(config).client();
                RedisConnection<String, String> connection = RedisConnection.open(client, StringCodec.UTF8)) {
            RedisCommands<String, String> commands = connection.sync(RedisCommands.class);
            commands.flushall();
            Map<String, String> sinkConfig = new HashMap<>(config);
            sinkConfig.put("redis.type", "STRING");
            sinkConfig.put("redis.keyspace", "");
            sink.start(sinkConfig);
            sink.put(List.of(record("tls:key", "snapshot")));
            Map<String, String> keysConfig = new HashMap<>(config);
            keysConfig.put("redis.keys.pattern", "tls:key");
            keysConfig.put("redis.keys.timeout", "100");
            keys.start(keysConfig);
            awaitValue(keys, "snapshot");
            sink.put(List.of(record("tls:key", "updated")));
            awaitValue(keys, "updated");

            Map<String, String> streamConfig = new HashMap<>(config);
            streamConfig.put("redis.stream.name", "tls:stream");
            stream.start(streamConfig);
            String id = commands.xadd("tls:stream", Map.of("value", "secure"));
            List<SourceRecord> records = new ArrayList<>();
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                records.addAll(stream.poll());
                return !records.isEmpty();
            });
            assertEquals(id, records.get(0).key());
            assertEquals(Map.of("value", "secure"), ((Struct) records.get(0).value()).getMap("body"));
            stream.commitRecord(records.get(0), null);
            stream.commit();
            assertEquals(0, commands.xpending("tls:stream", "kafka-consumer-group").getCount());
        } finally {
            stream.stop();
            keys.stop();
            sink.stop();
        }
    }

    private SinkRecord record(String key, String value) {
        return new SinkRecord("secure", 0, Schema.STRING_SCHEMA, key, Schema.STRING_SCHEMA, value, 0);
    }

    private void awaitValue(RedisKeysSourceTask source, String value) {
        await().atMost(Duration.ofSeconds(5)).until(() -> source.poll().stream().anyMatch(record ->
                "tls:key".equals(record.key()) && value.equals(((Struct) record.value()).getString("string"))));
    }

    private void connect(Map<String, String> config) {
        RedisSinkTask sink = new RedisSinkTask();
        try {
            Map<String, String> sinkConfig = new HashMap<>(config);
            sinkConfig.put("redis.type", "STRING");
            sink.start(sinkConfig);
            sink.put(List.of(record("tls:connection", "ok")));
        } finally {
            sink.stop();
        }
    }

    @Test
    void rejectsAnUntrustedServerCertificate() throws Exception {
        Map<String, String> config = props(tls, false);
        config.remove("redis.cacert");
        assertThrows(ConnectException.class, () -> connect(config));
    }

    @Test
    void redissUriChecksHostnameUnlessExplicitlyInsecure() throws Exception {
        Map<String, String> config = props(wrongHost, false);
        assertThrows(ConnectException.class, () -> connect(config));
        config.put("redis.insecure", "true");
        connect(config);
    }

    @Test
    void rejectsMissingClientCertificateForMutualTls() throws Exception {
        assertThrows(ConnectException.class, () -> connect(props(mutualTls, false)));
    }

    @Test
    void rejectsWrongPassword() throws Exception {
        Map<String, String> config = props(tls, false);
        config.put("redis.password", "wrong-password");
        assertThrows(ConnectException.class, () -> connect(config));
    }

    @Test
    void acceptsEncryptedClientKey() throws Exception {
        Map<String, String> config = props(mutualTls, true);
        config.put("redis.key.file", resource("client-encrypted.key"));
        config.put("redis.key.password", "test-key-password");
        connect(config);
    }

    @Test
    void tlsFlagWithHostAndPortPreservesAuthentication() throws Exception {
        Map<String, String> config = props(tls, false);
        config.remove("redis.uri");
        config.put("redis.host", tls.getHost());
        config.put("redis.port", Integer.toString(tls.getRedisPort()));
        config.put("redis.tls", "true");
        connect(config);
    }

    @Test
    void acceptsCredentialsInTheTlsUri() throws Exception {
        Map<String, String> config = props(tls, false);
        config.put("redis.uri", "rediss://default:test-password@" + tls.getHost() + ":" + tls.getRedisPort());
        config.remove("redis.username");
        config.remove("redis.password");
        connect(config);
    }
}
