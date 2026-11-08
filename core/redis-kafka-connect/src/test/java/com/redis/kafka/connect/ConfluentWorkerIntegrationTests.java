package com.redis.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.Consumer;
import io.lettuce.core.KillArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import java.io.DataInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipFile;
import java.util.zip.ZipEntry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.Container.ExecResult;
import static org.junit.jupiter.api.Assertions.*;

/** Black-box qualification of the shaded distribution and real Connect callback/offset lifecycle. */
class ConfluentWorkerIntegrationTests extends AbstractRedisIntegrationTests {
    private final RedisEnterprise8Container redis = RedisContainerFactory.enterprise();
    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private Network network;
    private ConfluentKafkaContainer kafka;
    private GenericContainer<?> worker;

    @Override
    protected RedisServer getRedisServer() {
        return redis;
    }

    @BeforeAll
    void startWorker() throws Exception {
        String version = System.getProperty("confluent.version", "8.3.2");
        network = Network.newNetwork();
        Testcontainers.exposeHostPorts(redis.getRedisPort());
        kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:" + version)
                .withNetwork(network).withListener("broker:19092")
                .withStartupTimeout(Duration.ofMinutes(3));
        kafka.setPortBindings(List.of("19093:9092"));
        kafka.start();
        boolean nativeRuntime = Boolean.getBoolean("connector.worker.native");
        worker = createWorker(Path.of(System.getProperty("connector.plugin.archive")), "service_load");
        worker.start();
        // Check the executable of ConnectDistributed itself, rather than the shell's PATH.
        ExecResult runtime = worker.execInContainer("sh", "-ec", """
                for proc in /proc/[0-9]*; do
                    case "$(readlink "$proc/exe" 2>/dev/null || true)" in
                        */java)
                            if tr '\\000' ' ' < "$proc/cmdline" | grep -q 'org.apache.kafka.connect.cli.ConnectDistributed'; then
                                exec "$proc/exe" -version
                            fi
                            ;;
                    esac
                done
                exit 1
                """);
        assertEquals(0, runtime.getExitCode(), runtime.getStderr());
        String java = runtime.getStderr();
        int javaFeature = Runtime.Version.parse(java.split("\"")[1]).feature();
        assertTrue(javaFeature >= 17, java);
        if (!nativeRuntime) {
            assertEquals(21, javaFeature, java);
        }
        System.out.println("Confluent " + version + " Connect process runtime: " + java);
        String expectedVersion;
        try (JarFile jar = new JarFile(System.getProperty("connector.plugin.jar"))) {
            expectedVersion = jar.getManifest().getMainAttributes().getValue("Implementation-Version");
        }
        assertNotNull(expectedVersion);
        assertPluginVersion(expectedVersion);
    }

    private void assertPluginVersion(String expectedVersion) throws Exception {
        JsonNode plugins = request("GET", "/connector-plugins", null);
        for (String connector : List.of("RedisSinkConnector", "RedisStreamSourceConnector", "RedisKeysSourceConnector")) {
            assertTrue(plugins.toString().contains("com.redis.kafka.connect." + connector), plugins.toString());
            for (JsonNode plugin : plugins) {
                if (plugin.get("class").asText().equals("com.redis.kafka.connect." + connector)) {
                    assertEquals(expectedVersion, plugin.get("version").asText());
                }
            }
        }
    }

    private GenericContainer<?> createWorker(Path archive, String discovery) {
        String version = System.getProperty("confluent.version", "8.3.2");
        ImageFromDockerfile image = new ImageFromDockerfile()
                .withFileFromPath("Dockerfile", Path.of(System.getProperty("connector.worker.dockerfile")))
                .withBuildArg("CONFLUENT_VERSION", version);
        boolean nativeRuntime = Boolean.getBoolean("connector.worker.native");
        GenericContainer<?> created = nativeRuntime
                ? new GenericContainer<>(DockerImageName.parse("confluentinc/cp-kafka-connect:" + version))
                : new GenericContainer<>(image);
        created.withNetwork(network).withExposedPorts(8083)
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", "broker:19092")
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "worker")
                .withEnv("CONNECT_GROUP_ID", "redis-connector-tests")
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect-configs")
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets")
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect-status")
                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_FLUSH_INTERVAL_MS", "1000")
                .withEnv("CONNECT_CONSUMER_SESSION_TIMEOUT_MS", "6000")
                .withEnv("CONNECT_CONSUMER_HEARTBEAT_INTERVAL_MS", "1000")
                .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
                .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
                .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/tmp/hub-components")
                .withEnv("CONNECT_PLUGIN_DISCOVERY", discovery)
                .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
                .withCopyFileToContainer(MountableFile.forHostPath(archive), "/tmp/redis-connector.zip")
                .withCommand("sh", "-ec", """
                        mkdir -p /tmp/hub-components
                        echo 'plugin.path=/tmp/hub-components' > /tmp/hub-worker.properties
                        confluent-hub install --no-prompt --component-dir /tmp/hub-components \
                            --worker-configs /tmp/hub-worker.properties /tmp/redis-connector.zip
                        exec /etc/confluent/docker/run
                        """)
                .waitingFor(Wait.forHttp("/connector-plugins").forPort(8083))
                .withStartupTimeout(Duration.ofMinutes(3));
        return created;
    }

    @AfterAll
    void stopWorker() throws Exception {
        try {
            if (worker != null) {
                worker.stop();
            }
        } finally {
            try {
                if (kafka != null) {
                    kafka.stop();
                }
            } finally {
                if (network != null) {
                    network.close();
                }
            }
        }
    }

    @Test
    void packagedBaseClassesAreCompatibleWithJava17() throws Exception {
        try (JarFile jar = new JarFile(System.getProperty("connector.plugin.jar"))) {
            List<JarEntry> classes = jar.stream().filter(entry -> entry.getName().endsWith(".class")
                    && !entry.getName().startsWith("META-INF/versions/")).toList();
            assertFalse(classes.isEmpty(), "packaged connector must contain classes");
            for (JarEntry entry : classes) {
                try (DataInputStream bytecode = new DataInputStream(jar.getInputStream(entry))) {
                    assertEquals(0xCAFEBABE, bytecode.readInt(), entry.getName());
                    bytecode.readUnsignedShort(); // minor version
                    int major = bytecode.readUnsignedShort();
                    assertTrue(major <= 61, entry.getName() + " requires class version " + major + ", above Java 17");
                }
            }
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "connector.previous.archive", matches = ".+")
    void releasedArchiveUpgradeAndRollbackPreservesConnectorState() throws Exception {
        assumeFalse(Boolean.getBoolean("connector.worker.native"), "legacy upgrade is qualified on the customer's Java 21 runtime");
        String sinkName = "worker-upgrade-sink";
        String streamName = "worker-upgrade-stream";
        String keysName = "worker-upgrade-keys";
        for (String topic : List.of(sinkName, streamName, keysName)) {
            createTopic(topic);
        }
        enableKeyspaceNotifications();
        redisConnection.sync().set("upgrade:watched", "seed");
        Map<String, String> sinkConfig = base("RedisSinkConnector");
        sinkConfig.put("topics", sinkName);
        sinkConfig.put("redis.type", "LIST");
        sinkConfig.put("redis.keyspace", sinkName);
        sinkConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        Map<String, String> streamConfig = base("RedisStreamSourceConnector");
        streamConfig.put("topic", streamName);
        streamConfig.put("redis.stream.name", streamName);
        Map<String, String> keysConfig = base("RedisKeysSourceConnector");
        keysConfig.put("topic", keysName);
        keysConfig.put("redis.keys.pattern", "upgrade:*");
        // The legacy reader waits for a full batch when idle timeout is disabled.
        keysConfig.put("batch.size", "1");
        Path candidateArchive = Path.of(System.getProperty("connector.plugin.archive"));
        Path previousArchive = Path.of(System.getProperty("connector.previous.archive"));
        String previousVersion = archiveVersion(previousArchive);
        String candidateVersion = archiveVersion(candidateArchive);
        assertNotEquals(previousVersion, candidateVersion, "upgrade must use two distinct connector versions");
        Throwable primaryFailure = null;
        try (KafkaProducer<String, String> producer = producer();
                KafkaConsumer<String, String> streamConsumer = consumer(streamName);
                KafkaConsumer<String, String> keysConsumer = consumer(keysName)) {
            switchWorker(previousArchive, "hybrid_warn");
            assertPluginVersion(previousVersion);
            putConnector(sinkName, sinkConfig, 1);
            putConnector(streamName, streamConfig, 1);
            putConnector(keysName, keysConfig, 1);
            migrationPhase(producer, streamConsumer, keysConsumer, "before", List.of("before"), true);
            switchWorker(candidateArchive, "service_load");
            assertPluginVersion(candidateVersion);
            for (String name : List.of(sinkName, streamName, keysName)) {
                running(name, 1);
            }
            migrationPhase(producer, streamConsumer, keysConsumer, "after", List.of("before", "after"), false);
            // Drain acknowledged source work before rollback, as required by the migration guide.
            switchWorker(previousArchive, "hybrid_warn");
            assertPluginVersion(previousVersion);
            for (String name : List.of(sinkName, streamName, keysName)) {
                running(name, 1);
            }
            migrationPhase(producer, streamConsumer, keysConsumer, "rollback", List.of("before", "after", "rollback"), true);
        } catch (Exception | AssertionError failure) {
            primaryFailure = failure;
            if (worker.isRunning()) {
                System.err.println(worker.getLogs());
            }
            throw failure;
        } finally {
            try {
                switchWorker(candidateArchive, "service_load");
                JsonNode connectors = request("GET", "/connectors", null);
                for (JsonNode name : connectors) {
                    if (List.of(sinkName, streamName, keysName).contains(name.asText())) {
                        request("DELETE", "/connectors/" + name.asText(), null);
                    }
                }
            } catch (Exception | AssertionError cleanupFailure) {
                if (primaryFailure == null) {
                    throw cleanupFailure;
                }
                primaryFailure.addSuppressed(cleanupFailure);
            }
        }
    }

    private void switchWorker(Path archive, String discovery) {
        worker.stop();
        // Use a fresh fixture so Docker allocates new host ports for the next process.
        worker = createWorker(archive, discovery);
        worker.start();
    }

    private String archiveVersion(Path archive) throws Exception {
        try (ZipFile zip = new ZipFile(archive.toFile())) {
            ZipEntry manifest = zip.stream().filter(entry -> entry.getName().endsWith("/manifest.json")).findFirst().orElseThrow();
            try (DataInputStream input = new DataInputStream(zip.getInputStream(manifest))) {
                return json.readTree(input).get("version").asText();
            }
        }
    }

    private void migrationPhase(KafkaProducer<String, String> producer, KafkaConsumer<String, String> streamConsumer,
            KafkaConsumer<String, String> keysConsumer, String phase, List<String> expected, boolean legacy) throws Exception {
        send(producer, "worker-upgrade-sink", phase);
        awaitList("worker-upgrade-sink", expected);
        String offsetKey = legacy ? "com.redis.kafka.connect.sink.offset.worker-upgrade-sink.0"
                : "com.redis.kafka.connect.sink.offset.worker-upgrade-sink.worker-upgrade-sink.0";
        awaitUntil(() -> {
            String offset = redisConnection.sync().get(offsetKey);
            return offset != null && json.readTree(offset).get("offset").asLong() == expected.size();
        });
        String id = redisConnection.sync().xadd("worker-upgrade-stream", Map.of("value", phase));
        awaitMigrationRecord(streamConsumer, id, phase, true);
        awaitUntil(() -> redisConnection.sync().xpending("worker-upgrade-stream", "kafka-consumer-group").getCount() == 0);
        redisConnection.sync().set("upgrade:watched", phase);
        awaitMigrationRecord(keysConsumer, "upgrade:watched", phase, false);
    }

    private void awaitMigrationRecord(KafkaConsumer<String, String> consumer, String key, String phase, boolean stream) {
        Awaitility.await().atMost(Duration.ofSeconds(40)).until(() -> {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                if (key.equals(record.key())) {
                    JsonNode value = json.readTree(record.value());
                    String actual = stream ? value.get("body").get("value").asText() : value.get("string").asText();
                    if (phase.equals(actual)) {
                        return true;
                    }
                }
            }
            return false;
        });
    }

    @Test
    void sinkSurvivesTaskAndBrokerRestarts() throws Exception {
        String name = "worker-sink";
        createTopic(name);
        Map<String, String> config = base("RedisSinkConnector");
        config.put("topics", name);
        config.put("redis.type", "LIST");
        config.put("redis.keyspace", name);
        config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        putConnector(name, config, 1);
        KafkaProducer<String, String> producer = producer();
        try {
            send(producer, name, "first");
            awaitList(name, List.of("first"));
            awaitSinkOffset(name, 1);
            request("POST", "/connectors/" + name + "/restart?includeTasks=true&onlyFailed=false", null);
            running(name, 1);
            send(producer, name, "second");
            awaitList(name, List.of("first", "second"));
            awaitSinkOffset(name, 2);
            kafka.getDockerClient().restartContainerCmd(kafka.getContainerId()).withTimeout(10).exec();
            Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers(),
                    "default.api.timeout.ms", "10000", "request.timeout.ms", "5000"));
            try {
                Awaitility.await().atMost(Duration.ofSeconds(60)).ignoreExceptions().until(() ->
                        !admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).isEmpty());
            } finally {
                admin.close(Duration.ofSeconds(5));
            }
            send(producer, name, "third");
            awaitList(name, List.of("first", "second", "third"));
            awaitSinkOffset(name, 3);
            assertTrue(worker.getLogs().contains("Wrote 1 records"), "Connector logs must use the worker's SLF4J provider");
        } finally {
            producer.close(Duration.ofSeconds(5));
            request("DELETE", "/connectors/" + name, null);
        }
    }

    @Test
    void streamCommitsAndContinuesAfterTaskScaleDown() throws Exception {
        String name = "worker-stream";
        createTopic(name);
        Map<String, String> config = base("RedisStreamSourceConnector");
        config.put("topic", name);
        config.put("redis.stream.name", name);
        config.put("tasks.max", "2");
        config.put("redis.stream.claim.idle", "1000");
        List<String> ids = new ArrayList<>();
        for (int index = 0; index < 6; index++) {
            ids.add(redisConnection.sync().xadd(name, Map.of("value", Integer.toString(index))));
        }
        try (KafkaConsumer<String, String> consumer = consumer(name)) {
            putConnector(name, config, 2);
            List<ConsumerRecord<String, String>> records = receive(consumer, 6);
            assertTrue(records.stream().map(ConsumerRecord::key).toList().containsAll(ids));
            awaitUntil(() -> redisConnection.sync().xpending(name, "kafka-consumer-group").getCount() == 0);
            // Connect defers task reconfiguration while paused. STOPPED removes both
            // tasks before resuming with one, so the removed consumer cannot recover its own PEL.
            request("PUT", "/connectors/" + name + "/stop", null);
            Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
                JsonNode status = request("GET", "/connectors/" + name + "/status", null);
                return "STOPPED".equals(status.get("connector").get("state").asText())
                        && status.get("tasks").isEmpty();
            });
            // Leave work owned by the consumer that tasks.max=1 will remove. No worker
            // delivery callback exists for this entry, so an offset flush cannot acknowledge it.
            String abandoned = redisConnection.sync().xadd(name, Map.of("value", "abandoned"));
            assertEquals(List.of(abandoned), redisConnection.sync().xreadgroup(
                    Consumer.from("kafka-consumer-group", "consumer-1"), XReadArgs.Builder.count(1),
                    StreamOffset.lastConsumed(name)).stream().map(message -> message.getId()).toList());
            assertEquals(1, redisConnection.sync().xpending(name, "kafka-consumer-group").getCount());
            config.put("tasks.max", "1");
            request("PUT", "/connectors/" + name + "/config", config);
            Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
                JsonNode status = request("GET", "/connectors/" + name + "/status", null);
                return "STOPPED".equals(status.get("connector").get("state").asText())
                        && status.get("tasks").isEmpty()
                        && "1".equals(request("GET", "/connectors/" + name + "/config", null).get("tasks.max").asText());
            });
            request("PUT", "/connectors/" + name + "/resume", null);
            running(name, 1);
            List<ConsumerRecord<String, String>> reclaimed = new ArrayList<>();
            Awaitility.await().atMost(Duration.ofSeconds(40)).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(reclaimed::add);
                for (ConsumerRecord<String, String> record : reclaimed) {
                    if (abandoned.equals(record.key())) {
                        assertEquals("abandoned", json.readTree(record.value()).get("body").get("value").asText());
                        return true;
                    }
                }
                return false;
            });
            awaitUntil(() -> redisConnection.sync().xpending(name, "kafka-consumer-group").getCount() == 0);
            String after = redisConnection.sync().xadd(name, Map.of("value", "after"));
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (after.equals(record.key())) {
                        JsonNode value = json.readTree(record.value());
                        assertEquals(name, value.get("stream").asText());
                        assertEquals("after", value.get("body").get("value").asText());
                        return true;
                    }
                }
                return false;
            });
            awaitUntil(() -> redisConnection.sync().xpending(name, "kafka-consumer-group").getCount() == 0);
        } finally {
            request("DELETE", "/connectors/" + name, null);
        }
    }

    @Test
    void keysRunAsOneTaskAndReportNotificationGaps() throws Exception {
        String name = "worker-keys";
        createTopic(name);
        enableKeyspaceNotifications();
        redisConnection.sync().set("watched:snapshot", "before");
        Map<String, String> config = base("RedisKeysSourceConnector");
        config.put("topic", name);
        config.put("redis.keys.pattern", "watched:*");
        config.put("tasks.max", "3");
        try (KafkaConsumer<String, String> consumer = consumer(name)) {
            putConnector(name, config, 1);
            ConsumerRecord<String, String> snapshot = receive(consumer, 1).get(0);
            assertEquals("watched:snapshot", snapshot.key());
            redisConnection.sync().set("watched:live", "after");
            ConsumerRecord<String, String> live = receive(consumer, 1).get(0);
            assertEquals("watched:live", live.key());
            assertEquals("after", json.readTree(live.value()).get("string").asText());
            redisConnection.sync().clientKill(KillArgs.Builder.typePubsub());
            Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
                JsonNode status = request("GET", "/connectors/" + name + "/status", null);
                return "FAILED".equals(status.get("tasks").get(0).get("state").asText());
            });
            request("POST", "/connectors/" + name + "/restart?includeTasks=true&onlyFailed=true", null);
            running(name, 1);
            List<ConsumerRecord<String, String>> replay = receive(consumer, 2);
            assertTrue(replay.stream().map(ConsumerRecord::key).toList().containsAll(List.of("watched:snapshot", "watched:live")));
        } finally {
            request("DELETE", "/connectors/" + name, null);
        }
    }

    @Test
    void sinkRetriesAfterRedisServerRestart() throws Exception {
        String name = "worker-redis-restart";
        createTopic(name);
        Map<String, String> config = base("RedisSinkConnector");
        config.put("topics", name);
        config.put("redis.type", "STRING");
        config.put("redis.keyspace", name);
        config.put("redis.timeout", "2");
        config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        putConnector(name, config, 1);
        KafkaProducer<String, String> producer = producer();
        boolean redisStopped = false;
        try {
            send(producer, name, "before");
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(() ->
                    "value".equals(redisConnection.sync().get(name + ":before")));
            awaitSinkOffset(name, 1);
            redisConnection.setTimeout(Duration.ofSeconds(1));
            redis.getDockerClient().stopContainerCmd(redis.getContainerId()).withTimeout(10).exec();
            redisStopped = true;
            int logStart = worker.getLogs().length();
            send(producer, name, "after");
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> {
                String logs = worker.getLogs().substring(logStart);
                return logs.contains("Could not get connection to Redis")
                        || logs.contains("Timeout while writing sink records")
                        || logs.contains("Could not write sink records");
            });
            redis.getDockerClient().startContainerCmd(redis.getContainerId()).exec();
            redisStopped = false;
            Awaitility.await().atMost(Duration.ofMinutes(2)).ignoreExceptions().until(() ->
                    "PONG".equals(redisConnection.sync().ping()));
            Awaitility.await().atMost(Duration.ofSeconds(45)).ignoreExceptions().until(() ->
                    "value".equals(redisConnection.sync().get(name + ":after")));
            running(name, 1);
            awaitSinkOffset(name, 2);
        } finally {
            if (redisStopped) {
                redis.getDockerClient().startContainerCmd(redis.getContainerId()).exec();
            }
            producer.close(Duration.ofSeconds(5));
            request("DELETE", "/connectors/" + name, null);
            redisConnection.setTimeout(Duration.ofSeconds(60));
        }
    }

    private Map<String, String> base(String connector) {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "com.redis.kafka.connect." + connector);
        config.put("tasks.max", "1");
        config.put("redis.uri", "redis://host.testcontainers.internal:" + redis.getRedisPort());
        return config;
    }

    private void createTopic(String topic) throws Exception {
        Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers(), "default.api.timeout.ms", "20000", "request.timeout.ms", "10000"));
        try {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
        } finally {
            admin.close(Duration.ofSeconds(5));
        }
    }

    private void putConnector(String name, Map<String, String> config, int tasks) throws Exception {
        request("PUT", "/connectors/" + name + "/config", config);
        running(name, tasks);
    }

    private void running(String name, int taskCount) {
        Awaitility.await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            JsonNode status = request("GET", "/connectors/" + name + "/status", null);
            assertEquals("RUNNING", status.get("connector").get("state").asText(), status.toString());
            assertEquals(taskCount, status.get("tasks").size(), status.toString());
            status.get("tasks").forEach(task -> assertEquals("RUNNING", task.get("state").asText(), status.toString()));
        });
    }

    private JsonNode request(String method, String path, Object body) throws Exception {
        HttpRequest.BodyPublisher publisher = body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body));
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + worker.getHost() + ":" + worker.getMappedPort(8083) + path))
                .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json").method(method, publisher).build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.statusCode() >= 200 && response.statusCode() < 300, response.statusCode() + ": " + response.body());
        return response.body().isBlank() ? json.nullNode() : json.readTree(response.body());
    }

    private KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000));
    }

    private void send(KafkaProducer<String, String> producer, String topic, String key) throws Exception {
        producer.send(new ProducerRecord<>(topic, key, "value")).get(35, TimeUnit.SECONDS);
    }

    private KafkaConsumer<String, String> consumer(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, topic + "-validation",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private List<ConsumerRecord<String, String>> receive(KafkaConsumer<String, String> consumer, int count) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        Awaitility.await().atMost(Duration.ofSeconds(40)).until(() -> {
            consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            return records.size() >= count;
        });
        return records;
    }

    private void awaitSinkOffset(String name, long offset) {
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> {
            String stored = redisConnection.sync().get("com.redis.kafka.connect.sink.offset." + name + "." + name + ".0");
            return stored != null && json.readTree(stored).get("offset").asLong() == offset;
        });
    }

    private void awaitList(String key, List<String> expected) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertEquals(expected, redisConnection.sync().lrange(key, 0, -1)));
    }
}
