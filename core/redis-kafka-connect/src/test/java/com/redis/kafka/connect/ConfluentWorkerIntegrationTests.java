package com.redis.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.KillArgs;
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
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.MountableFile;
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
        ImageFromDockerfile image = new ImageFromDockerfile()
                .withFileFromPath("Dockerfile", Path.of(System.getProperty("connector.worker.dockerfile")))
                .withBuildArg("CONFLUENT_VERSION", version);
        worker = new GenericContainer<>(image).withNetwork(network).withExposedPorts(8083)
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
                .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/opt/redis")
                .withEnv("CONNECT_PLUGIN_DISCOVERY", "service_load")
                .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
                .withCopyFileToContainer(MountableFile.forHostPath(System.getProperty("connector.plugin.jar")),
                        "/opt/redis/redis-kafka-connect.jar")
                .waitingFor(Wait.forHttp("/connector-plugins").forPort(8083))
                .withStartupTimeout(Duration.ofMinutes(3));
        worker.start();
        String java = worker.execInContainer("java", "-version").getStderr();
        assertTrue(java.contains("version \"21."), java);
        JsonNode plugins = request("GET", "/connector-plugins", null);
        for (String connector : List.of("RedisSinkConnector", "RedisStreamSourceConnector", "RedisKeysSourceConnector")) {
            assertTrue(plugins.toString().contains("com.redis.kafka.connect." + connector), plugins.toString());
            for (JsonNode plugin : plugins) {
                if (plugin.get("class").asText().equals("com.redis.kafka.connect." + connector)) {
                    assertNotEquals("N/A", plugin.get("version").asText());
                }
            }
        }
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
            config.put("tasks.max", "1");
            putConnector(name, config, 1);
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
        try {
            send(producer, name, "before");
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(() ->
                    "value".equals(redisConnection.sync().get(name + ":before")));
            awaitSinkOffset(name, 1);
            redisConnection.setTimeout(Duration.ofSeconds(1));
            redis.getDockerClient().restartContainerCmd(redis.getContainerId()).withTimeout(10).exec();
            send(producer, name, "after");
            Awaitility.await().atMost(Duration.ofMinutes(2)).ignoreExceptions().until(() ->
                    "PONG".equals(redisConnection.sync().ping()));
            Awaitility.await().atMost(Duration.ofSeconds(45)).ignoreExceptions().until(() ->
                    "value".equals(redisConnection.sync().get(name + ":after")));
            running(name, 1);
            awaitSinkOffset(name, 2);
        } finally {
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
