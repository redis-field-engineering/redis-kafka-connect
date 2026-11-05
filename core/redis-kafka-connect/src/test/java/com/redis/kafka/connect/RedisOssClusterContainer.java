package com.redis.kafka.connect;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.redis.testcontainers.RedisServer;
import java.time.Duration;
import java.io.IOException;
import org.awaitility.Awaitility;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

class RedisOssClusterContainer extends GenericContainer<RedisOssClusterContainer> implements RedisServer {
    RedisOssClusterContainer() {
        super(DockerImageName.parse(RedisContainerFactory.REDIS_IMAGE));
        for (int port = 17000; port <= 17003; port++) {
            addFixedExposedPort(port, port);
        }
        withCommand("sh", "-ec", """
                redis-server --loadmodule /usr/local/lib/redis/modules/rejson.so --loadmodule /usr/local/lib/redis/modules/redistimeseries.so --port 17000 --cluster-enabled yes --cluster-config-file /tmp/node0.conf --cluster-announce-ip 127.0.0.1 --protected-mode no &
                redis-server --loadmodule /usr/local/lib/redis/modules/rejson.so --loadmodule /usr/local/lib/redis/modules/redistimeseries.so --port 17001 --cluster-enabled yes --cluster-config-file /tmp/node1.conf --cluster-announce-ip 127.0.0.1 --protected-mode no &
                redis-server --loadmodule /usr/local/lib/redis/modules/rejson.so --loadmodule /usr/local/lib/redis/modules/redistimeseries.so --port 17003 --cluster-enabled yes --cluster-config-file /tmp/node3.conf --cluster-announce-ip 127.0.0.1 --protected-mode no &
                exec redis-server --loadmodule /usr/local/lib/redis/modules/rejson.so --loadmodule /usr/local/lib/redis/modules/redistimeseries.so --port 17002 --cluster-enabled yes --cluster-config-file /tmp/node2.conf --cluster-announce-ip 127.0.0.1 --protected-mode no
                """);
        waitingFor(Wait.forListeningPorts(17000, 17001, 17002, 17003));
        withStartupTimeout(Duration.ofMinutes(1));
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse info) {
        super.containerIsStarted(info);
        try {
            if (execInContainer("redis-cli", "--cluster", "create", "127.0.0.1:17000", "127.0.0.1:17001",
                    "127.0.0.1:17002", "--cluster-yes").getExitCode() != 0) {
                throw new ContainerLaunchException("Could not create OSS cluster");
            }
            Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
                for (int port = 17000; port <= 17002; port++) {
                    if (!execInContainer("redis-cli", "-p", Integer.toString(port), "cluster", "info")
                            .getStdout().contains("cluster_state:ok")) {
                        return false;
                    }
                }
                return true;
            });
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) { Thread.currentThread().interrupt(); }
            throw new ContainerLaunchException("Could not create OSS cluster", e);
        }
    }

    @Override public String getRedisHost() { return getHost(); }
    @Override public int getRedisPort() { return 17000; }
    @Override public boolean isRedisCluster() { return true; }
}
