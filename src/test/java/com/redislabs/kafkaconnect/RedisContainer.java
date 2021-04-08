package com.redislabs.kafkaconnect;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class RedisContainer extends GenericContainer<RedisContainer> {


    public RedisContainer() {
        super(DockerImageName.parse("redis:latest"));
        withExposedPorts(6379);
        waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));
    }

    public String getUri() {
        return "redis://" + this.getHost() + ":" + this.getFirstMappedPort();
    }
}
