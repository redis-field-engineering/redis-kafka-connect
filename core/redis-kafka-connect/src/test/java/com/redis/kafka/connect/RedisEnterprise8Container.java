package com.redis.kafka.connect;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.redis.enterprise.Admin;
import com.redis.enterprise.Command;
import com.redis.enterprise.Database;
import com.redis.enterprise.rest.CommandResponse;
import com.redis.enterprise.testcontainers.AbstractRedisEnterpriseContainer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.utility.DockerImageName;

/**
 * Uses data-port readiness checks because Redis Enterprise 8.2 no longer accepts
 * PING through the management API used by testcontainers-redis 3.0.0.
 */
class RedisEnterprise8Container extends AbstractRedisEnterpriseContainer<RedisEnterprise8Container> {

    private final Database database;

    RedisEnterprise8Container(DockerImageName image, Database database) {
        super(image);
        this.database = database;
        addFixedExposedPort(Admin.DEFAULT_PORT, Admin.DEFAULT_PORT);
        addFixedExposedPort(database.getPort(), database.getPort());
        withStartupTimeout(Duration.ofMinutes(2));
    }

    @Override
    protected String getAdminUserName() {
        return Admin.DEFAULT_USER_NAME;
    }

    @Override
    protected String getAdminPassword() {
        return Admin.DEFAULT_PASSWORD;
    }

    @Override
    protected void createCluster() {
        try (Admin admin = new Admin() {
            @Override
            public CommandResponse executeCommand(long uid, Command command)
                    throws IOException, GeneralSecurityException {
                if (!"PING".equals(command.getCommand())) {
                    return super.executeCommand(uid, command);
                }
                RedisURI uri = RedisURI.Builder.redis(getRedisHost(), getRedisPort())
                        .withTimeout(Duration.ofSeconds(2)).build();
                try (RedisClient client = RedisClient.create(uri);
                        StatefulRedisConnection<String, String> connection = client.connect()) {
                    CommandResponse response = new CommandResponse();
                    response.setResponse(BooleanNode.valueOf("PONG".equals(connection.sync().ping())));
                    return response;
                }
            }
        }) {
            admin.withHost(getHost()).withDatabaseCreationTimeout(Duration.ofMinutes(2));
            admin.waitForBoostrap();
            super.createCluster();
            Database actual = admin.createDatabase(database);
            if (!actual.isSharding() || actual.getShardCount() != 3 || actual.isOssCluster() != database.isOssCluster()) {
                throw new ContainerLaunchException("Enterprise fixture did not create the requested three-shard/API topology");
            }
        } catch (IOException | GeneralSecurityException e) {
            throw new ContainerLaunchException("Could not create Redis Enterprise database", e);
        }
    }

    @Override
    public boolean isRedisCluster() {
        return database.isOssCluster();
    }

    @Override
    public int getRedisPort() {
        return database.getPort();
    }
}
