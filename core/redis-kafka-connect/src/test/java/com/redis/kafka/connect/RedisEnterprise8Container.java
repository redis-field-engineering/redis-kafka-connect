package com.redis.kafka.connect;

import com.redis.enterprise.Admin;
import com.redis.enterprise.Database;
import com.redis.enterprise.testcontainers.AbstractRedisEnterpriseContainer;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.utility.DockerImageName;

/**
 * Three-shard Redis Enterprise 8.2 fixture on fixed ports, verifying the created
 * database topology before tests run.
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
        try (Admin admin = new Admin()) {
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
