/*
 * Copyright © 2021 Redis
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redis.kafka.connect.common;

import java.io.File;
import java.time.Duration;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.springframework.util.StringUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslOptions.Builder;
import io.lettuce.core.cluster.ClusterClientOptions;

public abstract class RedisConfig extends AbstractConfig {

    private static final char[] EMPTY_PASSWORD = new char[0];

    protected RedisConfig(RedisConfigDef config, Map<?, ?> originals) {
        super(config, originals);
    }

    public RedisURI uri() {
        RedisURI.Builder builder = redisURIBuilder();
        Boolean ssl = getBoolean(RedisConfigDef.TLS_CONFIG);
        if (Boolean.TRUE.equals(ssl)) {
            builder.withSsl(ssl);
            Boolean insecure = getBoolean(RedisConfigDef.INSECURE_CONFIG);
            if (Boolean.TRUE.equals(insecure)) {
                builder.withVerifyPeer(false);
            }
        }
        Password password = getPassword(RedisConfigDef.PASSWORD_CONFIG);
        if (password != null) {
            String passwordString = password.value();
            if (StringUtils.hasLength(passwordString)) {
                String username = getString(RedisConfigDef.USERNAME_CONFIG);
                if (StringUtils.hasLength(username)) {
                    builder.withAuthentication(username, passwordString);
                } else {
                    builder.withPassword((CharSequence) passwordString);
                }
            }
        }
        Long timeout = getLong(RedisConfigDef.TIMEOUT_CONFIG);
        if (timeout != null) {
            builder.withTimeout(Duration.ofSeconds(timeout));
        }
        return builder.build();
    }

    private RedisURI.Builder redisURIBuilder() {
        String uri = getString(RedisConfigDef.URI_CONFIG);
        if (StringUtils.hasLength(uri)) {
            return RedisURI.builder(RedisURI.create(uri));
        }
        String host = getString(RedisConfigDef.HOST_CONFIG);
        int port = getInt(RedisConfigDef.PORT_CONFIG);
        return RedisURI.Builder.redis(host, port);
    }

    private AbstractRedisClient client(RedisURI uri) {
        Boolean cluster = getBoolean(RedisConfigDef.CLUSTER_CONFIG);
        if (Boolean.TRUE.equals(cluster)) {
            RedisModulesClusterClient client = RedisModulesClusterClient.create(uri);
            client.setOptions(clientOptions(ClusterClientOptions.builder()).build());
            return client;
        }
        RedisModulesClient client = RedisModulesClient.create(uri);
        client.setOptions(clientOptions(ClientOptions.builder()).build());
        return client;
    }

    private <B extends ClientOptions.Builder> B clientOptions(B builder) {
        builder.sslOptions(sslOptions());
        return builder;
    }

    private SslOptions sslOptions() {
        Builder options = SslOptions.builder();
        String key = getString(RedisConfigDef.KEY_CONFIG);
        if (StringUtils.hasLength(key)) {
            String cert = getString(RedisConfigDef.KEY_CERT_CONFIG);
            Password password = getPassword(RedisConfigDef.KEY_PASSWORD_CONFIG);
            char[] passwordCharArray = password == null ? EMPTY_PASSWORD : password.value().toCharArray();
            options.keyManager(new File(cert), new File(key), passwordCharArray);
        }
        String cacert = getString(RedisConfigDef.CACERT_CONFIG);
        if (StringUtils.hasLength(cacert)) {
            options.trustManager(new File(cacert));
        }
        return options.build();
    }

    public AbstractRedisClient client() {
        return client(uri());
    }

    public int getPoolSize() {
        return getInt(RedisConfigDef.POOL_MAX_CONFIG);
    }

}
