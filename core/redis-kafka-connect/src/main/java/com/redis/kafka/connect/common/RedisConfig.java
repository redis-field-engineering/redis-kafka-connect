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

import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisURIBuilder;
import com.redis.spring.batch.common.PoolOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.netty.util.internal.StringUtil;

public abstract class RedisConfig extends AbstractConfig {

	protected RedisConfig(RedisConfigDef config, Map<?, ?> originals) {
		super(config, originals);
	}

	public RedisURI uri() {
		RedisURIBuilder builder = RedisURIBuilder.create();
		String uri = getString(RedisConfigDef.URI_CONFIG);
		if (StringUtil.isNullOrEmpty(uri)) {
			builder.host(getString(RedisConfigDef.HOST_CONFIG));
			builder.port(getInt(RedisConfigDef.PORT_CONFIG));
		} else {
			builder.uri(uri);
		}
		if (Boolean.TRUE.equals(getBoolean(RedisConfigDef.INSECURE_CONFIG))) {
			builder.sslVerifyMode(SslVerifyMode.NONE);
		}
		builder.ssl(getBoolean(RedisConfigDef.TLS_CONFIG));
		String username = getString(RedisConfigDef.USERNAME_CONFIG);
		if (!StringUtil.isNullOrEmpty(username)) {
			builder.username(username);
		}
		Password password = getPassword(RedisConfigDef.PASSWORD_CONFIG);
		if (password != null && !StringUtil.isNullOrEmpty(password.value())) {
			builder.password(password.value().toCharArray());
		}
		Long timeout = getLong(RedisConfigDef.TIMEOUT_CONFIG);
		if (timeout != null) {
			builder.timeout(Duration.ofSeconds(timeout));
		}
		return builder.build();
	}

	private AbstractRedisClient client(RedisURI uri) {
		ClientBuilder builder = ClientBuilder.create(uri);
		builder.cluster(getBoolean(RedisConfigDef.CLUSTER_CONFIG));
		String keyFile = getString(RedisConfigDef.KEY_CONFIG);
		if (!StringUtil.isNullOrEmpty(keyFile)) {
			builder.key(new File(keyFile));
			builder.keyCert(new File(getString(RedisConfigDef.KEY_CERT_CONFIG)));
			Password password = getPassword(RedisConfigDef.KEY_PASSWORD_CONFIG);
			if (password != null && !StringUtil.isNullOrEmpty(password.value())) {
				builder.keyPassword(password.value().toCharArray());
			}
		}
		String cacert = getString(RedisConfigDef.CACERT_CONFIG);
		if (!StringUtil.isNullOrEmpty(cacert)) {
			builder.trustManager(new File(cacert));
		}
		return builder.build();
	}

	public AbstractRedisClient client() {
		return client(uri());
	}

	public PoolOptions poolOptions() {
		return PoolOptions.builder().maxTotal(getInt(RedisConfigDef.POOL_MAX_CONFIG)).build();
	}

}
