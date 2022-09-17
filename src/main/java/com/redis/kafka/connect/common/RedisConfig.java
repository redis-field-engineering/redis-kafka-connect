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

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisURIBuilder;
import com.redis.spring.batch.common.ConnectionPoolBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisConfig extends AbstractConfig {

	public static final String CLUSTER_CONFIG = "redis.cluster";
	private static final boolean CLUSTER_DEFAULT = false;
	private static final String CLUSTER_DOC = "Connect to a Redis Cluster database";

	public static final String HOST_CONFIG = "redis.host";
	private static final HostAndPort HOST_DEFAULT = HostAndPort.fromParts("localhost", RedisURI.DEFAULT_REDIS_PORT);
	private static final String HOST_DOC = "The Redis host and port to connect to";

	public static final String URI_CONFIG = "redis.uri";
	private static final String URI_DEFAULT = "";
	private static final String URI_DOC = "URI of the Redis database to connect to, e.g. redis://redis-12000.redis.com:12000. For secure connections use rediss URI scheme, e.g. rediss://...";

	public static final String USERNAME_CONFIG = "redis.username";
	private static final String USERNAME_DEFAULT = "";
	private static final String USERNAME_DOC = "Username to use to connect to Redis";

	public static final String PASSWORD_CONFIG = "redis.password";
	private static final String PASSWORD_DEFAULT = "";
	private static final String PASSWORD_DOC = "Password to use to connect to Redis";

	public static final String TIMEOUT_CONFIG = "redis.timeout";
	private static final long TIMEOUT_DEFAULT = RedisURI.DEFAULT_TIMEOUT;
	private static final String TIMEOUT_DOC = "Redis command timeout in seconds";

	public static final String POOL_MAX_CONFIG = "redis.pool";
	private static final int POOL_MAX_DEFAULT = ConnectionPoolBuilder.DEFAULT_MAX_TOTAL;
	private static final String POOL_MAX_DOC = "Max pool connections";

	public static final String TLS_CONFIG = "redis.tls";
	private static final boolean TLS_DEFAULT = false;
	private static final String TLS_DOC = "Establish a secure TLS connection";

	public static final String INSECURE_CONFIG = "redis.insecure";
	private static final boolean INSECURE_DEFAULT = false;
	private static final String INSECURE_DOC = "Allow insecure connections (e.g. invalid certificates) to Redis when using SSL";

	public static final String KEY_CONFIG = "redis.key.file";
	public static final String KEY_DEFAULT = "";
	private static final String KEY_DOC = "PKCS#8 private key file to authenticate with (PEM format)";

	public static final String KEY_CERT_CONFIG = "redis.key.cert";
	public static final String KEY_CERT_DEFAULT = "";
	private static final String KEY_CERT_DOC = "X.509 certificate chain file to authenticate with (PEM format)";

	public static final String KEY_PASSWORD_CONFIG = "redis.key.password";
	private static final String KEY_PASSWORD_DEFAULT = "";
	private static final String KEY_PASSWORD_DOC = "Password of the private key file. Leave empty if key file is not password-protected";

	public static final String CACERT_CONFIG = "redis.cacert";
	public static final String CACERT_DEFAULT = "";
	private static final String CACERT_DOC = "X.509 CA certificate file to verify with";

	public RedisConfig(ConfigDef config, Map<?, ?> originals) {
		super(config, originals);
	}

	public static class RedisConfigDef extends ConfigDef {

		public RedisConfigDef() {
			defineConfigs();
		}

		public RedisConfigDef(ConfigDef base) {
			super(base);
			defineConfigs();
		}

		private void defineConfigs() {
			define(ConfigKeyBuilder.of(CLUSTER_CONFIG, ConfigDef.Type.BOOLEAN).documentation(CLUSTER_DOC)
					.defaultValue(CLUSTER_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(HOST_CONFIG, ConfigDef.Type.STRING).documentation(HOST_DOC)
					.defaultValue(HOST_DEFAULT.toString()).importance(ConfigDef.Importance.HIGH).build());
			define(ConfigKeyBuilder.of(URI_CONFIG, ConfigDef.Type.STRING).documentation(URI_DOC)
					.defaultValue(URI_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(TLS_CONFIG, ConfigDef.Type.BOOLEAN).documentation(TLS_DOC)
					.defaultValue(TLS_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(INSECURE_CONFIG, ConfigDef.Type.BOOLEAN).documentation(INSECURE_DOC)
					.defaultValue(INSECURE_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD).documentation(PASSWORD_DOC)
					.defaultValue(PASSWORD_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(USERNAME_CONFIG, ConfigDef.Type.STRING).documentation(USERNAME_DOC)
					.defaultValue(USERNAME_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(TIMEOUT_CONFIG, ConfigDef.Type.LONG).documentation(TIMEOUT_DOC)
					.defaultValue(TIMEOUT_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(POOL_MAX_CONFIG, ConfigDef.Type.INT).documentation(POOL_MAX_DOC)
					.defaultValue(POOL_MAX_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(KEY_CONFIG, ConfigDef.Type.STRING).documentation(KEY_DOC)
					.defaultValue(KEY_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(KEY_CERT_CONFIG, ConfigDef.Type.STRING).documentation(KEY_CERT_DOC)
					.defaultValue(KEY_CERT_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(KEY_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD).documentation(KEY_PASSWORD_DOC)
					.defaultValue(KEY_PASSWORD_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(CACERT_CONFIG, ConfigDef.Type.STRING).documentation(CACERT_DOC)
					.defaultValue(CACERT_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
		}

	}

	public RedisURI uri() {
		RedisURIBuilder builder = RedisURIBuilder.create();
		String uri = getString(URI_CONFIG);
		if (Strings.isNullOrEmpty(uri)) {
			HostAndPort hostAndPort = ConfigUtils.hostAndPort(this, HOST_CONFIG, 6379);
			builder.host(hostAndPort.getHost());
			builder.port(hostAndPort.getPort());
		} else {
			builder.uriString(uri);
		}
		if (Boolean.TRUE.equals(getBoolean(INSECURE_CONFIG))) {
			builder.sslVerifyMode(SslVerifyMode.NONE);
		}
		builder.ssl(getBoolean(TLS_CONFIG));
		String username = getString(USERNAME_CONFIG);
		if (!Strings.isNullOrEmpty(username)) {
			builder.username(username);
		}
		Password password = getPassword(PASSWORD_CONFIG);
		if (password != null && !Strings.isNullOrEmpty(password.value())) {
			builder.password(password.value().toCharArray());
		}
		Long timeout = getLong(TIMEOUT_CONFIG);
		if (timeout != null) {
			builder.timeout(Duration.ofSeconds(timeout));
		}
		return builder.build();
	}

	public AbstractRedisClient client(RedisURI uri) {
		ClientBuilder builder = ClientBuilder.create(uri);
		builder.cluster(getBoolean(CLUSTER_CONFIG));
		String keyFile = getString(KEY_CONFIG);
		if (!Strings.isNullOrEmpty(keyFile)) {
			builder.key(new File(keyFile));
			builder.keyCert(new File(getString(KEY_CERT_CONFIG)));
			Password password = getPassword(KEY_PASSWORD_CONFIG);
			if (password != null && !Strings.isNullOrEmpty(password.value())) {
				builder.keyPassword(password.value().toCharArray());
			}
		}
		String cacert = getString(CACERT_CONFIG);
		if (!Strings.isNullOrEmpty(cacert)) {
			builder.trustManager(new File(cacert));
		}
		return builder.build();
	}

	public AbstractRedisClient client() {
		return client(uri());
	}

	public GenericObjectPool<StatefulConnection<String, String>> pool(AbstractRedisClient client) {
		return pool(client, StringCodec.UTF8);
	}

	public <K, V> GenericObjectPool<StatefulConnection<K, V>> pool(AbstractRedisClient client, RedisCodec<K, V> codec) {
		return ConnectionPoolBuilder.create(client).maxTotal(getInt(POOL_MAX_CONFIG)).build(codec);

	}

}
