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

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class RedisConfig extends AbstractConfig {

	private static final Logger log = LoggerFactory.getLogger(RedisConfig.class);

	public static final String CLIENT_MODE_CONFIG = "redis.client.mode";
	private static final ClientMode CLIENT_MODE_DEFAULT = ClientMode.standalone;
	private static final String CLIENT_MODE_DOC = "Redis standalone or cluster mode";

	public static final String HOST_CONFIG = "redis.host";
	private static final HostAndPort HOST_DEFAULT = HostAndPort.fromParts("localhost", RedisURI.DEFAULT_REDIS_PORT);
	private static final String HOST_DOC = "The Redis host and port to connect to";

	public static final String URI_CONFIG = "redis.uri";
	private static final String URI_DEFAULT = "";
	private static final String URI_DOC = "URI of the Redis database to connect to, e.g. redis://redis-12000.redis.com:12000. For secure connections use rediss URI scheme, e.g. rediss://...";

	public static final String INSECURE_CONFIG = "redis.insecure";
	private static final boolean INSECURE_DEFAULT = false;
	private static final String INSECURE_DOC = "Allow insecure connections (e.g. invalid certificates) to Redis when using SSL";

	public static final String TLS_CONFIG = "redis.tls";
	private static final boolean TLS_DEFAULT = false;
	private static final String TLS_DOC = "Establish a secure TLS connection";

	public static final String USERNAME_CONFIG = "redis.username";
	private static final String USERNAME_DEFAULT = "";
	private static final String USERNAME_DOC = "Username to use to connect to Redis";

	public static final String PASSWORD_CONFIG = "redis.password";
	private static final String PASSWORD_DEFAULT = "";
	private static final String PASSWORD_DOC = "Password to use to connect to Redis";

	public static final String TIMEOUT_CONFIG = "redis.timeout";
	private static final long TIMEOUT_DEFAULT = RedisURI.DEFAULT_TIMEOUT;
	private static final String TIMEOUT_DOC = "Redis command timeout in seconds";

	public RedisConfig(ConfigDef config, Map<?, ?> originals) {
		super(config, originals);
	}

	public RedisURI getRedisURI() {
		RedisURI uri = uri();
		if (Boolean.TRUE.equals(getBoolean(INSECURE_CONFIG))) {
			uri.setVerifyPeer(false);
		}
		if (Boolean.TRUE.equals(getBoolean(TLS_CONFIG))) {
			uri.setSsl(true);
		}
		String username = getString(USERNAME_CONFIG);
		if (!Strings.isNullOrEmpty(username)) {
			uri.setUsername(username);
		}
		Password password = getPassword(PASSWORD_CONFIG);
		if (password != null) {
			String passwordValue = password.value();
			if (!Strings.isNullOrEmpty(passwordValue)) {
				uri.setPassword(passwordValue.toCharArray());
			}
		}
		Long timeout = getLong(TIMEOUT_CONFIG);
		if (timeout != null) {
			uri.setTimeout(Duration.ofSeconds(timeout));
		}
		return uri;
	}

	private RedisURI uri() {
		String uriString = getString(URI_CONFIG);
		if (Strings.isNullOrEmpty(uriString)) {
			HostAndPort hostAndPort = ConfigUtils.hostAndPort(this, HOST_CONFIG, 6379);
			return RedisURI.create(hostAndPort.getHost(), hostAndPort.getPort());
		}
		return RedisURI.create(ConfigUtils.uri(this, URI_CONFIG));
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
			define(ConfigKeyBuilder.of(CLIENT_MODE_CONFIG, ConfigDef.Type.STRING).documentation(CLIENT_MODE_DOC)
					.defaultValue(CLIENT_MODE_DEFAULT.name()).validator(Validators.validEnum(ClientMode.class))
					.importance(ConfigDef.Importance.MEDIUM).build());
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

		}

	}

	public enum ClientMode {

		standalone, cluster

	}

	public AbstractRedisClient redisClient() {
		RedisURI uri = getRedisURI();
		ClientMode clientMode = ConfigUtils.getEnum(ClientMode.class, this, CLIENT_MODE_CONFIG);
		if (clientMode == ClientMode.cluster) {
			log.info("Connecting to Redis cluster with {}", uri);
			return RedisModulesClusterClient.create(uri);
		}
		return RedisModulesClient.create(uri);
	}

}
