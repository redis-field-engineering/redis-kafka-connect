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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;

import io.lettuce.core.RedisURI;

public class RedisEnterpriseConfig extends AbstractConfig {

	public static final String REDIS_URI_CONFIG = "redis.uri";
	private static final String REDIS_URI_DEFAULT = "redis://localhost:6379";
	private static final String REDIS_URI_DOC = "URI of the Redis Enterprise database to connect to, e.g. redis://redis-12000.redis.com:12000. For secure connections use rediss URI scheme, e.g. rediss://...";

	public static final String INSECURE_CONFIG = "redis.insecure";
	public static final String INSECURE_DEFAULT = "false";
	public static final String INSECURE_DOC = "Allow insecure connections (e.g. invalid certificates) to Redis Enterprise when using SSL.";

	public static final String REDIS_USERNAME_CONFIG = "redis.username";
	private static final String REDIS_USERNAME_DEFAULT = null;
	private static final String REDIS_USERNAME_DOC = "Username to use to connect to Redis Enterprise database";

	public static final String REDIS_PASSWORD_CONFIG = "redis.password";
	private static final String REDIS_PASSWORD_DEFAULT = null;
	private static final String REDIS_PASSWORD_DOC = "Password to use to connect to Redis Enterprise database";

	public RedisEnterpriseConfig(ConfigDef config, Map<?, ?> originals) {
		super(config, originals);
	}

	public RedisURI getRedisURI() {
		RedisURI uri = RedisURI.create(ConfigUtils.uri(this, REDIS_URI_CONFIG));
		uri.setVerifyPeer(!getBoolean(INSECURE_CONFIG));
		String username = getString(REDIS_URI_CONFIG);
		if (!isEmpty(username)) {
			uri.setUsername(username);
		}
		if (get(REDIS_PASSWORD_CONFIG) != null) {
			char[] password = ConfigUtils.passwordCharArray(this, REDIS_PASSWORD_CONFIG);
			if (password.length > 0) {
				uri.setPassword(password);
			}
		}
		return uri;
	}

	protected static boolean isEmpty(String string) {
		return string == null || string.isEmpty();
	}

	protected static class RedisEnterpriseConfigDef extends ConfigDef {

		protected RedisEnterpriseConfigDef() {
			defineConfigs();
		}

		protected RedisEnterpriseConfigDef(ConfigDef base) {
			super(base);
			defineConfigs();
		}

		private void defineConfigs() {
			define(ConfigKeyBuilder.of(REDIS_URI_CONFIG, ConfigDef.Type.STRING).documentation(REDIS_URI_DOC)
					.defaultValue(REDIS_URI_DEFAULT).importance(ConfigDef.Importance.HIGH)
					.validator(Validators.validURI("redis", "rediss")).build());
			define(ConfigKeyBuilder.of(REDIS_USERNAME_CONFIG, ConfigDef.Type.STRING).documentation(REDIS_USERNAME_DOC)
					.defaultValue(REDIS_USERNAME_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(REDIS_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD).documentation(REDIS_PASSWORD_DOC)
					.defaultValue(REDIS_PASSWORD_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(INSECURE_CONFIG, ConfigDef.Type.BOOLEAN).documentation(INSECURE_DOC)
					.defaultValue(INSECURE_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
		}

	}

}
