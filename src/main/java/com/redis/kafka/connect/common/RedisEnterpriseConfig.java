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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import io.lettuce.core.RedisURI;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RedisEnterpriseConfig extends AbstractConfig {

    public static final String REDIS_URI = "redis.uri";
    private static final String REDIS_URI_DEFAULT = "redis://localhost:6379";
    private static final String REDIS_URI_DOC = "URI of the Redis Enterprise database to connect to, e.g. redis://redis-12000.redis.com:12000";

    public static final String INSECURE = "redis.insecure";
    public static final String INSECURE_DEFAULT = "false";
    public static final String INSECURE_DOC = "Allow insecure connections (e.g. invalid certificates) to Redis Enterprise when using SSL.";

    public RedisEnterpriseConfig(ConfigDef config, Map<?, ?> originals) {
        super(config, originals);
    }

    public RedisURI getRedisURI() {
        RedisURI uri = RedisURI.create(getString(REDIS_URI));
        uri.setVerifyPeer(!getBoolean(INSECURE));
        return uri;
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
            define(ConfigKeyBuilder.of(REDIS_URI, ConfigDef.Type.STRING).documentation(REDIS_URI_DOC).defaultValue(REDIS_URI_DEFAULT).importance(ConfigDef.Importance.HIGH).validator(Validators.validURI("redis", "rediss")).build());
            define(ConfigKeyBuilder.of(INSECURE, ConfigDef.Type.BOOLEAN).documentation(INSECURE_DOC).defaultValue(INSECURE_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
        }

    }

}
