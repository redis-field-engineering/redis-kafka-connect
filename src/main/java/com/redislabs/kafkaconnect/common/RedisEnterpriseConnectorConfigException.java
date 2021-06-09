/**
 * Copyright © 2021 Redis Labs
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
package com.redislabs.kafkaconnect.common;

import lombok.Getter;
import org.apache.kafka.common.config.ConfigException;

public class RedisEnterpriseConnectorConfigException extends ConfigException {

    @Getter
    private final String name;
    @Getter
    private final Object value;
    @Getter
    private final String originalMessage;

    public RedisEnterpriseConnectorConfigException(String message) {
        this(message, null, null);
    }

    public RedisEnterpriseConnectorConfigException(String name, Object value) {
        this(name, value, null);
    }

    public RedisEnterpriseConnectorConfigException(String name, Object value, String message) {
        super(name, value, message);
        this.name = name;
        this.value = value;
        this.originalMessage = message;
    }


}
