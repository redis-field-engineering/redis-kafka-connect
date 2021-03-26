/**
 * Copyright © 2021 Redis Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redislabs.kafkaconnect.common;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Optional;

@Slf4j
public final class ConnectionValidator {

  public static Optional<StatefulRedisConnection<String, String>> validateCanConnect(final Config config, final String redisUriConfigName) {
    Optional<ConfigValue> optionalRedisUriString = getConfigByName(config, redisUriConfigName);
    if (optionalRedisUriString.isPresent() && optionalRedisUriString.get().errorMessages().isEmpty()) {
      ConfigValue configValue = optionalRedisUriString.get();
      String redisUri = (String) configValue.value();
      RedisClient redisClient = RedisClient.create(redisUri);
      try {
        return Optional.of(redisClient.connect());
      } catch (Exception e) {
        configValue.addErrorMessage("Unable to connect to Redis Enterprise.");
        throw new ConnectException(e);
      }
    }
    return Optional.empty();
  }

  public static Optional<ConfigValue> getConfigByName(final Config config, final String name) {
    for (final ConfigValue configValue : config.configValues()) {
      if (configValue.name().equals(name)) {
        return Optional.of(configValue);
      }
    }
    return Optional.empty();
  }

  private ConnectionValidator() {
  }
}
