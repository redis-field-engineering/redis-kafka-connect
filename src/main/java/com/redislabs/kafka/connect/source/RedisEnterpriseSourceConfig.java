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
package com.redislabs.kafka.connect.source;

import com.redislabs.kafka.connect.RedisEnterpriseConfigException;
import io.netty.util.internal.StringUtil;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import java.util.Collections;
import java.util.Map;

public class RedisEnterpriseSourceConfig extends AbstractConfig {

  public static final ConfigDef CONFIG_DEF = new RedisEnterpriseSourceConfigDef();

  public static final String REDIS_URI_CONFIG = "redis.uri";
  private static final String REDIS_URI_DEFAULT = "redis://localhost:6379";
  public static final String REDIS_URI_DISPLAY = "Connection URI";
  private static final String REDIS_URI_DOC = "URI of the Redis Enterprise database to connect to, e.g. redis://redis-12000.redislabs.com:12000";

  public static final String STREAM_CONFIG = "redis.stream";
  private static final String STREAM_DISPLAY = "Stream name";
  private static final String STREAM_DOC = "Name of the Redis stream to read from";

  public static final String OFFSET_CONFIG = "redis.stream.offset";
  private static final String OFFSET_DEFAULT = "0-0";
  private static final String OFFSET_DISPLAY = "Stream offset";
  private static final String OFFSET_DOC = "Stream offset to start reading from";

  public static final String COUNT_CONFIG = "redis.stream.count";
  private static final int COUNT_DEFAULT = 50;
  private static final String COUNT_DISPLAY = "Message count";
  private static final String COUNT_DOC = "Number of messages to read from the stream: XREAD [COUNT count]";

  public static final String POLL_BATCH_SIZE_CONFIG = "poll.batch.size";
  private static final int POLL_BATCH_SIZE_DEFAULT = 50;
  private static final String POLL_BATCH_SIZE_DISPLAY = "The maximum batch size";
  private static final String POLL_BATCH_SIZE_DOC = "Maximum number of stream messages to include in a single batch when polling for new data. This setting can be used to limit the amount of data buffered internally in the connector.";

  public static final String POLL_INTERVAL_CONFIG = "poll.interval";
  private static final int POLL_INTERVAL_DEFAULT = 5000;
  private static final String POLL_INTERVAL_DISPLAY = "Max poll duration";
  private static final String POLL_INTERVAL_DOC = "The max amount of time in milliseconds to wait while polling for stream messages";

  public static final String TOPIC_CONFIG = "topic";
  private static final String TOPIC_DISPLAY = "Topic name";
  private static final String TOPIC_DOC = "Name of the topic to send messages to";


  @Getter
  private final String redisUri;

  public RedisEnterpriseSourceConfig(final Map<?, ?> originals) {
    this(originals, true);
  }

  private RedisEnterpriseSourceConfig(final Map<?, ?> originals, final boolean validateAll) {
    super(CONFIG_DEF, originals, false);
    redisUri = getString(REDIS_URI_CONFIG);
  }

  public String getStream() {
    return getString(STREAM_CONFIG);
  }

  public String getOffset() {
    return getString(OFFSET_CONFIG);
  }

  public Integer getCount() {
    return getInt(COUNT_CONFIG);
  }

  public int getBatchSize() {
    return getInt(POLL_BATCH_SIZE_CONFIG);
  }

  public long getPollInterval() {
    return getLong(POLL_INTERVAL_CONFIG);
  }

  public String getTopic() {
    return getString(TOPIC_CONFIG);
  }

  private void validateStream() {
    String stream = getString(STREAM_CONFIG);
    if (StringUtil.isNullOrEmpty(stream)) {
      throw new RedisEnterpriseConfigException(STREAM_CONFIG, stream, String.format("Missing stream configuration: '%s'", STREAM_CONFIG));
    }
  }

  private static class RedisEnterpriseSourceConfigDef extends ConfigDef {

    public RedisEnterpriseSourceConfigDef() {
      String group = "RedisStream";
      int index = 0;
      define(REDIS_URI_CONFIG, Type.STRING, REDIS_URI_DEFAULT, Importance.HIGH, REDIS_URI_DOC, group, ++index, Width.MEDIUM, REDIS_URI_DISPLAY);
      define(STREAM_CONFIG, Type.STRING, null, Importance.HIGH, STREAM_DOC, group, ++index, Width.SHORT, STREAM_DISPLAY);
      define(OFFSET_CONFIG, Type.STRING, OFFSET_DEFAULT, Importance.MEDIUM, OFFSET_DOC, group, ++index, Width.SHORT, OFFSET_DISPLAY);
      define(COUNT_CONFIG, Type.INT, COUNT_DEFAULT, ConfigDef.Range.atLeast(1), Importance.LOW, COUNT_DOC, group, ++index, Width.SHORT, COUNT_DISPLAY);
      define(POLL_BATCH_SIZE_CONFIG, Type.INT, POLL_BATCH_SIZE_DEFAULT, ConfigDef.Range.atLeast(1), Importance.LOW, POLL_BATCH_SIZE_DOC, group, ++index, Width.MEDIUM, POLL_BATCH_SIZE_DISPLAY);
      define(POLL_INTERVAL_CONFIG, Type.LONG, POLL_INTERVAL_DEFAULT, ConfigDef.Range.atLeast(1), Importance.LOW, POLL_INTERVAL_DOC, group, ++index, Width.MEDIUM, POLL_INTERVAL_DISPLAY);
      group = "Topic";
      index = 0;
      define(TOPIC_CONFIG, ConfigDef.Type.STRING, null, Importance.HIGH, TOPIC_DOC, group, ++index, ConfigDef.Width.LONG, TOPIC_DISPLAY);
    }

    @Override
    public Map<String, ConfigValue> validateAll(final Map<String, String> props) {
      Map<String, ConfigValue> results = super.validateAll(props);
      if (results.values().stream().anyMatch((c) -> !c.errorMessages().isEmpty())) {
        return results;
      }

      RedisEnterpriseSourceConfig config = new RedisEnterpriseSourceConfig(props, false);
      try {
        config.validateStream();
      } catch (RedisEnterpriseConfigException e) {
        results.put(e.getName(), new ConfigValue(e.getName(), e.getValue(), Collections.emptyList(), Collections.singletonList(e.getMessage())));
      }
      return results;
    }
  }

}
