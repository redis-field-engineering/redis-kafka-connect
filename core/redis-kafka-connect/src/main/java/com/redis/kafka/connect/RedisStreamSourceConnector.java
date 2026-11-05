/*
 * Copyright © 2021 Redis
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redis.kafka.connect;

import com.redis.kafka.connect.source.AbstractRedisSourceConnector;
import com.redis.kafka.connect.source.RedisStreamSourceConfig;
import com.redis.kafka.connect.source.RedisStreamSourceConfigDef;
import com.redis.kafka.connect.source.RedisStreamSourceTask;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;

public class RedisStreamSourceConnector extends AbstractRedisSourceConnector {

    private String consumerName;

    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        consumerName = props.getOrDefault(RedisStreamSourceConfigDef.STREAM_CONSUMER_NAME_CONFIG,
                RedisStreamSourceConfigDef.STREAM_CONSUMER_NAME_DEFAULT);
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1 && !consumerName.contains(RedisStreamSourceConfigDef.TOKEN_TASK)) {
            throw new ConfigException("Multiple stream source tasks require ${task} in redis.stream.consumer.name");
        }
        return super.taskConfigs(maxTasks);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisStreamSourceTask.class;
    }

    @Override
    public RedisStreamSourceConfigDef config() {
        return RedisStreamSourceConfig.CONFIG;
    }

}
