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
import com.redis.kafka.connect.source.RedisKeysSourceConfig;
import com.redis.kafka.connect.source.RedisKeysSourceConfigDef;
import com.redis.kafka.connect.source.RedisKeysSourceTask;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.connector.Task;

public class RedisKeysSourceConnector extends AbstractRedisSourceConnector {

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return super.taskConfigs(Math.min(maxTasks, 1));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisKeysSourceTask.class;
    }

    @Override
    public RedisKeysSourceConfigDef config() {
        return RedisKeysSourceConfig.CONFIG;
    }

}
