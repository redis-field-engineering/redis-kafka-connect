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

import org.apache.kafka.connect.connector.Task;

import com.redis.kafka.connect.source.AbstractRedisSourceConnector;
import com.redis.kafka.connect.source.RedisStreamSourceConfig;
import com.redis.kafka.connect.source.RedisStreamSourceConfigDef;
import com.redis.kafka.connect.source.RedisStreamSourceTask;

public class RedisStreamSourceConnector extends AbstractRedisSourceConnector {

    @Override
    public Class<? extends Task> taskClass() {
        return RedisStreamSourceTask.class;
    }

    @Override
    public RedisStreamSourceConfigDef config() {
        return RedisStreamSourceConfig.CONFIG;
    }

}
