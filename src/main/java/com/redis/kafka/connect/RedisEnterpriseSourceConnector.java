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

import com.redis.kafka.connect.source.RedisEnterpriseSourceConfig;
import com.redis.kafka.connect.source.RedisEnterpriseSourceTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisEnterpriseSourceConnector extends SourceConnector {

    private Map<String, String> props;
    private RedisEnterpriseSourceConfig config;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        try {
            this.config = new RedisEnterpriseSourceConfig(props);
        } catch (ConfigException configException) {
            throw new ConnectException(configException);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisEnterpriseSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (this.config.getReaderType() == RedisEnterpriseSourceConfig.ReaderType.KEYS) {
            // Partition the configs based on channels
            final List<List<String>> partitionedPatterns = ConnectorUtils
                    .groupPartitions(this.config.getKeyPatterns(), Math.min(this.config.getKeyPatterns().size(), maxTasks));

            // Create task configs based on the partitions
            return partitionedPatterns.stream().map(this::taskConfig).collect(Collectors.toList());
        }
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(this.props);
            taskConfig.put(RedisEnterpriseSourceTask.TASK_ID, Integer.toString(i));
            taskConfigs.add(taskConfig);
        }
        return taskConfigs;
    }

    private Map<String, String> taskConfig(List<String> patterns) {
        final Map<String, String> taskConfig = new HashMap<>(this.config.originalsStrings());
        taskConfig.put(RedisEnterpriseSourceConfig.KEY_PATTERNS_CONFIG, String.join(",", patterns));
        return taskConfig;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return new RedisEnterpriseSourceConfig.RedisEnterpriseSourceConfigDef();
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
