/*
 * Copyright © 2021 Redis Labs
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
package com.redislabs.kafka.connect;

import com.redislabs.kafka.connect.source.RedisEnterpriseSourceConfig;
import com.redislabs.kafka.connect.source.RedisEnterpriseSourceTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RedisEnterpriseSourceConnector extends SourceConnector {

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisEnterpriseSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.singletonList(props);
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
