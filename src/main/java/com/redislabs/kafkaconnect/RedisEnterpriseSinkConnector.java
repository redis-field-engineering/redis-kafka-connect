package com.redislabs.kafkaconnect;

import com.redislabs.kafkaconnect.sink.RedisEnterpriseSinkConfig;
import com.redislabs.kafkaconnect.sink.RedisEnterpriseSinkTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class  RedisEnterpriseSinkConnector extends SinkConnector {

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisEnterpriseSinkTask.class;
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
        return RedisEnterpriseSinkConfig.CONFIG_DEF;
    }


    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}