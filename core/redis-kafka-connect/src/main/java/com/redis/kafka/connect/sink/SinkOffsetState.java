package com.redis.kafka.connect.sink;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.TopicPartition;

public record SinkOffsetState(@JsonProperty("topic") String topic,
                              @JsonProperty("partition") Integer partition,
                              @JsonProperty("offset") Long offset) implements Comparable<SinkOffsetState> {
    @JsonCreator
    public SinkOffsetState {
        if (topic == null || topic.isEmpty() || partition == null || partition < 0 || offset == null || offset < 0) {
            throw new IllegalArgumentException("Sink offset state requires a topic and nonnegative partition/offset");
        }
    }

    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }

    public static SinkOffsetState of(TopicPartition partition, long offset) {
        if (partition == null) {
            throw new IllegalArgumentException("topicPartition cannot be null");
        }
        return of(partition.topic(), partition.partition(), offset);
    }

    public static SinkOffsetState of(String topic, int partition, long offset) {
        return new SinkOffsetState(topic, partition, offset);
    }

    @Override
    public int compareTo(SinkOffsetState other) {
        int compare = topic.compareTo(other.topic);
        if (compare == 0) {
            compare = partition.compareTo(other.partition);
        }
        return compare == 0 ? offset.compareTo(other.offset) : compare;
    }
}
