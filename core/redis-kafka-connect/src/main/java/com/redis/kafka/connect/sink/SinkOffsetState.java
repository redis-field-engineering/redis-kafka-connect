package com.redis.kafka.connect.sink;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.TopicPartition;

/**
 * Persisted Kafka sink offset for a single topic partition.
 *
 * @param topic Kafka topic name
 * @param partition nonnegative Kafka partition number
 * @param offset nonnegative stored Kafka offset
 */
public record SinkOffsetState(@JsonProperty("topic") String topic,
                              @JsonProperty("partition") Integer partition,
                              @JsonProperty("offset") Long offset) implements Comparable<SinkOffsetState> {
    /**
     * Creates and validates a persisted offset.
     *
     * @param topic nonempty Kafka topic name
     * @param partition nonnegative partition number
     * @param offset nonnegative stored offset
     * @throws IllegalArgumentException if any field is missing or invalid
     */
    @JsonCreator
    public SinkOffsetState {
        if (topic == null || topic.isEmpty() || partition == null || partition < 0 || offset == null || offset < 0) {
            throw new IllegalArgumentException("Sink offset state requires a topic and nonnegative partition/offset");
        }
    }

    /**
     * Returns the Kafka identity of this offset.
     *
     * @return topic and partition
     */
    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }

    /**
     * Creates an offset state from a Kafka partition.
     *
     * @param partition Kafka topic partition
     * @param offset nonnegative stored offset
     * @return validated offset state
     */
    public static SinkOffsetState of(TopicPartition partition, long offset) {
        if (partition == null) {
            throw new IllegalArgumentException("topicPartition cannot be null");
        }
        return of(partition.topic(), partition.partition(), offset);
    }

    /**
     * Creates an offset state from its individual fields.
     *
     * @param topic nonempty Kafka topic name
     * @param partition nonnegative partition number
     * @param offset nonnegative stored offset
     * @return validated offset state
     */
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
