package com.redis.kafka.connect.sink;

import java.util.Objects;

import org.apache.kafka.common.TopicPartition;
import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.netty.util.internal.StringUtil;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, creatorVisibility = Visibility.NONE)
public class SinkOffsetState implements Comparable<SinkOffsetState> {

    @JsonProperty("topic")
    private String topic;

    @JsonProperty("partition")
    private Integer partition;

    @JsonProperty("offset")
    private Long offset;

    @JsonCreator
    private SinkOffsetState() {

    }

    public String topic() {
        return this.topic;
    }

    public Integer partition() {
        return this.partition;
    }

    public Long offset() {
        return this.offset;
    }

    private SinkOffsetState(String topic, Integer partition, Long offset) {
        Assert.isTrue(!StringUtil.isNullOrEmpty(topic), "topic cannot be null or empty.");
        Assert.notNull(partition, "partition cannot be null.");
        Assert.notNull(offset, "offset cannot be null.");
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public TopicPartition topicPartition() {
        return new TopicPartition(this.topic, this.partition);
    }

    public static SinkOffsetState of(TopicPartition topicPartition, long offset) {
        Assert.notNull(topicPartition, "topicPartition cannot be null.");
        return of(topicPartition.topic(), topicPartition.partition(), offset);
    }

    public static SinkOffsetState of(String topic, int partition, long offset) {
        return new SinkOffsetState(topic, partition, offset);
    }

    @Override
    public String toString() {
        return "SinkOffsetState [topic=" + topic + ", partition=" + partition + ", offset=" + offset + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, partition, topic);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SinkOffsetState other = (SinkOffsetState) obj;
        return Objects.equals(offset, other.offset) && Objects.equals(partition, other.partition)
                && Objects.equals(topic, other.topic);
    }

    @Override
    public int compareTo(SinkOffsetState that) {
        int compare = topic.compareTo(that.topic);
        if (compare == 0) {
            compare = partition.compareTo(that.partition);
            if (compare == 0) {
                compare = offset.compareTo(that.offset);
            }
        }
        return compare;
    }

}
