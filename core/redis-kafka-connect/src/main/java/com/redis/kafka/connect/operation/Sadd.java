package com.redis.kafka.connect.operation;

import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import java.util.function.Function;

public class Sadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {
    private Function<T, V> valueFunction;
    private Function<T, Boolean> conditionFunction;

    public Sadd() {
    }

    public void setValueFunction(Function<T, V> function) {
        this.valueFunction = function;
    }

    public void setConditionFunction(Function<T, Boolean> function) {
        this.conditionFunction = function;
    }

    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        V value = valueFunction.apply(item);
        if (conditionFunction.apply(item)) {
            return ((RedisSetAsyncCommands<K, V>) commands).srem(key, value);
        } else {
            return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, value);
        }
    }
}
