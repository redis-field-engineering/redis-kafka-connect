package com.redis.kafka.connect.operation;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

import java.util.function.Function;

public class JsonSet<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {
    public static final String ROOT_PATH = "$";
    private Function<T, String> pathFunction = (t) -> {
        return "$";
    };
    private Function<T, V> valueFunction;
    private Function<T, Boolean> conditionFunction;

    public JsonSet() {
    }

    public void setPath(String path) {
        this.pathFunction = (t) -> {
            return path;
        };
    }

    public void setPathFunction(Function<T, String> path) {
        this.pathFunction = path;
    }

    public void setValueFunction(Function<T, V> value) {
        this.valueFunction = value;
    }

    public void setConditionFunction(Function<T, Boolean> function) {
        this.conditionFunction = function;
    }

    protected RedisFuture<String> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        if (conditionFunction.apply(item)) {
            return ((RedisKeyAsyncCommands)commands).del(new Object[]{key});
        } else {
            String path = (String)this.pathFunction.apply(item);
            V value = this.valueFunction.apply(item);
            return ((RedisJSONAsyncCommands)commands).jsonSet(key, path, value);
        }
    }
}
