package com.redis.kafka.connect.sink;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;

public class HsetDel<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

    private Function<T, Map<K, V>> mapFunction;

    public void setMapFunction(Function<T, Map<K, V>> map) {
        this.mapFunction = map;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        Map<K, V> map = mapFunction.apply(item);
        if (Objects.isNull(map) || CollectionUtils.isEmpty(map)) {
            return ((RedisKeyAsyncCommands<K, V>) commands).del(key);
        }
        return ((RedisHashAsyncCommands<K, V>) commands).hset(key, map);
    }

}