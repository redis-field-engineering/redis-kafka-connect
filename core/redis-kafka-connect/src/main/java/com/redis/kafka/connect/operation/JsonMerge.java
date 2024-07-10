package com.redis.kafka.connect.operation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

public class JsonMerge<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {
    private static final Logger log = LoggerFactory.getLogger(JsonSet.class);

    public static final String ROOT_PATH = "$";
    private static final Function<Object, String> DEFAULT_PATH_FUNCTION = t -> ROOT_PATH;

    private Function<T, String> pathFunction;
    private Function<T, V> valueFunction;
    private Function<T, Boolean> conditionFunction;

    private final ObjectMapper mapper;

    public JsonMerge() {
        // Set default path function
        this.pathFunction = (Function<T, String>) DEFAULT_PATH_FUNCTION;
        this.mapper = new ObjectMapper();
        this.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    public void setPath(String path) {
        this.pathFunction = t -> path;
    }

    public void setPathFunction(Function<T, String> pathFunction) {
        this.pathFunction = pathFunction;
    }

    public void setValueFunction(Function<T, V> valueFunction) {
        this.valueFunction = valueFunction;
    }

    public void setConditionFunction(Function<T, Boolean> conditionFunction) {
        this.conditionFunction = conditionFunction;
    }

    protected RedisFuture<String> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        try {
            String path = determinePath(item);
            logPath(path);
            V value = this.valueFunction.apply(item);
            log.info("Value: {}", value);

            if (conditionFunction.apply(item)) {
                if (isPathSet()) {
                    return deleteJsonPath(commands, key, path);
                } else {
                    return deleteKey(commands, key);
                }
            }
System.out.println("isPathSet() "+isPathSet());
            // Perform JSON operation based on whether path is set
            if (isPathSet()) {

                return performJsonMerge(commands, key, path, value);
            } else {
                return performJsonSet(commands, key, value);
            }
        } catch (JsonProcessingException e) {
            log.error("Error processing JSON", e);
            return null;
        } catch (Exception e) {
            log.error("Error executing Redis command", e);
            return null;
        }
    }

    private RedisFuture<String> deleteKey(BaseRedisAsyncCommands<K, V> commands, K key) {
        return ((RedisKeyAsyncCommands) commands).del(key);
    }

    private RedisFuture<String> deleteJsonPath(BaseRedisAsyncCommands<K, V> commands, K key, String path) {
        return ((RedisJSONAsyncCommands) commands).jsonDel(key, path);
    }

    private String determinePath(T item) {
        return this.pathFunction.apply(item);
    }

    private void logPath(String path) {
        if (isPathSet()) {
            log.info("Path is set to: {}", path);
        } else {
            log.info("Path is not set, using default: {}", ROOT_PATH);
        }
    }

    @SuppressWarnings("unchecked")
    private RedisFuture<String> performJsonMerge(BaseRedisAsyncCommands<K, V> commands, K key, String path, V value) throws JsonProcessingException {
        // Convert empty JSON object
        String emptyJson = mapper.writeValueAsString(new Object());
        byte[] emptyJsonBytes = emptyJson.getBytes(StandardCharsets.UTF_8);

        // Merge empty JSON object first
        ((RedisJSONAsyncCommands<K, V>) commands).jsonMerge(key, ROOT_PATH, (V) emptyJsonBytes);
        // Merge actual value
        return ((RedisJSONAsyncCommands<K, V>) commands).jsonMerge(key, path, value);
    }

    private RedisFuture<String> performJsonSet(BaseRedisAsyncCommands<K, V> commands, K key, V value) throws JsonProcessingException {
        return ((RedisJSONAsyncCommands<K, V>) commands).jsonSet(key, ROOT_PATH, value);
    }

    private boolean isPathSet() {
        return this.pathFunction != DEFAULT_PATH_FUNCTION;
    }
}
