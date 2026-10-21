package com.redis.kafka.connect.sink;

import com.redis.kafka.connect.common.RedisConnection;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import java.util.List;

/** A validated command; constructing a batch does not perform I/O. */
record RedisWrite(ProtocolKeyword command, byte[] key, List<byte[]> arguments, Reply reply, long ttl) {
    enum Reply { STATUS, INTEGER, VALUE }

    RedisFuture<?> enqueue(RedisConnection<byte[], byte[]> connection) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addKey(key);
        arguments.forEach(args::addValue);
        CommandOutput<byte[], byte[], ?> output = switch (reply) {
            case STATUS -> new StatusOutput<>(ByteArrayCodec.INSTANCE);
            case INTEGER -> new IntegerOutput<>(ByteArrayCodec.INSTANCE);
            case VALUE -> new ValueOutput<>(ByteArrayCodec.INSTANCE);
        };
        return connection.async().dispatch(command, output, args);
    }
}
