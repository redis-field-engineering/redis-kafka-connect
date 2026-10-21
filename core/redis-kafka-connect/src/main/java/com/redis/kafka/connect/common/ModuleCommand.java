package com.redis.kafka.connect.common;

import io.lettuce.core.protocol.ProtocolKeyword;
import java.nio.charset.StandardCharsets;

/** TimeSeries commands not yet covered by Lettuce's typed API. */
public enum ModuleCommand implements ProtocolKeyword {
    TS_ADD("TS.ADD"), TS_RANGE("TS.RANGE");

    private final byte[] bytes;

    ModuleCommand(String command) {
        this.bytes = command.getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
