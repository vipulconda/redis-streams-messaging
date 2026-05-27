package com.vipulconda.streams;

import java.util.HashMap;
import java.util.Map;

import io.lettuce.core.api.sync.RedisCommands;

public class DlqHandler {

    private final RedisCommands<String, String> commands;
    private final StreamConfig config;

    public DlqHandler(RedisCommands<String, String> commands, StreamConfig config) {
        this.commands = commands;
        this.config = config;
    }

    public String send(String stream, MessageEnvelope envelope, String error) throws Exception {
        Map<String, Object> dlqBody = new HashMap<>();
        dlqBody.put("id", envelope.id());
        dlqBody.put("attempt", envelope.attempt());
        dlqBody.put("timestamp", envelope.timestamp());
        dlqBody.put("payload", envelope.payload());
        dlqBody.put("dlq_error", error);
        dlqBody.put("dlq_timestamp", envelope.timestamp());
        return commands.xadd(config.dlqStream(stream), Map.of("data", EnvelopeCodec.MAPPER.writeValueAsString(dlqBody)));
    }
}
