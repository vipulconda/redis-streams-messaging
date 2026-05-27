package com.vipulconda.streams;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record MessageEnvelope(String id, int attempt, String timestamp, Map<String, Object> payload) {

    public static MessageEnvelope create(Map<String, Object> payload) {
        return create(payload, 0);
    }

    public static MessageEnvelope create(Map<String, Object> payload, int attempt) {
        return new MessageEnvelope(UUID.randomUUID().toString(), attempt, Instant.now().toString(), payload);
    }
}
