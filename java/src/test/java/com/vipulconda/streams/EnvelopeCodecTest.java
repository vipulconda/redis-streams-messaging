package com.vipulconda.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

class EnvelopeCodecTest {

    @Test
    void roundTripEnvelope() throws Exception {
        MessageEnvelope original = MessageEnvelope.create(Map.of("key", "value"), 1);
        String json = EnvelopeCodec.serialize(original);
        MessageEnvelope parsed = EnvelopeCodec.deserialize(json);
        assertEquals(original.id(), parsed.id());
        assertEquals(original.attempt(), parsed.attempt());
        assertEquals("value", parsed.payload().get("key"));
    }
}
