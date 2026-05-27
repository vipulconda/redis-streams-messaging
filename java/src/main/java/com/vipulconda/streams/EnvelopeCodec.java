package com.vipulconda.streams;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class EnvelopeCodec {

    static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new JavaTimeModule());

    private EnvelopeCodec() {}

    public static String serialize(MessageEnvelope envelope) throws Exception {
        return MAPPER.writeValueAsString(envelope);
    }

    public static MessageEnvelope deserialize(String json) throws Exception {
        return MAPPER.readValue(json, new TypeReference<>() {});
    }
}
