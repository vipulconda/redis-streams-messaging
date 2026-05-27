package com.vipulconda.streams;

import java.util.Map;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public class StreamProducer implements AutoCloseable {

    private final StreamConfig config;
    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final boolean ownsClient;

    public StreamProducer(StreamConfig config) {
        this(config, RedisClient.create(config.redisUrl()), true);
    }

    public StreamProducer(StreamConfig config, RedisClient client, boolean ownsClient) {
        this.config = config;
        this.client = client;
        this.connection = client.connect();
        this.ownsClient = ownsClient;
    }

    public String publish(String stream, Map<String, Object> payload) throws Exception {
        MessageEnvelope envelope = MessageEnvelope.create(payload);
        RedisCommands<String, String> commands = connection.sync();
        return commands.xadd(stream, Map.of("data", EnvelopeCodec.serialize(envelope)));
    }

    public RedisCommands<String, String> commands() {
        return connection.sync();
    }

    @Override
    public void close() {
        connection.close();
        if (ownsClient) {
            client.shutdown();
        }
    }
}
