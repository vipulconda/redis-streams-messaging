package com.vipulconda.streams;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public class StreamConsumer implements AutoCloseable {

    private final StreamConfig config;
    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final DlqHandler dlqHandler;
    private final boolean ownsClient;

    public StreamConsumer(StreamConfig config) {
        this(config, RedisClient.create(config.redisUrl()), true);
    }

    public StreamConsumer(StreamConfig config, RedisClient client, boolean ownsClient) {
        this.config = config;
        this.client = client;
        this.connection = client.connect();
        this.dlqHandler = new DlqHandler(connection.sync(), config);
        this.ownsClient = ownsClient;
    }

    public void ensureGroup(String stream) {
        RedisCommands<String, String> commands = connection.sync();
        try {
            commands.xgroupCreate(
                    XReadArgs.StreamOffset.from(stream, "0"),
                    config.groupName(),
                    XGroupCreateArgs.Builder.mkstream());
        } catch (RedisBusyException ignored) {
            // group already exists
        }
    }

    public int consume(String stream, Consumer<Map<String, Object>> handler, Integer maxMessages) throws Exception {
        ensureGroup(stream);
        RedisCommands<String, String> commands = connection.sync();
        int processed = 0;

        while (maxMessages == null || processed < maxMessages) {
            List<StreamMessage<String, String>> messages = commands.xreadgroup(
                    io.lettuce.core.Consumer.from(config.groupName(), config.consumerName()),
                    XReadArgs.Builder.block(config.blockMs()).count(config.batchSize()),
                    XReadArgs.StreamOffset.from(stream, ">"));

            if (messages == null || messages.isEmpty()) {
                break;
            }

            for (StreamMessage<String, String> message : messages) {
                processOne(stream, message, handler, commands);
                processed++;
                if (maxMessages != null && processed >= maxMessages) {
                    return processed;
                }
            }
        }

        return processed;
    }

    private void processOne(
            String stream,
            StreamMessage<String, String> message,
            Consumer<Map<String, Object>> handler,
            RedisCommands<String, String> commands)
            throws Exception {
        String raw = message.getBody().get("data");
        MessageEnvelope envelope = EnvelopeCodec.deserialize(raw);

        try {
            handler.accept(envelope.payload());
            commands.xack(stream, config.groupName(), message.getId());
        } catch (Exception exc) {
            commands.xack(stream, config.groupName(), message.getId());
            int nextAttempt = envelope.attempt() + 1;
            if (nextAttempt > config.maxRetries()) {
                dlqHandler.send(stream, envelope, exc.getMessage());
            } else {
                MessageEnvelope retry = MessageEnvelope.create(envelope.payload(), nextAttempt);
                commands.xadd(stream, Map.of("data", EnvelopeCodec.serialize(retry)));
            }
        }
    }

    @Override
    public void close() {
        connection.close();
        if (ownsClient) {
            client.shutdown();
        }
    }
}
