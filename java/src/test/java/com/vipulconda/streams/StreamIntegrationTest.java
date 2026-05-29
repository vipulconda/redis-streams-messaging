package com.vipulconda.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class StreamIntegrationTest {

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    private static String redisUrl;

    @BeforeAll
    static void setup() {
        redisUrl = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
    }

    @AfterAll
    static void noop() {}

    @Test
    void publishAndConsume() throws Exception {
        StreamConfig config = new StreamConfig(redisUrl, "g-java", "c-java", 2, 1000, 5);
        String stream = "orders-java";

        try (StreamProducer producer = new StreamProducer(config);
                StreamConsumer consumer = new StreamConsumer(config)) {
            producer.publish(stream, Map.of("orderId", 99));
            List<Map<String, Object>> seen = new ArrayList<>();
            int count = consumer.consume(stream, seen::add, 1);
            assertEquals(1, count);
            assertEquals(99, seen.get(0).get("orderId"));
        }
    }

    @Test
    void failedMessageGoesToDlq() throws Exception {
        StreamConfig config = new StreamConfig(redisUrl, "g-dlq", "c-dlq", 1, 1000, 5);
        String stream = "events-java";

        try (StreamProducer producer = new StreamProducer(config);
                StreamConsumer consumer = new StreamConsumer(config)) {
            producer.publish(stream, Map.of("event", "x"));

            consumer.consume(stream, payload -> {
                throw new RuntimeException("fail");
            }, 1);
            consumer.consume(stream, payload -> {
                throw new RuntimeException("fail again");
            }, 1);

            long dlqLen = producer.commands().xlen(config.dlqStream(stream));
            assertTrue(dlqLen >= 1);
        }
    }
}
