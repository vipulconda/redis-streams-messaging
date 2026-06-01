package com.vipulconda.streams;

import java.util.Map;

public final class ProducerDemo {

    private ProducerDemo() {}

    public static void main(String[] args) throws Exception {
        String redisUrl = System.getenv().getOrDefault("REDIS_URL", "redis://localhost:6379");
        String stream = System.getenv().getOrDefault("STREAM_NAME", "demo-events");

        try (StreamProducer producer = new StreamProducer(new StreamConfig(redisUrl, "app-consumers", "producer-1", 3, 2000, 10))) {
            String id = producer.publish(stream, Map.of("type", "demo", "value", 1));
            System.out.println("Published to " + stream + ": " + id);
        }
    }
}
