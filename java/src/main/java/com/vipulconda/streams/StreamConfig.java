package com.vipulconda.streams;

public record StreamConfig(
        String redisUrl,
        String groupName,
        String consumerName,
        int maxRetries,
        long blockMs,
        int batchSize) {

    public StreamConfig() {
        this("redis://localhost:6379", "app-consumers", "consumer-1", 3, 2000, 10);
    }

    public String dlqStream(String stream) {
        return stream + ":dlq";
    }
}
