#!/usr/bin/env python3
"""Consume messages from a Redis stream."""

import os

from redis_streams_messaging import StreamConfig, StreamConsumer


def main() -> None:
    config = StreamConfig(redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    consumer = StreamConsumer(config)
    stream = os.getenv("STREAM_NAME", "demo-events")

    def handler(payload: dict) -> None:
        print(f"Handled: {payload}")

    processed = consumer.consume(stream, handler, max_messages=5)
    print(f"Processed {processed} message(s)")
    consumer.close()


if __name__ == "__main__":
    main()
