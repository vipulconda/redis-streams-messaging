#!/usr/bin/env python3
"""Publish sample messages to a Redis stream."""

import os

from redis_streams_messaging import StreamConfig, StreamProducer


def main() -> None:
    config = StreamConfig(redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    producer = StreamProducer(config)
    stream = os.getenv("STREAM_NAME", "demo-events")
    message_id = producer.publish(stream, {"type": "demo", "value": 1})
    print(f"Published to {stream}: {message_id}")
    producer.close()


if __name__ == "__main__":
    main()
