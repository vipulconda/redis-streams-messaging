from dataclasses import dataclass


@dataclass
class StreamConfig:
    redis_url: str = "redis://localhost:6379/0"
    group_name: str = "app-consumers"
    consumer_name: str = "consumer-1"
    max_retries: int = 3
    block_ms: int = 2000
    batch_size: int = 10

    def dlq_stream(self, stream: str) -> str:
        return f"{stream}:dlq"
