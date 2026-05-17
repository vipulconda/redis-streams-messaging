from typing import Any

import redis

from redis_streams_messaging.config import StreamConfig
from redis_streams_messaging.envelope import build_envelope, serialize_envelope


class StreamProducer:
    def __init__(self, config: StreamConfig, client: redis.Redis | None = None):
        self._config = config
        self._client = client or redis.Redis.from_url(config.redis_url, decode_responses=True)
        self._owns_client = client is None

    def publish(self, stream: str, payload: dict[str, Any]) -> str:
        envelope = build_envelope(payload)
        return self._client.xadd(stream, {"data": serialize_envelope(envelope)})

    def close(self) -> None:
        if self._owns_client:
            self._client.close()
