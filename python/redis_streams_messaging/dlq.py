from typing import Any

import redis

from redis_streams_messaging.config import StreamConfig
from redis_streams_messaging.envelope import serialize_envelope


class DlqHandler:
    def __init__(self, client: redis.Redis, config: StreamConfig):
        self._client = client
        self._config = config

    def send(
        self,
        stream: str,
        envelope: dict[str, Any],
        error: str,
    ) -> str:
        dlq = self._config.dlq_stream(stream)
        dlq_envelope = {
            **envelope,
            "dlq_error": error,
            "dlq_timestamp": envelope.get("timestamp"),
        }
        return self._client.xadd(dlq, {"data": serialize_envelope(dlq_envelope)})
