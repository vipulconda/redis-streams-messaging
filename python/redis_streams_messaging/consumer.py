import logging
from collections.abc import Callable
from typing import Any

import redis

from redis_streams_messaging.config import StreamConfig
from redis_streams_messaging.dlq import DlqHandler
from redis_streams_messaging.envelope import build_envelope, parse_envelope, serialize_envelope

logger = logging.getLogger(__name__)

MessageHandler = Callable[[dict[str, Any]], None]


class StreamConsumer:
    def __init__(self, config: StreamConfig, client: redis.Redis | None = None):
        self._config = config
        self._client = client or redis.Redis.from_url(config.redis_url, decode_responses=True)
        self._owns_client = client is None
        self._dlq = DlqHandler(self._client, config)

    def ensure_group(self, stream: str) -> None:
        try:
            self._client.xgroup_create(stream, self._config.group_name, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def consume(self, stream: str, handler: MessageHandler, max_messages: int | None = None) -> int:
        self.ensure_group(stream)
        processed = 0

        while max_messages is None or processed < max_messages:
            entries = self._client.xreadgroup(
                groupname=self._config.group_name,
                consumername=self._config.consumer_name,
                streams={stream: ">"},
                count=self._config.batch_size,
                block=self._config.block_ms,
            )
            if not entries:
                break

            for _stream_name, messages in entries:
                for message_id, fields in messages:
                    self._process_one(stream, message_id, fields.get("data", ""), handler)
                    processed += 1
                    if max_messages is not None and processed >= max_messages:
                        return processed

        return processed

    def _process_one(
        self,
        stream: str,
        message_id: str,
        raw: str,
        handler: MessageHandler,
    ) -> None:
        envelope = parse_envelope(raw)
        try:
            handler(envelope["payload"])
            self._client.xack(stream, self._config.group_name, message_id)
        except Exception as exc:
            attempt = int(envelope.get("attempt", 0)) + 1
            logger.warning("Handler failed for %s attempt %s: %s", envelope.get("id"), attempt, exc)
            self._client.xack(stream, self._config.group_name, message_id)

            if attempt > self._config.max_retries:
                self._dlq.send(stream, envelope, str(exc))
            else:
                retry_envelope = build_envelope(envelope["payload"], attempt=attempt)
                self._client.xadd(stream, {"data": serialize_envelope(retry_envelope)})

    def close(self) -> None:
        if self._owns_client:
            self._client.close()
