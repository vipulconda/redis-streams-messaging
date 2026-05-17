import json
import uuid
from datetime import datetime, timezone
from typing import Any


def build_envelope(payload: dict[str, Any], attempt: int = 0) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "attempt": attempt,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def serialize_envelope(envelope: dict[str, Any]) -> str:
    return json.dumps(envelope)


def parse_envelope(raw: str) -> dict[str, Any]:
    data = json.loads(raw)
    if "payload" not in data:
        raise ValueError("Invalid envelope: missing payload")
    return data
