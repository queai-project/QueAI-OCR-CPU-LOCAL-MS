import json
from typing import Any


def encode_sse(event: str, data: dict[str, Any], event_id: str | None = None) -> bytes:
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    payload = json.dumps(data, ensure_ascii=False)
    for line in payload.splitlines():
        lines.append(f"data: {line}")
    lines.append("")
    lines.append("")
    return "\n".join(lines).encode("utf-8")


def encode_sse_comment(comment: str = "keep-alive") -> bytes:
    return f": {comment}\n\n".encode("utf-8")