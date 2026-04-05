"""JSON serialization with orjson fallback to stdlib json."""

from __future__ import annotations

from typing import Any

try:
    import orjson

    def dumps(obj: Any) -> bytes:
        return orjson.dumps(obj)

    def loads(data: bytes | str) -> Any:
        return orjson.loads(data)

except ImportError:
    import json

    def dumps(obj: Any) -> bytes:  # type: ignore[misc]
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

    def loads(data: bytes | str) -> Any:  # type: ignore[misc]
        return json.loads(data)
