"""WebSocket protocol: JSON envelope parse/serialize helpers."""

from __future__ import annotations

from typing import Any

from mkio._json import dumps, loads


def parse_message(data: bytes | str) -> dict[str, Any]:
    """Parse and validate a WebSocket message envelope."""
    msg = loads(data)
    if not isinstance(msg, dict):
        raise ValueError("Message must be a JSON object")
    return msg


def make_result(
    ref: str | None,
    service: str,
    version: str,
    payload: dict[str, Any] | None = None,
) -> bytes:
    envelope: dict[str, Any] = {"type": "result", "service": service, "version": version}
    if ref is not None:
        envelope["ref"] = ref
    if payload:
        envelope.update(payload)
    envelope.setdefault("ok", True)
    return dumps(envelope)


def make_error(ref: str | None, message: str) -> bytes:
    envelope: dict[str, Any] = {"type": "error", "message": message}
    if ref is not None:
        envelope["ref"] = ref
    return dumps(envelope)


def make_snapshot(
    ref: str | None,
    service: str,
    version: str,
    rows: list[dict[str, Any]],
) -> bytes:
    envelope: dict[str, Any] = {
        "type": "snapshot",
        "service": service,
        "version": version,
        "rows": rows,
    }
    if ref is not None:
        envelope["ref"] = ref
    return dumps(envelope)


def make_delta(
    ref: str | None,
    service: str,
    version: str,
    changes: list[dict[str, Any]],
) -> bytes:
    envelope: dict[str, Any] = {
        "type": "delta",
        "service": service,
        "version": version,
        "changes": changes,
    }
    if ref is not None:
        envelope["ref"] = ref
    return dumps(envelope)


def make_update(
    service: str,
    version: str,
    op: str,
    row: dict[str, Any],
) -> bytes:
    return dumps({
        "type": "update",
        "service": service,
        "version": version,
        "op": op,
        "row": row,
    })
