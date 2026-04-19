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
    ref: str,
    service: str,
    payload: dict[str, Any] | None = None,
    *,
    msgid: str | None = None,
) -> bytes:
    envelope: dict[str, Any] = {"type": "result", "service": service, "ref": ref}
    if msgid is not None:
        envelope["msgid"] = msgid
    if payload:
        envelope.update(payload)
    envelope.setdefault("ok", True)
    return dumps(envelope)


def make_error(ref: str | None, message: str, *, msgid: str | None = None) -> bytes:
    envelope: dict[str, Any] = {"type": "error", "message": message}
    if ref is not None:
        envelope["ref"] = ref
    if msgid is not None:
        envelope["msgid"] = msgid
    return dumps(envelope)


def make_snapshot(
    ref: str,
    service: str,
    rows: list[dict[str, Any]],
    *,
    subid: str | None = None,
) -> bytes:
    envelope: dict[str, Any] = {
        "type": "snapshot",
        "service": service,
        "ref": ref,
        "rows": rows,
    }
    if subid is not None:
        envelope["subid"] = subid
    return dumps(envelope)


def make_delta(
    ref: str,
    service: str,
    changes: list[dict[str, Any]],
    *,
    subid: str | None = None,
) -> bytes:
    envelope: dict[str, Any] = {
        "type": "delta",
        "service": service,
        "ref": ref,
        "changes": changes,
    }
    if subid is not None:
        envelope["subid"] = subid
    return dumps(envelope)


def make_update(
    service: str,
    ref: str,
    op: str,
    row: dict[str, Any],
    *,
    subid: str | None = None,
) -> bytes:
    envelope: dict[str, Any] = {
        "type": "update",
        "service": service,
        "ref": ref,
        "op": op,
        "row": row,
    }
    if subid is not None:
        envelope["subid"] = subid
    return dumps(envelope)
