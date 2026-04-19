"""WebSocket protocol: JSON envelope parse/serialize helpers."""

from __future__ import annotations

from typing import Any

from mkio._json import dumps, loads


_VALID_MSG_TYPES = frozenset({
    "subscribe", "unsubscribe", "monitor", "check", "transaction", "",
})


def parse_message(data: bytes | str) -> dict[str, Any]:
    """Parse and validate a WebSocket message envelope."""
    msg = loads(data)
    if not isinstance(msg, dict):
        raise ValueError(
            f"Message must be a JSON object, got {type(msg).__name__}"
        )
    msg_type = msg.get("type", "")
    if msg_type and msg_type not in _VALID_MSG_TYPES:
        raise ValueError(
            f"Unknown message type: {msg_type!r}. "
            f"Valid types: subscribe, unsubscribe, monitor, check"
        )
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


def make_nack(
    service: str,
    message: str,
    *,
    ref: str | None = None,
    msgid: str | None = None,
    subid: str | None = None,
) -> bytes:
    envelope: dict[str, Any] = {"type": "nack", "service": service, "message": message}
    if ref is not None:
        envelope["ref"] = ref
    if msgid is not None:
        envelope["msgid"] = msgid
    if subid is not None:
        envelope["subid"] = subid
    return dumps(envelope)


def make_snapshot(
    ref: str | None,
    service: str,
    rows: list[dict[str, Any]],
    *,
    subid: str | None = None,
) -> bytes:
    envelope: dict[str, Any] = {
        "type": "snapshot",
        "service": service,
        "rows": rows,
    }
    if ref is not None:
        envelope["ref"] = ref
    if subid is not None:
        envelope["subid"] = subid
    return dumps(envelope)


def make_delta(
    ref: str | None,
    service: str,
    changes: list[dict[str, Any]],
    *,
    subid: str | None = None,
) -> bytes:
    envelope: dict[str, Any] = {
        "type": "delta",
        "service": service,
        "changes": changes,
    }
    if ref is not None:
        envelope["ref"] = ref
    if subid is not None:
        envelope["subid"] = subid
    return dumps(envelope)


def make_update(
    service: str,
    ref: str | None,
    op: str,
    row: dict[str, Any],
    *,
    subid: str | None = None,
) -> bytes:
    envelope: dict[str, Any] = {
        "type": "update",
        "service": service,
        "op": op,
        "row": row,
    }
    if ref is not None:
        envelope["ref"] = ref
    if subid is not None:
        envelope["subid"] = subid
    return dumps(envelope)
