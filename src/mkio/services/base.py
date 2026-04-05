"""Service base class with lifecycle hooks."""

from __future__ import annotations

from typing import Any, Callable, Awaitable, TYPE_CHECKING

if TYPE_CHECKING:
    from aiohttp.web import WebSocketResponse
    from mkio.change_bus import ChangeBus
    from mkio.database import Database
    from mkio.writer import WriteBatcher

# Type for the monitor notification callback
MonitorNotifier = Callable[[str, str, dict[str, Any] | bytes], Awaitable[None]]


class Service:
    """Base class for all services (built-in and custom).

    Subclass and override hooks to implement custom behavior.
    """

    name: str = ""

    def __init__(
        self,
        config: dict[str, Any],
        db: Database,
        change_bus: ChangeBus,
        writer: WriteBatcher,
    ) -> None:
        self.config = config
        self.db = db
        self.bus = change_bus
        self.writer = writer
        self._monitor_notifier: MonitorNotifier | None = None

    async def notify_monitors(self, direction: str, data: dict[str, Any] | bytes) -> None:
        """Notify any active monitors about a message. Called by subclasses."""
        if self._monitor_notifier:
            await self._monitor_notifier(self.name, direction, data)

    async def start(self) -> None:
        """Called once at server startup. Override to init caches, subscribe to bus."""

    async def stop(self) -> None:
        """Called at shutdown."""

    async def on_subscribe(self, ws: WebSocketResponse, msg: dict[str, Any]) -> None:
        """Client wants to subscribe. Send snapshot, then start pushing updates."""

    async def on_unsubscribe(self, ws: WebSocketResponse, msg: dict[str, Any]) -> None:
        """Client unsubscribes or disconnects."""

    async def on_message(self, ws: WebSocketResponse, msg: dict[str, Any]) -> None:
        """Handle a message routed to this service (transactions, checks, custom)."""
