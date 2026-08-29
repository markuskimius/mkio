"""MkioApp: programmatic server lifecycle."""

from __future__ import annotations

import asyncio
import signal
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable, TYPE_CHECKING

from aiohttp import web

from mkio.config import load_config
from mkio.server import (
    _api_service_detail,
    _api_services,
    _make_config_handler,
    _make_index_handler,
    _on_shutdown,
    _on_startup,
    _preflight_services,
    _ws_handler,
)

if TYPE_CHECKING:
    from mkio.change_bus import ChangeBus, ChangeEvent
    from mkio.database import Database
    from mkio.services.base import Service
    from mkio.writer import WriteBatcher


class MkioApp:
    """Programmatic handle to an mkio server.

    Use :func:`create_app` to construct instances.

    Example::

        app = create_app({"port": 8080, ...})
        await app.start()   # non-blocking
        await app.wait()    # blocks until stop() or signal

    Or for a blocking one-liner::

        app = create_app("server.toml")
        app.run()
    """

    def __init__(
        self,
        config: dict[str, Any],
        routes: list[tuple[str, str, Callable]] | None = None,
    ) -> None:
        self._config = config
        self._pending_routes: list[tuple[str, str, Callable]] = list(routes or [])
        self._pending_services: list[tuple[str, type[Service], dict[str, Any]]] = []
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._stopped: asyncio.Event | None = None
        self._running = False
        self._aiohttp_app: web.Application | None = None

        # Lifecycle hook lists
        self._startup_hooks: list[Callable[[], Awaitable[None]]] = []
        self._shutdown_hooks: list[Callable[[], Awaitable[None]]] = []
        self._connect_hooks: list[Callable[[web.WebSocketResponse], Awaitable[None]]] = []
        self._disconnect_hooks: list[Callable[[web.WebSocketResponse], Awaitable[None]]] = []

        # Auth handler (overrides table-backed auth)
        self._auth_handler: Callable[[dict], Awaitable[dict]] | None = None

        # Subscription tasks (for cleanup on stop)
        self._sub_tasks: list[tuple[asyncio.Task, list[str]]] = []

    @property
    def config(self) -> dict[str, Any]:
        """The resolved config dict."""
        return self._config

    # -- Pre-start registration --

    def add_routes(self, routes: list[tuple[str, str, Callable]]) -> None:
        """Add HTTP routes before starting.

        Args:
            routes: List of (method, path, handler) tuples.
                    method is an HTTP method string like ``"GET"`` or ``"POST"``.

        Raises:
            RuntimeError: If the server is already running.
        """
        if self._running:
            raise RuntimeError("Cannot add routes after the server has started")
        self._pending_routes.extend(routes)

    def add_service(
        self,
        name: str,
        cls: type[Service],
        config: dict[str, Any] | None = None,
    ) -> None:
        """Register a custom service class.

        The service is instantiated during :meth:`start` with the same
        lifecycle as config-driven services (``start()`` is called,
        monitors work, WS dispatch works).

        Args:
            name: Service name (used in WS protocol and API).
            cls: A :class:`Service` subclass.
            config: Service config dict. If omitted, an empty dict is used.

        Raises:
            RuntimeError: If the server is already running.
        """
        if self._running:
            raise RuntimeError("Cannot add services after the server has started")
        self._pending_services.append((name, cls, config or {}))

    # -- Lifecycle hooks --

    def on_startup(self, callback: Callable[[], Awaitable[None]]) -> None:
        """Register a callback invoked after all services start."""
        self._startup_hooks.append(callback)

    def on_shutdown(self, callback: Callable[[], Awaitable[None]]) -> None:
        """Register a callback invoked before services stop."""
        self._shutdown_hooks.append(callback)

    def on_connect(self, callback: Callable[[web.WebSocketResponse], Awaitable[None]]) -> None:
        """Register a callback invoked when a WebSocket client connects."""
        self._connect_hooks.append(callback)

    def on_disconnect(self, callback: Callable[[web.WebSocketResponse], Awaitable[None]]) -> None:
        """Register a callback invoked when a WebSocket client disconnects."""
        self._disconnect_hooks.append(callback)

    def on_auth(self, callback: Callable[[dict], Awaitable[dict]]) -> None:
        """Register a custom auth handler, overriding table-backed auth.

        The callback receives the raw ``data`` dict from the client's auth
        message and must return a dict with at least ``"user"`` and ``"role"``
        keys. Raise any exception to reject the auth attempt.
        """
        self._auth_handler = callback

    # -- Raw internals (unstable, available after start) --

    @property
    def db(self) -> Database | None:
        """The Database instance. Available after start(). Unstable API."""
        if self._aiohttp_app is None:
            return None
        return self._aiohttp_app.get("db")

    @property
    def writer(self) -> WriteBatcher | None:
        """The WriteBatcher instance. Available after start(). Unstable API."""
        if self._aiohttp_app is None:
            return None
        return self._aiohttp_app.get("writer")

    @property
    def change_bus(self) -> ChangeBus | None:
        """The ChangeBus instance. Available after start(). Unstable API."""
        if self._aiohttp_app is None:
            return None
        return self._aiohttp_app.get("bus")

    @property
    def services(self) -> dict[str, Service]:
        """Map of service name to Service instance. Available after start(). Unstable API."""
        if self._aiohttp_app is None:
            return {}
        return self._aiohttp_app.get("services", {})

    # -- Data facade --

    async def execute(
        self,
        service: str,
        data: dict[str, Any],
        *,
        op: str | None = None,
    ) -> dict[str, Any]:
        """Submit a transaction through the write path.

        Same semantics as a WS client sending a message — goes through
        WriteBatcher, publishes to ChangeBus, fans out to subscribers.

        Args:
            service: Name of a transaction service.
            data: Row data dict.
            op: Op name (required if the service has named ops).

        Returns:
            ``{"ok": True, "ref": "..."}``

        Raises:
            RuntimeError: If the server is not running.
            KeyError: If the service does not exist.
            ValueError: If the service is not a transaction service, or unknown op.
        """
        writer = self.writer
        if writer is None:
            raise RuntimeError("Server is not running")
        svc = self.services.get(service)
        if svc is None:
            raise KeyError(f"Unknown service: {service!r}")
        if svc.config.get("protocol") != "transaction":
            raise ValueError(
                f"Service {service!r} uses protocol {svc.config.get('protocol')!r}, "
                f"not 'transaction'"
            )
        from mkio.services.transaction import TransactionService, _extract_params
        assert isinstance(svc, TransactionService)
        msg = {"data": data}
        if op is not None:
            msg["op"] = op
        compiled_ops = svc._resolve_ops(msg)
        params_list = tuple(_extract_params(o, data) for o in compiled_ops)
        return await writer.submit(compiled_ops, params_list, data)

    async def query(
        self,
        sql: str,
        params: tuple[Any, ...] | dict[str, Any] = (),
    ) -> list[dict[str, Any]]:
        """Read query on the read connection.

        Args:
            sql: SQL query string.
            params: Query parameters (tuple for positional, dict for named).

        Returns:
            List of row dicts.

        Raises:
            RuntimeError: If the server is not running.
        """
        db = self.db
        if db is None:
            raise RuntimeError("Server is not running")
        return await db.read(sql, params)

    async def subscribe(
        self,
        tables: list[str],
        callback: Callable[[ChangeEvent], Awaitable[None]],
    ) -> Callable[[], None]:
        """Subscribe to change events on the given tables.

        Args:
            tables: List of table names to watch.
            callback: Async function called with each :class:`ChangeEvent`.

        Returns:
            An unsubscribe function. Call it to stop receiving events.

        Raises:
            RuntimeError: If the server is not running.
        """
        bus = self.change_bus
        if bus is None:
            raise RuntimeError("Server is not running")
        q = bus.subscribe(tables)

        async def _drain():
            try:
                while True:
                    event = await q.get()
                    await callback(event)
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(_drain())
        self._sub_tasks.append((task, tables))

        def unsub():
            task.cancel()
            bus.unsubscribe(tables, q)
            self._sub_tasks = [(t, tbl) for t, tbl in self._sub_tasks if t is not task]

        return unsub

    # -- Server lifecycle --

    async def start(self) -> None:
        """Start the server (non-blocking). Binds the port and begins serving."""
        if self._running:
            raise RuntimeError("Server is already running")

        cfg = self._config

        # Migration (sync, before the event loop owns the DB)
        db_path = cfg.get("db_path", "mkio.db")
        if db_path != ":memory:" and cfg.get("tables"):
            from mkio.database import Database
            db_pre = Database(path=db_path, tables=cfg["tables"], config=cfg)
            db_pre._run_migration()

        # Preflight service validation
        await _preflight_services(cfg)

        # Build aiohttp app
        app = web.Application()
        app["config"] = cfg
        app["mkio_app"] = self

        app.on_startup.append(_on_startup)
        app.on_shutdown.append(_on_shutdown)

        # Built-in routes
        app.router.add_get("/api/services", _api_services)
        app.router.add_get("/api/services/{service_name}", _api_service_detail)
        app.router.add_get("/ws", _ws_handler)
        app.router.add_get("/ws/{service_name}", _ws_handler)

        js_path = Path(__file__).parent / "client" / "mkio.js"
        if js_path.exists():
            async def serve_js(request: web.Request) -> web.FileResponse:
                return web.FileResponse(
                    js_path, headers={"Content-Type": "application/javascript"}
                )
            app.router.add_get("/mkio.js", serve_js)

        expr_js_path = Path(__file__).parent / "client" / "mkio-expr.mjs"
        if expr_js_path.exists():
            async def serve_expr_js(request: web.Request) -> web.FileResponse:
                return web.FileResponse(
                    expr_js_path, headers={"Content-Type": "application/javascript"}
                )
            app.router.add_get("/mkio-expr.js", serve_expr_js)

        # Config file routes
        for route, directory in cfg.get("config", {}).items():
            config_path = Path(directory).resolve()
            route_pattern = route.rstrip("/") + "/{path:.*}"
            app.router.add_get(route_pattern, _make_config_handler(config_path))

        # User-supplied routes
        _method_map = {
            "GET": app.router.add_get,
            "POST": app.router.add_post,
            "PUT": app.router.add_put,
            "DELETE": app.router.add_delete,
            "PATCH": app.router.add_patch,
            "HEAD": app.router.add_head,
            "OPTIONS": app.router.add_options,
        }
        for method, path, handler in self._pending_routes:
            adder = _method_map.get(method.upper())
            if adder is None:
                raise ValueError(f"Unsupported HTTP method: {method!r}")
            adder(path, handler)

        # Static file routes (after user routes so user can override)
        for route, directory in cfg.get("static", {}).items():
            path = Path(directory).resolve()
            if route == "/":
                app.router.add_get("/", _make_index_handler(path))
                app.router.add_static("/static", path)
            else:
                app.router.add_static(route, path)

        # Start via AppRunner for non-blocking lifecycle
        runner = web.AppRunner(app, shutdown_timeout=cfg.get("shutdown_timeout", 0))
        # Set _aiohttp_app before setup so startup hooks can access internals
        self._aiohttp_app = app
        try:
            await runner.setup()
        except Exception:
            self._aiohttp_app = None
            raise

        host = cfg.get("host", "0.0.0.0")
        port = cfg.get("port", 8080)
        site = web.TCPSite(runner, host, port)
        await site.start()

        self._runner = runner
        self._site = site
        self._stopped = asyncio.Event()
        self._running = True

    async def stop(self) -> None:
        """Graceful shutdown. Drains writes, closes connections, stops the DB."""
        if not self._running:
            return
        self._running = False

        # Cancel subscription drain tasks
        for task, tables in self._sub_tasks:
            task.cancel()
            if self.change_bus is not None:
                bus = self.change_bus
                # Queue ref not available here, but unsubscribe handles missing queues gracefully
        self._sub_tasks.clear()

        if self._runner:
            await self._runner.cleanup()
            self._runner = None
            self._site = None
        self._aiohttp_app = None
        if self._stopped:
            self._stopped.set()

    async def wait(self) -> None:
        """Block until the server stops (via :meth:`stop` or a signal)."""
        if self._stopped is None:
            raise RuntimeError("Server has not been started")
        await self._stopped.wait()

    def run(self) -> None:
        """Blocking convenience that starts the server and waits for shutdown.

        Handles SIGINT/SIGTERM for graceful shutdown. Tries uvloop if available.
        """
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            pass

        async def _run() -> None:
            loop = asyncio.get_running_loop()
            await self.start()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: asyncio.ensure_future(self.stop()))
            await self.wait()

        asyncio.run(_run())


def create_app(
    config: str | Path | dict[str, Any],
    *,
    routes: list[tuple[str, str, Callable]] | None = None,
) -> MkioApp:
    """Create an mkio application.

    Args:
        config: Path to a TOML file, or a config dict.
        routes: Optional list of ``(method, path, handler)`` tuples to register
                as additional HTTP routes.

    Returns:
        A :class:`MkioApp` instance ready to be started.

    Example::

        from mkio import create_app

        app = create_app("server.toml", routes=[
            ("GET", "/health", health_handler),
        ])
        app.run()
    """
    cfg = load_config(config)
    return MkioApp(cfg, routes=routes)
