"""MkioApp: programmatic server lifecycle."""

from __future__ import annotations

import asyncio
import signal
import sys
from pathlib import Path
from typing import Any, Callable

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
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._stopped: asyncio.Event | None = None
        self._running = False

    @property
    def config(self) -> dict[str, Any]:
        """The resolved config dict."""
        return self._config

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
        await runner.setup()

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
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
            self._site = None
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
