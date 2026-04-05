# mkio

Config-driven Python microservice framework. Single TCP port serves HTTP + WebSocket, backed by embedded SQLite.

## Build & Test

```bash
pip install -e ".[dev]"        # Install with dev dependencies
pip install -e ".[fast,dev]"   # With orjson + uvloop acceleration
pytest tests/                  # Run all tests (132 tests)
pytest tests/ -x -v            # Stop on first failure, verbose
```

## Architecture

```
src/mkio/
├── _json.py          # orjson-with-fallback (dumps -> bytes, loads)
├── _version.py       # "YYYYMMDD HH:mm:ss.mmmuuunnnppp" version strings
├── _expr.py          # Expression language: tokenizer, parser, evaluator
├── config.py         # TOML/dict loader, normalization, validation
├── migration.py      # Schema diff, change classification, data preservation
├── database.py       # Dual aiosqlite connections (write + read), WAL mode
├── change_bus.py     # Async broadcast with pre-serialized bytes
├── writer.py         # Write batcher: SAVEPOINTs, single commit per batch
├── ws_protocol.py    # JSON envelope helpers
├── server.py         # aiohttp wiring, WS dispatch, static serving, monitor protocol
├── services/
│   ├── base.py       # Service base class with monitor notification
│   ├── transaction.py # Config-driven SQL ops + result cache
│   ├── subpub.py     # In-memory cache + live push + delta reconnect
│   ├── stream.py     # Ring buffer + version-based cursor
│   └── query.py      # SQLite snapshot + change feed
└── client/
    ├── __init__.py   # Python client with auto-reconnect
    └── mkio.js       # JS client, auto-served at /mkio.js
```

## Key Patterns

- **Write path**: TransactionService -> WriteBatcher queue -> batch with SAVEPOINTs -> single COMMIT -> ChangeBus publish -> fan out to subscribers
- **In-memory databases** use shared-cache URI (`file:mkio_{uuid}?mode=memory&cache=shared`) so write and read connections see the same data
- **Expression language** is parsed once (at subscribe/startup), evaluated per row via AST walk
- **Version strings** are lexicographically sortable UTC timestamps with sub-nanosecond counter for uniqueness
- **Schema migration** uses recreate-table strategy for changes SQLite's ALTER TABLE can't handle

## Conventions

- Python 3.11+ required (for `asyncio.TaskGroup`, `tomllib`)
- All async tests use `pytest-asyncio` with `asyncio_mode = "auto"`
- `from mkio._json import dumps, loads` everywhere (never raw json/orjson)
- Services communicate changes via `ChangeBus` (never direct DB polling)
- **Monitor protocol**: WS clients send `{"type": "monitor", "service": "..."}` to tap into a service's inbound/outbound message flow. `GET /api/services` lists available services.
- **CLI tools**: `mkio services <url>` lists services, `mkio monitor <url> <service>` streams live traffic
