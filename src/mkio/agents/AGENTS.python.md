# mkio Python Client

## Install

```bash
pip install mkio
```

## Import

```python
from mkio.client import MkioClient
```

## API

### Lifecycle

```python
# Context manager (recommended)
async with MkioClient("ws://localhost:8080/ws") as client:
    ...

# Manual
client = MkioClient("ws://localhost:8080/ws", reconnect=True)
await client.connect()
...
await client.close()
```

Constructor options: `reconnect` (bool, default True), `backoff_base` (float, default 0.1s), `backoff_max` (float, default 1.0s).

### send(service, data, ref=None, **kwargs) -> dict

Send a transaction and wait for the result.

```python
result = await client.send("orders", {"side": "Buy", "symbol": "AAPL", "qty": 100, "price": 150.0}, op="new")
```

- `op="name"` — selects a named operation (passed as kwarg)
- `ref="..."` — optional client ref; if omitted, server assigns one
- `msgid="..."` — optional correlation ID, echoed on result/error
- Returns the full result dict: `{"type": "result", "ok": True, "ref": "...", "rows": [...]}`

### subscribe(service, topic=None, filter=None, ref=None, subid=None, snapshot=True, updates=True, fields=None) -> AsyncIterator[dict]

Subscribe and yield messages (snapshot, update).

```python
# SubPub — topic required, returns single row with _mkio_exists
async for msg in client.subscribe("last_trade", topic="AAPL"):
    if msg["type"] == "snapshot":
        row = msg["rows"][0]  # always one row
        print(row["_mkio_exists"])  # True or False
    elif msg["type"] == "update":
        row = msg["row"]

# Query — with filter
async for msg in client.subscribe("all_orders", filter="status == 'pending'", fields=["symbol", "qty"]):
    if msg["type"] == "snapshot":
        rows = msg["rows"]
    elif msg["type"] == "update":
        op, row = msg["op"], msg["row"]
```

- `topic` — (subpub only, required) the key column value to subscribe to
- `filter` — (query only) expression string, only for services with `filterable` fields
- `fields` — list of field names to include in each row (omit for all fields)
- `ref` — pass the last received `ref` to resume from that point (stream only, required for streams)
- `subid` — optional string echoed on every response for this subscription (for multiplexing)
- `snapshot` — set to False to skip the initial snapshot (query only; subpub always sends both)
- `updates` — set to False to receive only the snapshot then stop (query only)
- The client tracks `ref` internally — on auto-reconnect, it re-subscribes with the last seen ref

### check(service, ref) -> dict

Verify a transaction committed after a disconnect.

```python
result = await client.check("orders", ref="20260409 14:30:45.123456000000")
if result.get("ok"):
    print("Transaction committed")
```

## End-to-End Example

```python
import asyncio
import aiohttp
from mkio.client import MkioClient

async def main():
    base_url = "http://localhost:8080"

    # 1. Discover available services
    async with aiohttp.ClientSession() as http:
        async with http.get(f"{base_url}/api/services") as resp:
            services = await resp.json()
            print("Services:", [s["name"] for s in services])

        # 2. Get detail for the transaction service
        async with http.get(f"{base_url}/api/services/orders") as resp:
            detail = await resp.json()
            print("Ops:", list(detail["ops"].keys()))

    # 3. Connect and send a transaction
    async with MkioClient("ws://localhost:8080/ws") as client:
        result = await client.send(
            "orders",
            {"side": "Buy", "symbol": "AAPL", "qty": 100, "price": 150.0},
            op="new",
        )
        ref = result["ref"]
        print(f"Placed order, ref={ref}")

        # 4. Subscribe with ref tracking for recovery
        async for msg in client.subscribe("all_orders"):
            print(msg["type"], msg.get("ref"))
            # On reconnect, the client automatically resumes from last ref

asyncio.run(main())
```

## Auto-Reconnect Behavior

The client reconnects automatically with exponential backoff when the WebSocket drops. On reconnect:

1. A new WebSocket connection is established
2. All active subscriptions are re-sent with their stored state (ref, topic, filter, fields, etc.)
3. Stream services resume from the last ref; subpub and query always send a full snapshot

No application code needed — just iterate the async generator and it resumes.
