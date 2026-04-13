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

### subscribe(service, filter=None, ref=None) -> AsyncIterator[dict]

Subscribe and yield messages (snapshot, delta, update).

```python
async for msg in client.subscribe("all_orders", filter="status == 'pending'"):
    if msg["type"] == "snapshot":
        rows = msg["rows"]
    elif msg["type"] == "update":
        op, row = msg["op"], msg["row"]
```

- `filter` — expression string (only for services with `filterable` fields)
- `ref` — pass the last received `ref` to resume from that point (delta recovery)
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
2. All active subscriptions are re-sent with the last received `ref`
3. The server responds with a delta (changes since that ref) or a full snapshot if the ref is too old

No application code needed — just iterate the async generator and it resumes.
