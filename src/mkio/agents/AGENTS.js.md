# mkio JavaScript Client

## Load

Auto-served by any running mkio server — no CDN, no bundler:

```html
<script src="http://<host>:<port>/mkio.js"></script>
```

Or vendor the file from `src/mkio/client/mkio.js`. Works as a global (`window.MkioClient`) and as a CommonJS module.

## API

### Lifecycle

```javascript
const client = new MkioClient("ws://localhost:8080/ws", {
  reconnect: true,       // default: true
  backoffBase: 100,      // ms, default: 100
  backoffMax: 30000,     // ms, default: 30000
  onConnect: () => {},   // called on (re)connect
  onDisconnect: () => {},
});

await client.connect();
// ... use client ...
client.close();
```

### send(service, data, opts) -> Promise\<Object\>

Send a transaction and wait for the result.

```javascript
const result = await client.send("orders", {side: "Buy", symbol: "AAPL", qty: 100, price: 150.0}, {op: "new"});
```

- `opts.ref` — optional client ref (auto-generated if omitted via `MkioClient.makeRef()`)
- `opts.op` — named operation
- `opts.msgid` — optional correlation ID, echoed on result/error
- All opts are spread into the WebSocket message
- Returns: `{type: "result", ok: true, ref: "...", rows: [...]}`

**Note:** Unlike the Python client, the JS client auto-generates a `ref` if you don't provide one. The Python client leaves it to the server.

### subscribe(service, opts)

Subscribe with callbacks. No return value — messages arrive via callbacks.

```javascript
client.subscribe("all_orders", {
  filter: "status == 'pending'",
  ref: lastRef,  // optional, for delta recovery
  onSnapshot: (rows) => {
    console.log("Initial state:", rows.length, "rows");
  },
  onDelta: (changes) => {
    changes.forEach(c => console.log(c.op, c.row));
  },
  onUpdate: (op, row) => {
    console.log("Live:", op, row);
  },
});
```

- `filter` — expression string (only for services with `filterable` fields)
- `ref` — pass the last received ref to resume (delta recovery)
- The client tracks `ref` internally — on auto-reconnect, it re-subscribes with the last seen ref

### unsubscribe(service)

```javascript
client.unsubscribe("all_orders");
```

### check(service, ref) -> Promise\<Object\>

Verify a transaction committed after a disconnect.

```javascript
const result = await client.check("orders", "20260409 14:30:45.123456000000");
```

### MkioClient.makeRef() -> string

Generate a client-side ref string in the same `YYYYMMDD HH:mm:ss.mmmuuunnnppp` format as the server.

```javascript
const ref = MkioClient.makeRef();
```

## End-to-End Example

```html
<script src="/mkio.js"></script>
<script>
async function main() {
  // 1. Discover available services
  const services = await fetch("/api/services").then(r => r.json());
  console.log("Services:", services.map(s => s.name));

  // 2. Get detail for the transaction service
  const detail = await fetch("/api/services/orders").then(r => r.json());
  console.log("Ops:", Object.keys(detail.ops));

  // 3. Connect
  const client = new MkioClient(`ws://${location.host}/ws`);
  await client.connect();

  // 4. Send a transaction
  const result = await client.send("orders", {
    side: "Buy", symbol: "AAPL", qty: 100, price: 150.0,
  }, {op: "new"});
  console.log("Placed order, ref:", result.ref);

  // 5. Subscribe with live updates
  client.subscribe("all_orders", {
    onSnapshot: (rows) => renderTable(rows),
    onDelta: (changes) => applyChanges(changes),
    onUpdate: (op, row) => updateRow(op, row),
  });
}

main();
</script>
```

## Auto-Reconnect Behavior

The client reconnects automatically with exponential backoff when the WebSocket drops. On reconnect:

1. A new WebSocket connection is established
2. All active subscriptions are re-sent with the last received `ref`
3. The server responds with a delta (changes since that ref) or a full snapshot if the ref is too old
4. `onConnect` callback fires, then subscription callbacks resume
