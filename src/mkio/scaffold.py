"""Project scaffolding: init and default config."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any


def init(directory: str | Path = ".", *, no_static: bool = False) -> list[Path]:
    """Scaffold a new mkio project.

    Creates a server.toml config file and optionally a static/ directory
    with an index.html. Returns the list of created file paths.

    Args:
        directory: Target directory (created if it doesn't exist).
        no_static: If True, skip static/index.html and the [static] config.

    Raises:
        FileExistsError: If server.toml already exists in the target directory.
    """
    target = Path(directory)
    target.mkdir(parents=True, exist_ok=True)

    config_path = target / "server.toml"
    if config_path.exists():
        raise FileExistsError(f"{config_path} already exists")

    toml_content = _INIT_SERVER_TOML_BASE if no_static else _INIT_SERVER_TOML_BASE + _INIT_SERVER_TOML_STATIC
    config_path.write_text(toml_content)
    created = [config_path]

    if not no_static:
        static_dir = target / "static"
        static_dir.mkdir(exist_ok=True)
        index_path = static_dir / "index.html"
        index_path.write_text(_INIT_INDEX_HTML)
        created.append(index_path)

    return created


def get_default_config() -> dict[str, Any]:
    """Return the default project config as a Python dict.

    This is the same config that ``init()`` writes as server.toml,
    parsed into a dict so callers can modify it before passing to
    ``create_app()``.
    """
    toml_bytes = (_INIT_SERVER_TOML_BASE + _INIT_SERVER_TOML_STATIC).encode()
    return tomllib.loads(toml_bytes.decode())


_INIT_SERVER_TOML_BASE = """\
port = 8080
host = "0.0.0.0"
db_path = "data.db"
name = "my-app"
version = "1.0.0"
batch_max_size = 500
batch_max_wait_ms = 2.0
change_log_size = 10000
shutdown_timeout = 5
auto_migrate = "safe"
wal_checkpoint_interval_s = 300

# --- Database schema ---

[tables.items]
columns = { category = "TEXT NOT NULL DEFAULT 'general'", name = "TEXT NOT NULL", value = "TEXT DEFAULT ''" }
primary_key = ["category", "name"]

[tables.audit]
columns = { id = "INTEGER PRIMARY KEY AUTOINCREMENT", category = "TEXT", name = "TEXT", action = "TEXT" }

# --- Transaction service ---

[services.items]
protocol = "transaction"
description = "CRUD operations on items"
change_log_size = 5000

[services.items.descriptions]
add = "Create a new item"
save = "Insert or update an item"
update = "Update an existing item"
remove = "Delete an item"

[services.items.ops]
add = [
    { table = "items", op_type = "insert", fields = ["name", "category", "value"], defaults = { category = "general" } },
    { table = "audit", op_type = "insert", defaults = { action = "add" }, bind = { category = "$0.category", name = "$0.name" } },
]
save = [
    { table = "items", op_type = "upsert", key = ["category", "name"], fields = ["name", "category", "value"] },
    { table = "audit", op_type = "insert", defaults = { action = "save" }, bind = { category = "$0.category", name = "$0.name" } },
]
update = [
    { table = "items", op_type = "update", key = ["category", "name"], fields = ["value"] },
]
remove = [
    { table = "audit", op_type = "insert", fields = ["category", "name"], defaults = { action = "remove" } },
    { table = "items", op_type = "delete", key = ["category", "name"] },
]

# --- SubPub service ---

[services.item]
protocol = "subpub"
description = "Subscribe to a single item by name"
primary_table = "items"
watch_tables = ["items"]
topic = "name"
where = "category != 'hidden'"
change_log_size = 5000

[services.item.publish]
name = "name"
category = "category"
value = "value"

[services.item.defaults]
name = "''"
category = "'unknown'"
value = "''"

# --- Stream service ---

[services.feed]
protocol = "stream"
description = "Append-only activity log"
primary_table = "audit"
watch_tables = ["audit"]
sql = "SELECT audit.id, audit.action, audit.name, audit.category, audit._mkio_ref FROM audit"
buffer_size = 5000
filterable = ["action"]

[services.feed.publish]
id = "id"
action = "action"
name = "name"
category = "category"
_mkio_ref = "_mkio_ref"

# --- Query service ---

[services.all_items]
protocol = "query"
description = "Live query of all items"
primary_table = "items"
watch_tables = ["items"]
sql = "SELECT * FROM items WHERE category != 'hidden'"
where = "category != 'hidden'"
filterable = ["category"]
change_log_size = 5000

[services.all_items.publish]
category = "category"
name = "name"
value = "value"

# --- ReqRep services ---

[services.lookup]
protocol = "reqrep"
description = "Look up a single item by name"
sql = "SELECT * FROM items WHERE name = :name"

[services.summary]
protocol = "reqrep"
description = "Count items in a category"
params = { cat = "LOWER(category)" }
sql = "SELECT category, COUNT(*) as count FROM items WHERE category = :cat GROUP BY category"
reply = { category = "category", count = "count" }

[services.item_count]
protocol = "reqrep"
description = "Total number of items"
sql = "SELECT COUNT(*) as n FROM items"
reply = "COALESCE(n, 0)"

[services.calculate]
protocol = "reqrep"
description = "Pure computation, no database"
reply = { length = "LEN(value)", label = "UPPER(name)" }

"""

_INIT_SERVER_TOML_STATIC = """\
# --- Static file serving ---

[static]
"/" = "./static"
"""

_INIT_INDEX_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>mkio</title>
  <script src="/mkio.js"></script>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 600px; margin: 2rem auto; padding: 0 1rem; background: #0f172a; color: #e2e8f0; }
    h1 { color: #f1f5f9; }
    table { width: 100%; border-collapse: collapse; margin: 1rem 0; }
    th, td { border: 1px solid #334155; padding: 8px; text-align: left; }
    th { background: #1e293b; color: #94a3b8; font-weight: 600; }
    td:last-child, th:last-child { width: 1%; white-space: nowrap; }
    form { display: flex; gap: 0.5rem; margin: 1rem 0; }
    input { padding: 6px 10px; border: 1px solid #475569; border-radius: 4px; background: #1e293b; color: #e2e8f0; }
    button { padding: 6px 14px; background: #2563eb; color: white; border: none; border-radius: 4px; cursor: pointer; }
    button:hover { background: #3b82f6; }
    .btn-del { background: #dc2626; font-size: 0.75rem; padding: 3px 8px; }
    .btn-del:hover { background: #ef4444; }
    #status { font-size: 0.875rem; }
    .connected { color: #6ee7b7; }
    .disconnected { color: #fca5a5; }
    .error { background: #450a0a; color: #fca5a5; border: 1px solid #dc2626; border-radius: 4px; padding: 8px 12px; margin: 0.5rem 0; font-size: 0.875rem; }
  </style>
</head>
<body>
  <h1>mkio</h1>
  <p id="status" class="disconnected">Disconnected</p>

  <div id="error"></div>
  <form id="add-form">
    <input name="category" list="categories" placeholder="Category" autocomplete="off">
    <datalist id="categories"></datalist>
    <input name="name" placeholder="Name" required autocomplete="off">
    <input name="value" placeholder="Value" autocomplete="off">
    <button type="submit">Add</button>
  </form>

  <table>
    <thead><tr><th>Category</th><th>Name</th><th>Value</th><th></th></tr></thead>
    <tbody id="items"></tbody>
  </table>

  <script>
    const statusEl = document.getElementById('status');
    const client = new MkioClient(`ws://${location.host}/ws`, {
      onConnect() { statusEl.textContent = 'Connected'; statusEl.className = 'connected'; },
      onDisconnect() { statusEl.textContent = 'Disconnected'; statusEl.className = 'disconnected'; },
    });

    const errorEl = document.getElementById('error');
    function showError(msg) { errorEl.textContent = msg; errorEl.className = 'error'; setTimeout(() => { errorEl.textContent = ''; errorEl.className = ''; }, 5000); }

    const items = new Map();
    const tbody = document.getElementById('items');
    const catList = document.getElementById('categories');

    function rowKey(row) { return row._mkio_row || (row.category + '\\0' + row.name); }

    function render() {
      tbody.innerHTML = '';
      for (const row of items.values()) {
        const tr = document.createElement('tr');
        const cat = row.category.replace(/'/g, "\\\\'");
        const name = row.name.replace(/'/g, "\\\\'");
        tr.innerHTML = `<td>${row.category}</td><td>${row.name}</td><td>${row.value}</td><td><button class="btn-del" onclick="removeItem('${cat}','${name}')">x</button></td>`;
        tbody.appendChild(tr);
      }
      const cats = [...new Set([...items.values()].map(r => r.category).filter(Boolean))].sort();
      catList.innerHTML = cats.map(c => `<option value="${c}">`).join('');
    }

    async function removeItem(category, name) {
      const res = await client.send('items', { category, name }, { op: 'remove' });
      if (res.type === 'error') showError(res.message);
    }

    client.connect().then(() => {
      client.subscribe('all_items', 'query', {
        onSnapshot(rows) { items.clear(); rows.forEach(r => items.set(rowKey(r), r)); render(); },
        onDelta(changes) { changes.forEach(c => { if (c.op === 'delete') items.delete(rowKey(c.row)); else items.set(rowKey(c.row), c.row); }); render(); },
        onUpdate(op, row) { if (op === 'delete') items.delete(rowKey(row)); else items.set(rowKey(row), row); render(); },
      });
    });

    document.getElementById('add-form').addEventListener('submit', async (e) => {
      e.preventDefault();
      const form = e.target;
      const data = { name: form.name.value, value: form.value.value };
      if (form.category.value) data.category = form.category.value;
      const res = await client.send('items', data, { op: 'save' });
      if (res.type === 'error') { showError(res.message); return; }
      form.reset();
      setTimeout(() => form.category.focus(), 0);
    });
  </script>
</body>
</html>
"""
