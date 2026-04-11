---
name: mkio
description: Use when the project integrates with an mkio server — sending transactions, subscribing to live data, or using MkioClient. Triggers on mentions of mkio, MkioClient, /api/services, or WebSocket transaction/subpub/stream/query services.
---

# mkio Integration

## Step 1: Read the protocol guide

Find the mkio agents docs inside the installed package:

```bash
python -c "import mkio, os; print(os.path.join(os.path.dirname(mkio.__file__), 'agents'))"
```

Read `AGENTS.md` in that directory for the universal protocol (WebSocket envelope, refs, service types, discovery).

## Step 2: Read the language-specific client guide

- **Python projects:** read `AGENTS.python.md` in the same directory
- **JS/TS projects:** read `AGENTS.js.md` in the same directory
- **Both:** read both

## Step 3: Discover services on the running instance

Before calling any service, discover its schema:

```bash
python -m mkio.skill_helpers.discover <mkio-url>              # list all services
python -m mkio.skill_helpers.discover <mkio-url> <service>    # full descriptor
```

Never guess field names — always discover first.
