"""Discovery helper for LLM agents — fetches mkio service descriptors as JSON.

Usage:
    python -m mkio.skill_helpers.discover <url>              # list all services
    python -m mkio.skill_helpers.discover <url> <service>    # full descriptor

Uses only stdlib (no aiohttp dependency) so it works in any Python 3.11+ environment.
"""

from __future__ import annotations

import json
import sys
import urllib.request
import urllib.error


def _fetch_json(url: str) -> dict | list:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def _normalize_http_url(url: str) -> str:
    url = url.rstrip("/")
    if url.startswith("ws://"):
        url = "http://" + url[5:]
    elif url.startswith("wss://"):
        url = "https://" + url[6:]
    elif not url.startswith("http://") and not url.startswith("https://"):
        url = "http://" + url
    # Strip /ws suffix if present
    if url.endswith("/ws"):
        url = url[:-3]
    return url


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print("Usage: python -m mkio.skill_helpers.discover <url> [service]")
        print()
        print("  <url>       mkio server URL (http, ws, or bare host:port)")
        print("  [service]   optional service name for full descriptor")
        sys.exit(1)

    base = _normalize_http_url(args[0])
    service_name = args[1] if len(args) > 1 else None

    try:
        if service_name:
            data = _fetch_json(f"{base}/api/services/{service_name}")
        else:
            data = _fetch_json(f"{base}/api/services")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(json.dumps({"error": f"Unknown service: {service_name}"}, indent=2))
        else:
            print(json.dumps({"error": f"HTTP {e.code}: {e.reason}"}, indent=2))
        sys.exit(1)
    except urllib.error.URLError as e:
        print(json.dumps({"error": f"Cannot connect to {base}: {e.reason}"}, indent=2))
        sys.exit(1)

    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
