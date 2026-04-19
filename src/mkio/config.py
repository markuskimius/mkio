"""Config loader: TOML or dict, validation, normalization."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from mkio._expr import compile_filter, compile_formatter


_DEFAULTS = {
    "host": "0.0.0.0",
    "port": 8080,
    "db_path": "mkio.db",
    "batch_max_size": 500,
    "batch_max_wait_ms": 2.0,
    "change_log_size": 10000,
    "wal_checkpoint_interval_s": 300,
    "auto_migrate": False,
    "backup_on_startup": False,
    "backup_dir": "./backups",
}


def load_config(source: str | Path | dict[str, Any]) -> dict[str, Any]:
    """Load and normalize a config from a TOML file path or dict."""
    if isinstance(source, (str, Path)):
        path = Path(source)
        with open(path, "rb") as f:
            config = tomllib.load(f)
    elif isinstance(source, dict):
        config = dict(source)  # shallow copy
    else:
        raise TypeError(f"Config must be a path or dict, got {type(source)}")

    # Apply defaults
    for key, default in _DEFAULTS.items():
        config.setdefault(key, default)

    config.setdefault("tables", {})
    config.setdefault("services", {})
    config.setdefault("static", {})

    # Normalize services
    for svc_name, svc_config in config["services"].items():
        _normalize_service(svc_name, svc_config, config)

    return config


def _normalize_service(
    name: str, svc: dict[str, Any], config: dict[str, Any]
) -> None:
    """Normalize a single service config."""
    svc_type = svc.get("type", "")

    # Transaction shorthand: single-table -> ops list
    if svc_type == "transaction":
        if "ops" not in svc and "table" in svc:
            svc["ops"] = [{
                "table": svc.pop("table"),
                "op_type": svc.pop("op_type"),
                "key": svc.pop("key", []),
                "fields": svc.pop("fields", []),
            }]

    # Normalize watch_tables
    if "primary_table" in svc and "watch_tables" not in svc:
        svc["watch_tables"] = [svc["primary_table"]]

    # Default change_log_size from global config
    if svc_type in ("subpub", "query"):
        svc.setdefault("change_log_size", config.get("change_log_size", 10000))

    # Default buffer_size for stream
    if svc_type == "stream":
        svc.setdefault("buffer_size", 10000)

    # Compile publish formatter
    if "publish" in svc:
        svc["_compiled_formatter"] = compile_formatter(svc["publish"])

    # Compile where filter
    if "where" in svc:
        svc["_compiled_where"] = compile_filter(svc["where"])

    # Compile defaults as expressions (subpub not-found template)
    if "defaults" in svc and svc_type == "subpub":
        raw = svc["defaults"]
        svc["_compiled_defaults"] = compile_formatter(
            {k: str(v) if not isinstance(v, str) else v for k, v in raw.items()}
        )

    # Validate filterable fields (query only — subpub uses topic instead)
    if "filterable" in svc and svc_type != "subpub":
        _validate_filterable(name, svc, config)


def _validate_filterable(
    name: str, svc: dict[str, Any], config: dict[str, Any]
) -> None:
    """Validate that filterable fields exist in the table schema."""
    filterable = svc.get("filterable", [])
    if not filterable:
        return

    # Collect all column names from watched tables
    watch_tables = svc.get("watch_tables", [])
    if not watch_tables and "primary_table" in svc:
        watch_tables = [svc["primary_table"]]

    all_columns: set[str] = set()
    tables = config.get("tables", {})
    for table_name in watch_tables:
        table_config = tables.get(table_name, {})
        all_columns.update(table_config.get("columns", {}).keys())

    # Also allow SQL aliases — if the service has a custom SQL, we can't
    # fully validate column names, so skip strict validation in that case
    if "sql" in svc:
        return

    for field_name in filterable:
        if field_name not in all_columns:
            raise ValueError(
                f"Service '{name}': filterable field '{field_name}' "
                f"not found in tables {watch_tables}"
            )
