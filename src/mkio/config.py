"""Config loader: TOML or dict, validation, normalization."""

from __future__ import annotations

import difflib
import logging
import tomllib
from pathlib import Path
from typing import Any

from mkio._expr import compile_filter, compile_formatter


log = logging.getLogger("mkio.config")

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

_VALID_TOP_LEVEL_KEYS = frozenset(
    set(_DEFAULTS.keys()) | {"tables", "services", "static", "shutdown_timeout"}
)

_VALID_SERVICE_KEYS: dict[str, frozenset[str]] = {
    "transaction": frozenset({
        "protocol", "ops", "table", "op_type", "key", "fields",
        "defaults", "change_log_size", "description", "descriptions",
    }),
    "subpub": frozenset({
        "protocol", "primary_table", "watch_tables", "topic", "sql",
        "where", "publish", "defaults", "change_log_size", "description",
    }),
    "stream": frozenset({
        "protocol", "primary_table", "watch_tables", "sql",
        "publish", "filterable", "buffer_size", "description",
    }),
    "query": frozenset({
        "protocol", "primary_table", "watch_tables", "sql",
        "publish", "filterable", "where", "change_log_size", "description",
    }),
}

_VALID_PROTOCOLS = frozenset({"transaction", "subpub", "stream", "query"})


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

    # Warn about unknown top-level keys
    _warn_unknown_keys(
        "top-level config", config, _VALID_TOP_LEVEL_KEYS,
        exclude_prefixes=("_",),
    )

    # Normalize and validate services
    for svc_name, svc_config in config["services"].items():
        _normalize_service(svc_name, svc_config, config)

    return config


def _warn_unknown_keys(
    context: str,
    obj: dict[str, Any],
    valid_keys: frozenset[str],
    *,
    exclude_prefixes: tuple[str, ...] = (),
) -> None:
    """Log warnings for unrecognized keys, suggesting close matches."""
    for key in obj:
        if any(key.startswith(p) for p in exclude_prefixes):
            continue
        if key not in valid_keys:
            close = difflib.get_close_matches(key, valid_keys, n=1, cutoff=0.6)
            hint = f" (did you mean {close[0]!r}?)" if close else ""
            log.warning(f"Unknown key in {context}: {key!r}{hint}")


def _normalize_service(
    name: str, svc: dict[str, Any], config: dict[str, Any]
) -> None:
    """Normalize a single service config."""
    svc_type = svc.get("protocol", "")

    # Validate protocol
    if not svc_type:
        raise ValueError(
            f"Service '{name}': missing required 'protocol' field. "
            f"Valid protocols: {', '.join(sorted(_VALID_PROTOCOLS))}"
        )
    if svc_type not in _VALID_PROTOCOLS and not isinstance(svc_type, type) and "." not in str(svc_type):
        close = difflib.get_close_matches(str(svc_type), _VALID_PROTOCOLS, n=1, cutoff=0.5)
        hint = f" Did you mean {close[0]!r}?" if close else ""
        raise ValueError(
            f"Service '{name}': unknown protocol {svc_type!r}. "
            f"Valid protocols: {', '.join(sorted(_VALID_PROTOCOLS))}.{hint}"
        )

    # Warn about unknown service config keys
    if svc_type in _VALID_SERVICE_KEYS:
        _warn_unknown_keys(
            f"service '{name}'", svc, _VALID_SERVICE_KEYS[svc_type],
            exclude_prefixes=("_",),
        )

    # Transaction shorthand: single-table -> ops list
    if svc_type == "transaction":
        if "ops" not in svc and "table" in svc:
            svc["ops"] = [{
                "table": svc.pop("table"),
                "op_type": svc.pop("op_type"),
                "key": svc.pop("key", []),
                "fields": svc.pop("fields", []),
            }]

    # Validate required fields per protocol
    if svc_type in ("subpub", "query", "stream"):
        if "primary_table" not in svc:
            raise ValueError(
                f"Service '{name}' ({svc_type}): missing required 'primary_table' field"
            )
    if svc_type == "subpub":
        if "topic" not in svc:
            raise ValueError(
                f"Service '{name}' (subpub): missing required 'topic' field"
            )
    if svc_type == "transaction":
        if "ops" not in svc:
            raise ValueError(
                f"Service '{name}' (transaction): missing 'ops' — "
                f"provide either 'ops' or shorthand 'table'+'op_type' fields"
            )

    # Normalize watch_tables
    if "primary_table" in svc and "watch_tables" not in svc:
        svc["watch_tables"] = [svc["primary_table"]]

    # Validate table references exist in config
    tables = config.get("tables", {})
    _validate_table_refs(name, svc, tables)

    # Validate topic field exists in table columns
    if svc_type == "subpub" and "sql" not in svc:
        _validate_topic_field(name, svc, tables)

    # Validate transaction ops
    if svc_type == "transaction":
        _validate_transaction_ops(name, svc, tables)

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


def _validate_table_refs(
    name: str, svc: dict[str, Any], tables: dict[str, Any]
) -> None:
    """Validate that all referenced tables exist in the config."""
    # Check primary_table
    primary = svc.get("primary_table")
    if primary and primary not in tables:
        available = ", ".join(sorted(tables.keys())) if tables else "(none defined)"
        raise ValueError(
            f"Service '{name}': primary_table {primary!r} "
            f"not found in [tables]. Available tables: {available}"
        )

    # Check watch_tables
    for wt in svc.get("watch_tables", []):
        if wt not in tables:
            available = ", ".join(sorted(tables.keys())) if tables else "(none defined)"
            raise ValueError(
                f"Service '{name}': watch_tables entry {wt!r} "
                f"not found in [tables]. Available tables: {available}"
            )

    # Check transaction op tables
    ops = svc.get("ops", [])
    op_list = ops if isinstance(ops, list) else [op for ops_list in ops.values() for op in ops_list]
    for spec in op_list:
        t = spec.get("table", "")
        if t and t not in tables:
            available = ", ".join(sorted(tables.keys())) if tables else "(none defined)"
            raise ValueError(
                f"Service '{name}': op references table {t!r} "
                f"not found in [tables]. Available tables: {available}"
            )


def _validate_topic_field(
    name: str, svc: dict[str, Any], tables: dict[str, Any]
) -> None:
    """Validate that the topic field exists in the primary table."""
    topic = svc.get("topic")
    primary = svc.get("primary_table")
    if not topic or not primary:
        return
    table_config = tables.get(primary, {})
    columns = set(table_config.get("columns", {}).keys())
    if columns and topic not in columns:
        close = difflib.get_close_matches(topic, columns, n=1, cutoff=0.6)
        hint = f" Did you mean {close[0]!r}?" if close else ""
        raise ValueError(
            f"Service '{name}': topic field {topic!r} "
            f"not found in table {primary!r}. "
            f"Available columns: {', '.join(sorted(columns))}.{hint}"
        )


def _validate_transaction_ops(
    name: str, svc: dict[str, Any], tables: dict[str, Any]
) -> None:
    """Validate transaction op specs against table schemas."""
    ops = svc.get("ops", [])
    if isinstance(ops, dict):
        all_op_sets = list(ops.items())
    else:
        all_op_sets = [("default", ops)]

    valid_op_types = ("insert", "update", "delete", "upsert")

    for op_name, op_list in all_op_sets:
        if not isinstance(op_list, list):
            raise ValueError(
                f"Service '{name}': op {op_name!r} must be a list of operation specs"
            )
        for i, spec in enumerate(op_list):
            table = spec.get("table")
            op_type = spec.get("op_type")
            step_label = f"service '{name}', op '{op_name}', step {i}"

            if not table:
                raise ValueError(f"{step_label}: missing 'table' field")
            if not op_type:
                raise ValueError(f"{step_label}: missing 'op_type' field")
            if op_type not in valid_op_types:
                raise ValueError(
                    f"{step_label}: unknown op_type {op_type!r}. "
                    f"Valid types: {', '.join(valid_op_types)}"
                )

            # Validate fields and key against table columns (skip if table not in config)
            table_config = tables.get(table, {})
            columns = set(table_config.get("columns", {}).keys())
            if not columns:
                continue

            for field in spec.get("fields", []):
                if field not in columns:
                    close = difflib.get_close_matches(field, columns, n=1, cutoff=0.6)
                    hint = f" Did you mean {close[0]!r}?" if close else ""
                    raise ValueError(
                        f"{step_label}: field {field!r} not found in table "
                        f"{table!r}. Available columns: "
                        f"{', '.join(sorted(columns))}.{hint}"
                    )
            for key_field in spec.get("key", []):
                if key_field not in columns:
                    close = difflib.get_close_matches(key_field, columns, n=1, cutoff=0.6)
                    hint = f" Did you mean {close[0]!r}?" if close else ""
                    raise ValueError(
                        f"{step_label}: key field {key_field!r} not found in "
                        f"table {table!r}. Available columns: "
                        f"{', '.join(sorted(columns))}.{hint}"
                    )

            # Validate bind references
            bind = spec.get("bind", {})
            for col, ref in bind.items():
                if col not in columns:
                    raise ValueError(
                        f"{step_label}: bind column {col!r} not found in "
                        f"table {table!r}. Available columns: "
                        f"{', '.join(sorted(columns))}"
                    )
                if isinstance(ref, str) and ref.startswith("$"):
                    idx_str, _, _ = ref[1:].partition(".")
                    try:
                        ref_idx = int(idx_str)
                    except ValueError:
                        raise ValueError(
                            f"{step_label}: invalid bind reference {ref!r} — "
                            f"index must be an integer (format: '$N.field')"
                        )
                    if ref_idx >= len(op_list):
                        raise ValueError(
                            f"{step_label}: bind reference {ref!r} refers to "
                            f"op index {ref_idx}, but there are only "
                            f"{len(op_list)} ops (indices 0–{len(op_list) - 1})"
                        )
                    if ref_idx >= i:
                        raise ValueError(
                            f"{step_label}: bind reference {ref!r} refers to "
                            f"op index {ref_idx}, but binds can only reference "
                            f"earlier ops (indices 0–{i - 1})"
                        )

            # Validate defaults reference valid columns
            for def_col in spec.get("defaults", {}):
                if def_col not in columns:
                    close = difflib.get_close_matches(def_col, columns, n=1, cutoff=0.6)
                    hint = f" Did you mean {close[0]!r}?" if close else ""
                    raise ValueError(
                        f"{step_label}: defaults column {def_col!r} not found "
                        f"in table {table!r}. Available columns: "
                        f"{', '.join(sorted(columns))}.{hint}"
                    )

            # Validate key required for update/delete/upsert
            if op_type in ("update", "delete", "upsert") and not spec.get("key"):
                raise ValueError(
                    f"{step_label}: op_type {op_type!r} requires a 'key' "
                    f"field to identify which rows to {op_type}"
                )


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
            close = difflib.get_close_matches(field_name, all_columns, n=1, cutoff=0.6)
            hint = f" Did you mean {close[0]!r}?" if close else ""
            raise ValueError(
                f"Service '{name}': filterable field '{field_name}' "
                f"not found in tables {watch_tables}. "
                f"Available columns: {', '.join(sorted(all_columns))}.{hint}"
            )
