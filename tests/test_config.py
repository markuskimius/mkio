"""Tests for config loader."""

import tempfile
from pathlib import Path

import pytest
from mkio.config import load_config


MINIMAL_TOML = """\
port = 9000
host = "127.0.0.1"

[tables.items]
columns = { id = "TEXT PRIMARY KEY", name = "TEXT" }

[services.add_item]
type = "transaction"
table = "items"
op_type = "insert"
fields = ["id", "name"]
"""


def test_load_from_toml():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        f.write(MINIMAL_TOML)
        f.flush()
        config = load_config(f.name)
    assert config["port"] == 9000
    assert config["host"] == "127.0.0.1"
    assert "items" in config["tables"]


def test_load_from_dict():
    config = load_config({
        "port": 8080,
        "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
    })
    assert config["port"] == 8080
    assert config["db_path"] == "mkio.db"  # default


def test_defaults_applied():
    config = load_config({})
    assert config["host"] == "0.0.0.0"
    assert config["port"] == 8080
    assert config["batch_max_size"] == 500
    assert config["batch_max_wait_ms"] == 2.0
    assert config["db_path"] == "mkio.db"
    assert config["change_log_size"] == 10000


def test_single_table_transaction_normalization():
    config = load_config({
        "tables": {"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "INTEGER"}}},
        "services": {
            "add_order": {
                "type": "transaction",
                "table": "orders",
                "op_type": "insert",
                "fields": ["id", "qty"],
            }
        },
    })
    svc = config["services"]["add_order"]
    assert "ops" in svc
    assert len(svc["ops"]) == 1
    assert svc["ops"][0]["table"] == "orders"
    assert svc["ops"][0]["op_type"] == "insert"
    assert "table" not in svc  # removed from top level


def test_watch_tables_normalization():
    config = load_config({
        "tables": {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}},
        "services": {
            "live": {
                "type": "subpub",
                "primary_table": "orders",
                "key": "id",
            }
        },
    })
    svc = config["services"]["live"]
    assert svc["watch_tables"] == ["orders"]


def test_filterable_validation_strict():
    with pytest.raises(ValueError, match="filterable field 'bogus'"):
        load_config({
            "tables": {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {
                "live": {
                    "type": "query",
                    "primary_table": "orders",
                    "filterable": ["bogus"],
                }
            },
        })


def test_filterable_validation_skipped_for_sql():
    # Should not raise — when SQL is provided, can't validate aliases
    config = load_config({
        "tables": {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}},
        "services": {
            "live": {
                "type": "query",
                "primary_table": "orders",
                "sql": "SELECT id, id || '-alias' as computed FROM orders",
                "filterable": ["computed"],
            }
        },
    })
    assert "computed" in config["services"]["live"]["filterable"]


def test_publish_compiled():
    config = load_config({
        "tables": {"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "INTEGER"}}},
        "services": {
            "live": {
                "type": "subpub",
                "primary_table": "orders",
                "key": "id",
                "publish": {"double_qty": "qty * 2"},
            }
        },
    })
    svc = config["services"]["live"]
    assert "_compiled_formatter" in svc
    assert callable(svc["_compiled_formatter"])
    result = svc["_compiled_formatter"]({"qty": 5})
    assert result == {"double_qty": 10}
