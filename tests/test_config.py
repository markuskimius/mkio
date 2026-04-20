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
protocol = "transaction"
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
                "protocol": "transaction",
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
                "protocol": "subpub",
                "primary_table": "orders",
                "topic": "id",
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
                    "protocol": "query",
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
                "protocol": "query",
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
                "protocol": "subpub",
                "primary_table": "orders",
                "topic": "id",
                "publish": {"double_qty": "qty * 2"},
            }
        },
    })
    svc = config["services"]["live"]
    assert "_compiled_formatter" in svc
    assert callable(svc["_compiled_formatter"])
    result = svc["_compiled_formatter"]({"qty": 5})
    assert result == {"double_qty": 10}


# ---- Validation tests --------------------------------------------------------


def test_missing_protocol():
    with pytest.raises(ValueError, match="missing required 'protocol'"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {"svc": {"primary_table": "t"}},
        })


def test_invalid_protocol():
    with pytest.raises(ValueError, match="unknown protocol 'streem'"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {"svc": {"protocol": "streem", "primary_table": "t"}},
        })


def test_missing_primary_table():
    with pytest.raises(ValueError, match="missing required 'primary_table'"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {"svc": {"protocol": "subpub", "topic": "id"}},
        })


def test_missing_topic():
    with pytest.raises(ValueError, match="missing required 'topic'"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {"svc": {"protocol": "subpub", "primary_table": "t"}},
        })


def test_missing_ops():
    with pytest.raises(ValueError, match="missing 'ops'"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {"svc": {"protocol": "transaction"}},
        })


def test_nonexistent_primary_table():
    with pytest.raises(ValueError, match="primary_table 'bogus'.*not found"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {
                "svc": {"protocol": "subpub", "primary_table": "bogus", "topic": "id"},
            },
        })


def test_nonexistent_watch_table():
    with pytest.raises(ValueError, match="watch_tables entry 'bogus'.*not found"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {
                "svc": {
                    "protocol": "query",
                    "primary_table": "t",
                    "watch_tables": ["t", "bogus"],
                },
            },
        })


def test_nonexistent_op_table():
    with pytest.raises(ValueError, match="op references table 'bogus'.*not found"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {
                "svc": {
                    "protocol": "transaction",
                    "ops": [{"table": "bogus", "op_type": "insert", "fields": ["id"]}],
                },
            },
        })


def test_invalid_op_type():
    with pytest.raises(ValueError, match="unknown op_type 'inset'"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {
                "svc": {
                    "protocol": "transaction",
                    "ops": [{"table": "t", "op_type": "inset", "fields": ["id"]}],
                },
            },
        })


def test_nonexistent_field_in_op():
    with pytest.raises(ValueError, match="field 'bogus' not found in table 't'"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {
                "svc": {
                    "protocol": "transaction",
                    "ops": [{"table": "t", "op_type": "insert", "fields": ["bogus"]}],
                },
            },
        })


def test_nonexistent_key_field_in_op():
    with pytest.raises(ValueError, match="key field 'bogus' not found"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY", "name": "TEXT"}}},
            "services": {
                "svc": {
                    "protocol": "transaction",
                    "ops": [{"table": "t", "op_type": "update", "key": ["bogus"], "fields": ["name"]}],
                },
            },
        })


def test_topic_field_not_in_table():
    with pytest.raises(ValueError, match="topic field 'bogus'.*not found in table"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {
                "svc": {"protocol": "subpub", "primary_table": "t", "topic": "bogus"},
            },
        })


def test_topic_field_skipped_with_sql():
    config = load_config({
        "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
        "services": {
            "svc": {
                "protocol": "subpub",
                "primary_table": "t",
                "topic": "computed",
                "sql": "SELECT id as computed FROM t",
            },
        },
    })
    assert config["services"]["svc"]["topic"] == "computed"


def test_bind_ref_forward_reference():
    with pytest.raises(ValueError, match="bind reference.*refers to op index 1.*only reference earlier"):
        load_config({
            "tables": {
                "a": {"columns": {"id": "INTEGER PRIMARY KEY", "val": "TEXT"}},
                "b": {"columns": {"id": "INTEGER PRIMARY KEY", "a_id": "INTEGER"}},
            },
            "services": {
                "svc": {
                    "protocol": "transaction",
                    "ops": [
                        {"table": "a", "op_type": "insert", "fields": ["val"], "bind": {"id": "$1.id"}},
                        {"table": "b", "op_type": "insert", "fields": ["a_id"]},
                    ],
                },
            },
        })


def test_bind_ref_out_of_bounds():
    with pytest.raises(ValueError, match="bind reference.*refers to op index 5.*only 1 ops"):
        load_config({
            "tables": {"t": {"columns": {"id": "INTEGER PRIMARY KEY", "ref_id": "INTEGER"}}},
            "services": {
                "svc": {
                    "protocol": "transaction",
                    "ops": [
                        {"table": "t", "op_type": "insert", "fields": ["id"], "bind": {"ref_id": "$5.id"}},
                    ],
                },
            },
        })


def test_update_without_key():
    with pytest.raises(ValueError, match="op_type 'update' requires a 'key'"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY", "name": "TEXT"}}},
            "services": {
                "svc": {
                    "protocol": "transaction",
                    "ops": [{"table": "t", "op_type": "update", "fields": ["name"]}],
                },
            },
        })


def test_unknown_config_key_warns(caplog):
    import logging
    with caplog.at_level(logging.WARNING, logger="mkio.config"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {
                "svc": {
                    "protocol": "subpub",
                    "primary_table": "t",
                    "topic": "id",
                    "filerable": ["id"],
                },
            },
        })
    assert any("filerable" in r.message for r in caplog.records)


def test_unknown_top_level_key_warns(caplog):
    import logging
    with caplog.at_level(logging.WARNING, logger="mkio.config"):
        load_config({"tbles": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}}})
    assert any("tbles" in r.message for r in caplog.records)


def test_defaults_column_not_in_table():
    with pytest.raises(ValueError, match="defaults column 'bogus'.*not found"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {
                "svc": {
                    "protocol": "transaction",
                    "ops": [{"table": "t", "op_type": "insert", "fields": ["id"], "defaults": {"bogus": "x"}}],
                },
            },
        })


def test_subpub_defaults_invalid_column():
    with pytest.raises(ValueError, match="defaults column 'bogus'.*not found"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY", "name": "TEXT"}}},
            "services": {
                "svc": {
                    "protocol": "subpub",
                    "primary_table": "t",
                    "topic": "id",
                    "defaults": {"bogus": "'x'"},
                },
            },
        })


def test_subpub_defaults_valid_publish_column():
    cfg = load_config({
        "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY", "val": "INTEGER"}}},
        "services": {
            "svc": {
                "protocol": "subpub",
                "primary_table": "t",
                "topic": "id",
                "publish": {"out_val": "val * 2"},
                "defaults": {"out_val": "0"},
            },
        },
    })
    assert cfg["services"]["svc"].get("_compiled_defaults") is not None


def test_filterable_shows_available_columns():
    with pytest.raises(ValueError, match="Available columns:.*id"):
        load_config({
            "tables": {"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "INTEGER"}}},
            "services": {
                "live": {
                    "protocol": "query",
                    "primary_table": "orders",
                    "filterable": ["bogus"],
                }
            },
        })
