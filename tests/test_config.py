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
    assert config["event_loop"] == "auto"


def test_cache_control_validated():
    assert load_config({})["cache_control"] == "no-cache"
    assert load_config({"cache_control": ""})["cache_control"] == ""  # off
    assert load_config({"cache_control": " max-age=60 "})["cache_control"] == "max-age=60"
    for bad in ({"cache_control": False}, {"cache_control": 60},
                {"cache_control": "no-cache\r\nX-Evil: 1"}):
        with pytest.raises(ValueError, match="cache_control"):
            load_config(bad)


def test_file_route_entries_validated(caplog):
    from mkio.config import route_entry

    config = load_config({
        "static": {"/": "./static", "/assets": {"path": "./a", "cache_control": " immutable "}},
        "config": {"/conf": {"path": "./c"}},
    })
    assert route_entry(config, "static", "/") == ("./static", None)
    assert route_entry(config, "static", "/assets") == ("./a", "immutable")
    assert route_entry(config, "config", "/conf") == ("./c", None)

    for bad in ({"static": {"/": 5}}, {"static": {"/": {"cache_control": "no-cache"}}},
                {"config": {"/c": {"path": ""}}},
                {"static": {"/": {"path": "./s", "cache_control": False}}},
                {"config": {"/c": {"path": "./c", "cache_control": "a\nb"}}}):
        with pytest.raises(ValueError, match=r"\[(static|config)\]"):
            load_config(bad)

    with caplog.at_level("WARNING"):
        load_config({"static": {"/": {"path": "./s", "cache_contrl": "no-cache"}}})
    assert "did you mean 'cache_control'" in caplog.text


def test_websocket_knobs_validated():
    config = load_config({})
    assert config["ws_heartbeat_s"] == 30 and config["ws_send_buffer_mb"] == 16
    assert load_config({"ws_heartbeat_s": 0})["ws_heartbeat_s"] == 0  # off
    assert load_config({"ws_send_buffer_mb": 0.5})["ws_send_buffer_mb"] == 0.5
    for bad in ({"ws_heartbeat_s": -1}, {"ws_heartbeat_s": "30"}, {"ws_heartbeat_s": True},
                {"ws_send_buffer_mb": 0}, {"ws_send_buffer_mb": -4}):
        with pytest.raises(ValueError, match="ws_"):
            load_config(bad)


def test_event_loop_validated(monkeypatch):
    """`event_loop` names how run() builds its loop; a value outside the four,
    or "proactor" off Windows (the loop exists only there), fails at load."""
    import sys
    for value in ("auto", "selector", "uvloop"):
        assert load_config({"event_loop": value})["event_loop"] == value
    with pytest.raises(ValueError, match="event_loop must be one of"):
        load_config({"event_loop": "gevent"})
    monkeypatch.setattr(sys, "platform", "linux")
    with pytest.raises(ValueError, match="Windows only"):
        load_config({"event_loop": "proactor"})
    monkeypatch.setattr(sys, "platform", "win32")
    assert load_config({"event_loop": "proactor"})["event_loop"] == "proactor"


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


def test_query_key_must_name_primary_table_columns():
    with pytest.raises(ValueError, match="key column 'bogus' not found in table 't'"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY", "name": "TEXT"}}},
            "services": {
                "svc": {"protocol": "query", "primary_table": "t", "key": ["bogus"]},
            },
        })
    with pytest.raises(ValueError, match="key must be a non-empty list"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {
                "svc": {"protocol": "query", "primary_table": "t", "key": "id"},
            },
        })
    config = load_config({
        "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
        "services": {
            "svc": {"protocol": "query", "primary_table": "t", "key": ["id"]},
        },
    })
    assert config["services"]["svc"]["key"] == ["id"]


def test_query_key_may_alias_under_a_custom_sql():
    """A custom sql may rename columns, so only the shape is checked."""
    config = load_config({
        "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
        "services": {
            "svc": {
                "protocol": "query", "primary_table": "t",
                "sql": "SELECT id AS ident FROM t", "key": ["ident"],
            },
        },
    })
    assert config["services"]["svc"]["key"] == ["ident"]


def _joined(watch_columns, **extra):
    return {
        "tables": {
            "orders": {"columns": {"id": "TEXT PRIMARY KEY", "symbol": "TEXT"}},
            "symbols": {"columns": {"symbol": "TEXT PRIMARY KEY", "name": "TEXT", "last": "REAL"}},
        },
        "services": {"svc": {
            "protocol": "query", "primary_table": "orders",
            "watch_tables": ["orders", "symbols"],
            "sql": "SELECT o.*, s.name FROM orders o JOIN symbols s ON s.symbol = o.symbol",
            "watch_columns": watch_columns, **extra,
        }},
    }


def test_watch_columns_name_columns_of_a_watched_secondary_table():
    config = load_config(_joined({"symbols": ["name"]}))
    assert config["services"]["svc"]["watch_columns"] == {"symbols": ["name"]}
    with pytest.raises(ValueError, match="watch_columns column 'nmae' not found in table 'symbols'"):
        load_config(_joined({"symbols": ["nmae"]}))
    with pytest.raises(ValueError, match="watch_columns table 'vendors' is not in watch_tables"):
        load_config(_joined({"vendors": ["name"]}))
    with pytest.raises(ValueError, match="cannot name the primary table 'orders'"):
        load_config(_joined({"orders": ["symbol"]}))
    with pytest.raises(ValueError, match=r"watch_columns\['symbols'\] must be a non-empty list"):
        load_config(_joined({"symbols": "name"}))
    with pytest.raises(ValueError, match="watch_columns must map"):
        load_config(_joined(["name"]))


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


# ---- auto_migrate validation ------------------------------------------------


def test_auto_migrate_false():
    config = load_config({"auto_migrate": False})
    assert config["auto_migrate"] is False


def test_auto_migrate_true_normalized_to_safe():
    config = load_config({"auto_migrate": True})
    assert config["auto_migrate"] == "safe"


def test_auto_migrate_safe():
    config = load_config({"auto_migrate": "safe"})
    assert config["auto_migrate"] == "safe"


def test_auto_migrate_risky():
    config = load_config({"auto_migrate": "risky"})
    assert config["auto_migrate"] == "risky"


def test_auto_migrate_destructive():
    config = load_config({"auto_migrate": "destructive"})
    assert config["auto_migrate"] == "destructive"


def test_auto_migrate_invalid_value():
    with pytest.raises(ValueError, match="auto_migrate"):
        load_config({"auto_migrate": "yolo"})
