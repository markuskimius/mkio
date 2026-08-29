"""Run the JS expression implementation against the shared conformance fixtures."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
RUNNER = ROOT / "tests" / "run_expr_cases.mjs"
JS_EXPR = ROOT / "src" / "mkio" / "client" / "mkio-expr.mjs"


def test_js_expr_file_exists():
    assert JS_EXPR.exists()


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_js_conformance():
    result = subprocess.run(["node", str(RUNNER)], capture_output=True, text=True)
    lines = [l for l in result.stdout.strip().splitlines() if l.startswith("{")]
    assert lines, f"no JSON summary from node runner\nstdout: {result.stdout}\nstderr: {result.stderr}"
    report = json.loads(lines[-1])
    assert report["total"] > 300
    if report["failures"]:
        msg = "\n".join(f"{f['id']} {f['expr']!r}: {f['message']}" for f in report["failures"])
        pytest.fail(f"{len(report['failures'])} JS conformance failure(s):\n{msg}")
    assert result.returncode == 0


def test_js_language_version_matches_python():
    from mkio.expr import LANGUAGE_VERSION
    src = JS_EXPR.read_text()
    assert f'export const LANGUAGE_VERSION = "{LANGUAGE_VERSION}"' in src
