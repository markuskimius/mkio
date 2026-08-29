"""The README function tables are generated from the registry; keep them in sync."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent


def test_readme_expr_tables_in_sync():
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "gen_expr_docs.py"), "--check"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
