"""Run the suite against this working tree, whatever mkio is installed.

Without it a plain ``pytest`` imports the mkio in site-packages — a released
wheel, on a machine that cannot hold an editable install — and tests the old
code against the new tests. Subprocesses (``python -m mkio``, the docs
generator) get the same path through PYTHONPATH.
"""

import os
import sys
from pathlib import Path

_SRC = str(Path(__file__).parent / "src")

if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
os.environ["PYTHONPATH"] = os.pathsep.join(p for p in (_SRC, os.environ.get("PYTHONPATH")) if p)
