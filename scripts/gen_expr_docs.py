"""Generate the expression-language function tables in README.md from the registry.

    python scripts/gen_expr_docs.py          # rewrite README.md in place
    python scripts/gen_expr_docs.py --check  # exit 1 if README.md is stale

The tables live between <!-- expr-functions:start --> and <!-- expr-functions:end -->.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from mkio.expr import LIBRARIES  # noqa: E402

START = "<!-- expr-functions:start -->"
END = "<!-- expr-functions:end -->"
ORDER = ("core", "math", "string", "format", "time", "collection")
BLURB = {
    "core": "Control flow, coercion, and safe lookup. `IF`, `CASE`, `TRY`, and `LET` are lazy — only the branches they take are evaluated.",
    "math": "Numeric helpers. `ROUND` rounds half away from zero on the decimal representation (`ROUND(2.675, 2)` is `2.68`).",
    "string": "`REPLACE` and `SPLIT` take literal strings; only `MATCHES` takes a regular expression. Lengths and positions count characters (code points).",
    "format": "Numbers to display strings. Rounding follows `ROUND`; no locale is involved.",
    "time": "The numeric unit is **seconds since the Unix epoch**. `EPOCH` accepts numbers, mkio ref strings (`\"20260828 12:34:56.123456789000\"`), and ISO-8601. Formats understand `%Y %m %d %H %M %S %f %z %%` (`%f` = microseconds); time zones are `'UTC'` (default), `'local'`, or a fixed offset such as `'+09:00'`.",
    "collection": "Arrays and maps; the higher-order functions take lambdas.",
}


def signature(fdef) -> str:
    if fdef.params:
        return f"`{fdef.name}({', '.join(fdef.params)})`"
    if fdef.name in ("COALESCE", "MIN", "MAX", "CONCAT", "MERGE"):
        return f"`{fdef.name}(...)`"
    if fdef.name == "CASE":
        return "`CASE(cond, value, ..., default?)`"
    if fdef.name == "LET":
        return "`LET(name, value, ..., body)`"
    if fdef.name == "FORMAT":
        return "`FORMAT(pattern, ...)`"
    return f"`{fdef.name}()`"


def render() -> str:
    out = [START, ""]
    for lib in ORDER:
        fns = LIBRARIES[lib]
        out.append(f"#### `{lib}`")
        out.append("")
        out.append(BLURB[lib])
        out.append("")
        out.append("| Function | Description |")
        out.append("|---|---|")
        for name in sorted(fns):
            f = fns[name]
            flags = " *(lazy)*" if f.lazy else ""
            out.append(f"| {signature(f)}{flags} | {f.doc} |")
        out.append("")
    out.append(END)
    return "\n".join(out)


def main() -> int:
    readme = Path(__file__).resolve().parent.parent / "README.md"
    text = readme.read_text()
    a, b = text.index(START), text.index(END) + len(END)
    new = text[:a] + render() + text[b:]
    if "--check" in sys.argv:
        if new != text:
            print("README.md expression tables are stale — run scripts/gen_expr_docs.py")
            return 1
        print("README.md expression tables are up to date")
        return 0
    readme.write_text(new)
    print("README.md updated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
