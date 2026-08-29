"""Expression language tests.

The bulk of the language is specified by ``tests/expr_cases.json`` — the
conformance fixtures shared with the JS implementation. The remaining tests
cover the Python API: registries, environments, lazy functions, host types,
analysis, and error positions.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from mkio import expr
from mkio.expr import (
    Env, ExprError, compile_filter, compile_formatter, compile_template,
    field_refs, function_refs, numeric_fields, parse,
    register_function, register_library, register_type, unregister_library,
)
from mkio.expr._values import equals, kind

CASES_PATH = Path(__file__).parent / "expr_cases.json"
CASES = json.loads(CASES_PATH.read_text())["cases"]


# -- Conformance fixtures ----------------------------------------------------

@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
def test_conformance(case):
    env = Env(strict=case.get("strict", True))
    if "template" in case:
        run = lambda: compile_template(case["template"], env)(case["scope"])
    else:
        run = lambda: expr.compile(case["expr"], env)(case["scope"])
    if "error" in case:
        with pytest.raises(ExprError) as info:
            run()
        assert case["error"] in str(info.value)
    else:
        got = run()
        assert equals(got, case["expect"]), f"got {got!r}, want {case['expect']!r}"
        assert kind(got) == kind(case["expect"])


def test_fixture_ids_unique():
    ids = [c["id"] for c in CASES]
    assert len(ids) == len(set(ids))


def test_fixture_language_version():
    assert json.loads(CASES_PATH.read_text())["language"] == expr.LANGUAGE_VERSION


# -- Registry & environments -------------------------------------------------

@pytest.fixture
def scratch_library():
    yield "scratch"
    unregister_library("scratch")


def test_register_function_default_library():
    register_function("MASK_PAN", lambda s: "****" + s[-4:], library="scratch")
    try:
        assert expr.evaluate("MASK_PAN(pan)", {"pan": "1234567890"}) == "****7890"
        assert expr.evaluate("mask_pan('12345')") == "****2345"
    finally:
        unregister_library("scratch")


def test_register_reserved_name_raises():
    with pytest.raises(ExprError, match="reserved"):
        register_function("null", lambda: 1)
    with pytest.raises(ExprError, match="Invalid"):
        register_function("bad-name", lambda: 1)


def test_register_collision_across_libraries_raises(scratch_library):
    with pytest.raises(ExprError, match="already registered"):
        register_function("UPPER", lambda s: s, library=scratch_library)


def test_register_library_with_metadata(scratch_library):
    register_library(scratch_library, {
        "TWICE": (lambda x: x * 2, {"numeric": True, "params": ("x",), "doc": "Double it"}),
        "HELLO": lambda: "hi",
    })
    assert expr.evaluate("TWICE(a)", {"a": 2}) == 4
    assert expr.evaluate("HELLO()") == "hi"
    assert numeric_fields(parse("TWICE(a) + b")) == {"a"}
    fdef = Env().function("twice")
    assert fdef.doc == "Double it" and fdef.library == scratch_library and fdef.params == ("x",)


def test_named_args_validated_against_params(scratch_library):
    register_function("F", lambda x, y=0: x + y, params=("x", "y"), library=scratch_library)
    assert expr.evaluate("F(1, y: 2)") == 3
    with pytest.raises(ExprError, match="no parameter 'z'"):
        expr.compile("F(1, z: 2)")


def test_env_restricts_libraries(scratch_library):
    register_function("SECRET", lambda: 42, library=scratch_library)
    assert expr.compile("SECRET()")() == 42
    with pytest.raises(ExprError, match="Unknown function: SECRET"):
        expr.compile("SECRET()", Env(libraries=["core", "math"]))
    with pytest.raises(ExprError, match="Unknown function: UPPER"):
        expr.compile("UPPER('a')", Env(libraries=["core"]))


def test_env_strict_vs_lenient():
    with pytest.raises(ExprError, match="Unknown field: 'x'. Available fields: a, b"):
        expr.compile("x")({"a": 1, "b": 2})
    assert expr.compile("x", Env(strict=False))({"a": 1}) is None
    assert expr.compile("x ?? 'd'", Env(strict=False))({}) == "d"


def test_lazy_function_receives_thunks(scratch_library):
    calls = []
    def when(ctx, cond, then):
        calls.append(cond.name)
        return then.value() if expr.truthy(cond.value()) else None
    register_function("WHEN", when, lazy=True, library=scratch_library)
    assert expr.evaluate("WHEN(a, 1 / a)", {"a": 2}) == 0.5
    assert expr.evaluate("WHEN(a, 1 / a)", {"a": 0}) is None   # division never evaluated
    assert calls == ["a", "a"]


def test_lazy_function_can_bind_names(scratch_library):
    def with_(ctx, name, value, body):
        scope = ctx.scope.child({name.name: value.value()})
        return body.eval(scope)
    register_function("WITH", with_, lazy=True, library=scratch_library)
    assert expr.evaluate("WITH(x, a * 2, x + 1)", {"a": 5}) == 11


def test_register_type_hooks():
    register_type(
        "decimal",
        is_instance=lambda v: isinstance(v, Decimal),
        add=lambda a, b: a + b,
        to_string=lambda v: f"{v:f}",
        truthy=lambda v: v != 0,
        compare=lambda a, b: (a > b) - (a < b),
    )
    try:
        scope = {"a": Decimal("1.10"), "b": Decimal("2.20"), "z": Decimal("0")}
        assert expr.evaluate("a + b", scope) == Decimal("3.30")
        assert expr.evaluate("a < b && !z", scope) is True
        assert expr.evaluate("a == a", scope) is True
        assert expr.evaluate("TYPE(a)", scope) == "decimal"
        assert compile_template("${a}!")(scope) == "1.10!"
        assert expr.evaluate("BOOL(z)", scope) is False
    finally:
        expr.TYPES.pop("decimal")


def test_register_type_concat_hook_survives_templates():
    class Tag:
        def __init__(self, parts): self.parts = parts
    register_type("tag", is_instance=lambda v: isinstance(v, Tag),
                  to_string=lambda t: "".join(t.parts),
                  concat=lambda a, b: Tag((a.parts if isinstance(a, Tag) else [a]) + (b.parts if isinstance(b, Tag) else [b])))
    try:
        out = compile_template("<${x}> and ${y}")({"x": Tag(["a"]), "y": 2})
        assert isinstance(out, Tag) and out.parts == ["<", "a", "> and ", "2"]
        assert compile_template("${x}")({"x": Tag(["a"])}).parts == ["a"]
    finally:
        expr.TYPES.pop("tag")


def test_host_type_unknown_to_operators_errors():
    class Opaque: ...
    with pytest.raises(ExprError, match="Cannot add"):
        expr.evaluate("a + 1", {"a": Opaque()})
    assert expr.evaluate("TYPE(a)", {"a": Opaque()}) == "Opaque"


def test_closure_callable_from_python():
    f = expr.evaluate("(a, b) -> a * b + c", {"c": 1})
    assert f(2, 3) == 7
    with pytest.raises(ExprError, match="expects 2"):
        f(1)


# -- Compile helpers ---------------------------------------------------------

def test_compile_filter_truthiness():
    pred = compile_filter("qty > 50 && status == 'pending'")
    assert pred({"qty": 100, "status": "pending"}) is True
    assert pred({"qty": 10, "status": "pending"}) is False
    assert compile_filter("tags")({"tags": []}) is False
    assert compile_filter("note ?? 0")({"note": None}) is False


def test_compile_formatter():
    fmt = compile_formatter({"total": "qty * price", "ticker": "UPPER(symbol)", "fee": "qty * price |> (s -> ROUND(s * 0.001, 2))"})
    assert fmt({"qty": 10, "price": 2.5, "symbol": "aapl"}) == {"total": 25, "ticker": "AAPL", "fee": 0.03}
    assert set(fmt.compiled) == {"total", "ticker", "fee"}


def test_compiled_attributes():
    c = expr.compile("qty * price + LET(x, 1, x)")
    assert c.source.startswith("qty")
    assert c.field_refs == {"qty", "price"}
    assert repr(c) == "<expr 'qty * price + LET(x, 1, x)'>"


def test_compile_template_attributes():
    t = compile_template("Order ${id}: ${NUM(qty * price, digits: 2)}")
    assert t.field_refs == {"id", "qty", "price"}
    assert not t.is_pure
    assert compile_template("${a}").is_pure
    assert expr.has_expressions("a ${b}") and not expr.has_expressions("plain")


def test_compile_reports_unknown_function_at_compile_time():
    with pytest.raises(ExprError, match="Unknown function: NOPE"):
        expr.compile("IF(TRUE, 1, NOPE())")


def test_compile_rejects_non_string():
    with pytest.raises(ExprError, match="must be a string"):
        expr.compile(42)  # type: ignore[arg-type]


# -- Analysis ----------------------------------------------------------------

def test_field_refs():
    assert field_refs(parse("qty * price > limit")) == {"qty", "price", "limit"}
    assert field_refs(parse("MAP(items, i -> i.qty + z)")) == {"items", "z"}
    assert field_refs(parse("LET(s, qty * price, s + fee)")) == {"qty", "price", "fee"}
    assert field_refs(parse("LET(s, s, s)")) == {"s"}   # binding value sees outer s
    assert field_refs(parse("x |> (v -> v + y)")) == {"x", "y"}
    assert field_refs(parse("NUM(a, digits: d)")) == {"a", "d"}
    assert field_refs(parse("'literal' + 1")) == set()
    assert field_refs(parse("m[k].n")) == {"m", "k"}


def test_function_refs():
    assert function_refs(parse("IF(a, upper(b), Round(c, 2)) |> (x -> LEN(x))")) == {"IF", "UPPER", "ROUND", "LEN"}


def test_numeric_fields():
    assert numeric_fields(parse("a * b")) == {"a", "b"}
    assert numeric_fields(parse("a + b")) == set()
    assert numeric_fields(parse("-a")) == {"a"}
    assert numeric_fields(parse("ROUND(a) + UPPER(b)")) == {"a"}
    assert numeric_fields(parse("SUM(xs) // d ** e % f")) == {"xs", "d", "e", "f"}
    assert numeric_fields(parse("NUM(a, digits: d)")) == set()   # NUM is not numeric-flagged
    assert numeric_fields(parse("LET(x, a * 2, x + b)")) == {"a"}
    assert numeric_fields(parse("MAP(xs, i -> i * 2)")) == set()


# -- Errors ------------------------------------------------------------------

def test_error_positions():
    with pytest.raises(ExprError) as info:
        expr.compile("1 + 'a'")()
    assert info.value.pos == 2
    assert "(at position 2)" in str(info.value)
    with pytest.raises(ExprError) as info:
        parse("a b")
    assert info.value.pos == 2


def test_function_python_errors_are_wrapped():
    with pytest.raises(ExprError, match=r"UPPER\(\)"):
        expr.evaluate("UPPER('a', 'b')")
    with pytest.raises(ExprError, match=r"NUM\(\)"):
        expr.evaluate("NUM(1, 2, 3, 4)")


def test_now_is_current():
    import time
    assert abs(expr.evaluate("NOW()") - time.time()) < 5
    assert expr.evaluate("DATE(NOW(), tz: 'local')") == time.strftime("%Y-%m-%d")
