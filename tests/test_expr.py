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


# -- Language 2: tokens, prefix parsing, field paths, opt-in libraries --------

def test_duration_tokens_keep_their_text():
    toks = [(t.type, t.value) for t in expr.tokenize("after 1_500ms + 2s")]
    assert toks == [("IDENT", "after"), ("DURATION", "1500ms"), ("OP", "+"), ("DURATION", "2s"), ("EOF", "")]


def _shape(node):
    """An AST without its source offsets."""
    if isinstance(node, tuple):
        return tuple(_shape(n) for n in node)
    if not hasattr(node, "__dataclass_fields__"):
        return node
    return (type(node).__name__, *(
        _shape(getattr(node, f)) for f in node.__dataclass_fields__ if f != "pos"))


def test_word_operators_build_the_symbol_nodes():
    assert _shape(parse("a and b or not c")) == _shape(parse("a && b || !c"))
    assert _shape(parse("a and not b == c")) == _shape(parse("a && !(b == c)"))
    assert _shape(parse("not a in b")) == _shape(parse("a not in b")) == _shape(parse("!(a in b)"))
    assert _shape(parse("a in b")) == ("Binary", "in", ("Name", "a"), ("Name", "b"))


def test_not_is_reserved_as_a_function_name():
    with pytest.raises(ExprError, match="reserved"):
        register_function("not", lambda x: not x)
    with pytest.raises(ExprError, match="reserved"):
        register_library("scratch", {"NOT": lambda x: not x})
    unregister_library("scratch")


def test_parse_prefix_ends_where_the_expression_does():
    line = "fill qty: MIN(100, order.leaves_qty), price: order.price after 2s ± 200ms"
    node, end = expr.parse_prefix(line, 10)
    assert line[10:end] == "MIN(100, order.leaves_qty)" and node == parse("          MIN(100, order.leaves_qty)")
    start = line.index("order.price")
    node, end = expr.parse_prefix(line, start)
    assert line[end:] == "after 2s ± 200ms"
    node, end = expr.parse_prefix(line, line.index("2s"))
    assert node.value == 2 and line[end:] == "± 200ms", "a character the language has no use for ends it"
    assert expr.parse_prefix("x", 0) == (parse("x"), 1), "the end of the text is an end like any other"
    assert expr.parse_prefix("(a, b) -> a + b rest", 0)[1] == 16
    assert expr.parse_prefix("f(x)) tail", 0)[1] == 4, "a bracket it did not open"


def test_parse_prefix_positions_are_offsets_into_the_text():
    line = "when fill and trade.last_price > limit"
    node, end = expr.parse_prefix(line, 14)
    assert (node.pos, node.left.target.pos, node.right.pos, end) == (31, 14, 33, len(line))
    assert expr.compile_node(node, Env())(expr.Scope({"trade": {"last_price": 3}, "limit": 2})) is True
    with pytest.raises(ExprError) as info:
        expr.parse_prefix("when fill and qty >", 14)
    assert info.value.pos == 19
    with pytest.raises(ExprError, match="Expected '\\)'") as info:
        expr.parse_prefix("x: (1 + 2 else", 3)
    assert info.value.pos == 10


def test_parse_prefix_never_reads_past_the_expression():
    line = "expect ack within 2s else fail 'it's broken"
    node, end = expr.parse_prefix(line, line.index("2s"))
    assert line[end:].startswith("else fail")
    with pytest.raises(ExprError, match="Unterminated string") as info:
        expr.parse_prefix("after 'oops", 6)
    assert info.value.pos == 6, "a lexical error inside the expression is still one"
    with pytest.raises(ExprError, match="Bad number literal"):
        expr.parse_prefix("after 5min", 6)


def test_parse_prefix_stop_phrases():
    line = "wait fill where qty > 100 or side in ['Buy'] or timeout 2s"
    start = line.index("qty")
    node, end = expr.parse_prefix(line, start, stop=["or timeout"])
    assert line[end:] == "or timeout 2s" and node == parse(" " * start + "qty > 100 or side in ['Buy']")
    node, end = expr.parse_prefix(line, start, stop=["OR  Timeout"])
    assert line[end:] == "or timeout 2s", "phrases match case-insensitively, by word"
    node, end = expr.parse_prefix("a and b and then c", 0, stop=["and then"])
    assert end == 8
    node, end = expr.parse_prefix("a not in b", 0, stop=["not"])
    assert end == 2
    assert expr.parse_prefix("a or b", 0, stop=["or else"])[1] == 6, "a phrase must match whole"


def test_field_paths():
    paths = lambda s: [str(p) for p in expr.field_paths(parse(s))]
    assert paths("order.leaves_qty > 0 and n") == ["order.leaves_qty", "n"]
    assert paths("event.tag['150'] == event.tag.150") == ["event.tag.150", "event.tag.*"], \
        "a numeric key is an element, written .150 or [150]"
    assert paths("trades[0].px + trades[i].px") == ["trades.*.px", "i", "trades.*.px"]
    assert paths("MAP(trades, t -> t.px * k)") == ["trades", "trades.*.px", "k"]
    assert paths("COUNT(history, h -> h.status == s)") == ["history", "history.*.status", "s"]
    assert paths("REDUCE(items, (acc, i) -> acc + i.qty, 0)") == ["items", "items.*.qty"]
    assert paths("SUM(FILTER(items, i -> i.ok), j -> j.qty)") == ["items", "items.*.ok"], \
        "what a computed array holds is not described"
    assert paths("LET(o, order, o.symbol)") == ["order", "order.symbol"]
    assert paths("LET(x, a + 1, x.y)") == ["a"]
    assert paths("order |> (o -> o.qty)") == ["order", "order.qty"]
    assert paths("(x -> x.a)") == []
    assert paths("{a: m.b}.a") == ["m.b"]
    assert paths("'literal' + 1") == []


def test_field_path_positions():
    (fp,) = expr.field_paths(parse("  order.leaves_qty"))
    assert fp.path == ("order", "leaves_qty") and fp.positions == (2, 8) and fp.pos == 8
    inner = expr.field_paths(parse("MAP(trades, t -> t.px)"))[1]
    assert inner.positions[-1] == 19, "a lambda's read is reported where it is written"


def test_check_fields():
    schema = {
        "order": {"leaves_qty": None, "symbol": None},
        "trades": {"*": {"last_price": None}},
        "event": {"kind": None, "tag": {"*": None}},
        "n": None,
    }
    check = lambda s: [(e.message, e.pos) for e in expr.check_fields(parse(s), schema)]
    assert check("order.leaves_qty > n and event.tag.150 == 'F' and trades[0].last_price > 1") == []
    assert check("MIN(100, order.leave_qty)") == [
        ("Unknown field: 'order.leave_qty'. order has: leaves_qty, symbol", 15)]
    assert check("COUNT(trades, t -> t.last_px > 1) > 0") == [
        ("Unknown field: 'trades.*.last_px'. trades.* has: last_price", 21)]
    assert check("nope + order.symbol") == [
        ("Unknown field: 'nope'. Available fields: event, n, order, trades", 0)]
    assert check("order.symbol.first + n.anything") == [], "below an undescribed value nothing is checked"
    assert check("order[key]") == [("Unknown field: 'key'. Available fields: event, n, order, trades", 6)], \
        "a computed key into known fields cannot be checked; what computes it can"
    assert len(check("order.a + order.b")) == 2, "every problem is reported, not the first"


def test_opt_in_library_stays_out_of_the_default_env():
    register_library("scratch", {"RANDOM": lambda: 4}, default=False)
    try:
        assert "scratch" in expr.OPT_IN_LIBRARIES
        with pytest.raises(ExprError, match="Unknown function: RANDOM"):
            expr.compile("RANDOM()")
        assert "RANDOM" not in Env().functions()
        assert expr.compile("RANDOM() + LEN('ab')", Env(extra=["scratch"]))() == 6
        assert expr.compile("RANDOM()", Env(libraries=["scratch"]))() == 4
        assert "UPPER" in Env(extra=["scratch"]).functions()
        assert Env(libraries=["core"], extra=["scratch"]).function("upper") is None
    finally:
        unregister_library("scratch")
    assert "scratch" not in expr.OPT_IN_LIBRARIES
    register_library("scratch", {"HELLO": lambda: "hi"})
    try:
        assert expr.compile("HELLO()")() == "hi", "re-registered as a default library"
    finally:
        unregister_library("scratch")


def test_lenient_tokenizer_ends_in_an_error_token():
    toks = expr.tokenize("a + 1 ± b", lenient=True)
    assert [(t.type, t.pos) for t in toks] == [("IDENT", 0), ("OP", 2), ("NUMBER", 4), ("ERROR", 6)]
    assert "Unexpected character" in toks[-1].value
    assert [t.type for t in expr.tokenize("x 'open", lenient=True)] == ["IDENT", "ERROR"]
    assert [t.type for t in expr.tokenize("±", lenient=True)] == ["ERROR"]
    assert [(t.type, t.pos) for t in expr.tokenize("skip a", 5)] == [("IDENT", 5), ("EOF", 6)]
    with pytest.raises(ExprError, match="Unexpected character"):
        expr.tokenize("a ± b")


def test_language_2_reaches_filters_and_formatters():
    keep = compile_filter("symbol in ['AAPL', 'MSFT'] and not note and age < 1.5m")
    assert keep({"symbol": "AAPL", "note": "", "age": 60}) is True
    assert keep({"symbol": "AAPL", "note": "held", "age": 60}) is False
    assert keep({"symbol": "IBM", "note": "", "age": 60}) is False
    fmt = compile_formatter({"fills": "COUNT(items, i -> i.qty > 0)", "notional": "SUM(items, i -> i.qty * i.px)"})
    assert fmt({"items": [{"qty": 2, "px": 3}, {"qty": 0, "px": 9}]}) == {"fills": 1, "notional": 6}
    assert field_refs(parse("a in b and not c")) == {"a", "b", "c"}
    assert numeric_fields(parse("a in b")) == set()
