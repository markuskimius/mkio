"""Tests for expression language."""

import pytest
from mkio._expr import (
    ExprError,
    compile_filter,
    compile_formatter,
    evaluate,
    parse,
    register_function,
    tokenize,
    FUNCTIONS,
)


# -- Tokenizer ---------------------------------------------------------------

def test_tokenize_number():
    tokens = tokenize("42")
    assert tokens[0].type == "NUMBER"
    assert tokens[0].value == "42"


def test_tokenize_float():
    tokens = tokenize("3.14")
    assert tokens[0].type == "NUMBER"
    assert tokens[0].value == "3.14"


def test_tokenize_string():
    tokens = tokenize("'hello'")
    assert tokens[0].type == "STRING"
    assert tokens[0].value == "hello"


def test_tokenize_operators():
    tokens = tokenize("== != >= <= > <")
    ops = [t.value for t in tokens if t.type == "OP"]
    assert ops == ["==", "!=", ">=", "<=", ">", "<"]


def test_tokenize_ident():
    tokens = tokenize("status")
    assert tokens[0].type == "IDENT"
    assert tokens[0].value == "status"


def test_unterminated_string():
    with pytest.raises(ExprError, match="Unterminated"):
        tokenize("'hello")


# -- Parser -------------------------------------------------------------------

def test_parse_literal():
    from mkio._expr import Literal
    node = parse("42")
    assert isinstance(node, Literal)
    assert node.value == 42


def test_parse_field():
    from mkio._expr import FieldRef
    node = parse("status")
    assert isinstance(node, FieldRef)
    assert node.name == "status"


def test_parse_comparison():
    from mkio._expr import BinOp
    node = parse("qty > 100")
    assert isinstance(node, BinOp)
    assert node.op == ">"


def test_parse_and_or():
    from mkio._expr import BinOp
    node = parse("a == 1 AND b == 2 OR c == 3")
    # OR has lower precedence, so top node is OR
    assert isinstance(node, BinOp)
    assert node.op == "OR"


def test_parse_not():
    from mkio._expr import UnaryOp
    node = parse("NOT active")
    assert isinstance(node, UnaryOp)
    assert node.op == "NOT"


def test_parse_is_null():
    from mkio._expr import UnaryOp
    node = parse("name IS NULL")
    assert isinstance(node, UnaryOp)
    assert node.op == "IS NULL"


def test_parse_is_not_null():
    from mkio._expr import UnaryOp
    node = parse("name IS NOT NULL")
    assert isinstance(node, UnaryOp)
    assert node.op == "IS NOT NULL"


def test_parse_function_call():
    from mkio._expr import FuncCall
    node = parse("UPPER(name)")
    assert isinstance(node, FuncCall)
    assert node.name == "UPPER"


def test_parse_nested_parens():
    from mkio._expr import BinOp
    node = parse("(a + b) * c")
    assert isinstance(node, BinOp)
    assert node.op == "*"


def test_parse_precedence():
    from mkio._expr import BinOp
    node = parse("a + b * c")
    assert isinstance(node, BinOp)
    assert node.op == "+"
    assert isinstance(node.right, BinOp)
    assert node.right.op == "*"


# -- Evaluator ---------------------------------------------------------------

ROW = {"name": "Alice", "qty": 100, "price": 15.0, "status": "pending", "tag": None}


def test_eval_field():
    assert evaluate(parse("name"), ROW) == "Alice"


def test_eval_comparison():
    assert evaluate(parse("qty > 50"), ROW) is True
    assert evaluate(parse("qty < 50"), ROW) is False


def test_eval_equality():
    assert evaluate(parse("status == 'pending'"), ROW) is True
    assert evaluate(parse("status != 'pending'"), ROW) is False


def test_eval_arithmetic():
    assert evaluate(parse("qty * price"), ROW) == 1500.0
    assert evaluate(parse("qty + 10"), ROW) == 110


def test_eval_and_or():
    assert evaluate(parse("qty > 50 AND status == 'pending'"), ROW) is True
    assert evaluate(parse("qty > 200 OR status == 'pending'"), ROW) is True
    assert evaluate(parse("qty > 200 AND status == 'filled'"), ROW) is False


def test_eval_not():
    assert evaluate(parse("NOT qty > 200"), ROW) is True


def test_eval_is_null():
    assert evaluate(parse("tag IS NULL"), ROW) is True
    assert evaluate(parse("name IS NULL"), ROW) is False
    assert evaluate(parse("tag IS NOT NULL"), ROW) is False


def test_eval_contains():
    assert evaluate(parse("name CONTAINS 'lic'"), ROW) is True
    assert evaluate(parse("name CONTAINS 'xyz'"), ROW) is False


def test_eval_starts_with():
    assert evaluate(parse("name STARTS_WITH 'Ali'"), ROW) is True
    assert evaluate(parse("name STARTS_WITH 'Bob'"), ROW) is False


def test_eval_function_upper():
    assert evaluate(parse("UPPER(name)"), ROW) == "ALICE"


def test_eval_function_lower():
    assert evaluate(parse("LOWER(name)"), ROW) == "alice"


def test_eval_function_round():
    assert evaluate(parse("ROUND(price, 0)"), {"price": 15.7}) == 16.0


def test_eval_function_abs():
    assert evaluate(parse("ABS(qty)"), {"qty": -5}) == 5


def test_eval_function_coalesce():
    assert evaluate(parse("COALESCE(tag, 'default')"), ROW) == "default"
    assert evaluate(parse("COALESCE(name, 'default')"), ROW) == "Alice"


def test_eval_division_by_zero():
    with pytest.raises(ExprError, match="Division by zero"):
        evaluate(parse("10 / 0"), {})


def test_eval_unknown_field():
    with pytest.raises(ExprError, match="Unknown field"):
        evaluate(parse("nonexistent"), ROW)


def test_unknown_function():
    with pytest.raises(ExprError, match="Unknown function"):
        parse("BOGUS(x)")


# -- Unary minus ---------------------------------------------------------------

def test_eval_unary_minus():
    assert evaluate(parse("-qty"), ROW) == -100


# -- compile_filter -----------------------------------------------------------

def test_compile_filter():
    pred = compile_filter("qty > 50 AND status == 'pending'")
    assert pred(ROW) is True
    assert pred({"qty": 10, "status": "pending"}) is False


# -- compile_formatter --------------------------------------------------------

def test_compile_formatter():
    fmt = compile_formatter({
        "total": "qty * price",
        "ticker": "UPPER(name)",
    })
    result = fmt(ROW)
    assert result == {"total": 1500.0, "ticker": "ALICE"}


# -- register_function --------------------------------------------------------

def test_register_function():
    register_function("DOUBLE", lambda x: x * 2)
    assert evaluate(parse("DOUBLE(qty)"), ROW) == 200
    # Cleanup
    del FUNCTIONS["DOUBLE"]


def test_register_function_used_in_filter():
    register_function("HALF", lambda x: x / 2)
    pred = compile_filter("HALF(qty) > 30")
    assert pred(ROW) is True
    del FUNCTIONS["HALF"]


def test_register_keyword_name_raises():
    with pytest.raises(ExprError, match="keyword"):
        register_function("AND", lambda: None)


def test_register_special_form_name_raises():
    with pytest.raises(ExprError, match="keyword"):
        register_function("IF", lambda *a: None)
    with pytest.raises(ExprError, match="keyword"):
        register_function("if", lambda *a: None)


# -- IF function --------------------------------------------------------------

def test_if_true_branch():
    assert evaluate(parse("IF(qty > 50, 'big', 'small')"), ROW) == "big"


def test_if_false_branch():
    assert evaluate(parse("IF(qty < 50, 'big', 'small')"), ROW) == "small"


def test_if_with_expressions():
    row = {"side": "Buy", "price": 10.0}
    assert evaluate(parse("IF(side == 'Buy', price, -price)"), row) == 10.0
    row["side"] = "Sell"
    assert evaluate(parse("IF(side == 'Buy', price, -price)"), row) == -10.0


def test_if_wrong_arg_count():
    with pytest.raises(ExprError, match="3 arguments"):
        evaluate(parse("IF(1, 2)"), {})


def test_if_in_formatter():
    fmt = compile_formatter({"price": "IF(side == 'Buy', price, -price)"})
    assert fmt({"side": "Buy", "price": 5.0}) == {"price": 5.0}
    assert fmt({"side": "Sell", "price": 5.0}) == {"price": -5.0}


# -- Security -----------------------------------------------------------------

def test_rejects_attribute_access():
    node = parse("name.__class__")
    with pytest.raises(ExprError, match="dunder"):
        evaluate(node, {"name": "Alice"})


def test_rejects_dunder_bracket():
    node = parse("meta['__class__']")
    with pytest.raises(ExprError, match="dunder"):
        evaluate(node, {"meta": {"__class__": "x"}})


# -- Tokenizer: new tokens ---------------------------------------------------

def test_tokenize_brackets():
    tokens = tokenize("[1, 2]")
    types = [t.type for t in tokens if t.type != "EOF"]
    assert types == ["LBRACKET", "NUMBER", "COMMA", "NUMBER", "RBRACKET"]


def test_tokenize_braces():
    tokens = tokenize("{a: 1}")
    types = [t.type for t in tokens if t.type != "EOF"]
    assert types == ["LBRACE", "IDENT", "COLON", "NUMBER", "RBRACE"]


def test_tokenize_colon():
    tokens = tokenize(":")
    assert tokens[0].type == "COLON"
    assert tokens[0].value == ":"


def test_tokenize_dot():
    tokens = tokenize("x.y")
    types = [t.type for t in tokens if t.type != "EOF"]
    assert types == ["IDENT", "DOT", "IDENT"]


def test_tokenize_dot_number_literal():
    tokens = tokenize(".5")
    assert tokens[0].type == "NUMBER"
    assert tokens[0].value == ".5"


def test_tokenize_float_unchanged():
    tokens = tokenize("3.14")
    assert tokens[0].type == "NUMBER"
    assert tokens[0].value == "3.14"


def test_tokenize_dot_after_number():
    tokens = tokenize("a.0")
    types = [t.type for t in tokens if t.type != "EOF"]
    assert types == ["IDENT", "DOT", "NUMBER"]


# -- Parser: arrays -----------------------------------------------------------

def test_parse_array_literal():
    from mkio._expr import ArrayLiteral, Literal
    node = parse("[1, 2, 3]")
    assert isinstance(node, ArrayLiteral)
    assert len(node.elements) == 3
    assert all(isinstance(e, Literal) for e in node.elements)


def test_parse_empty_array():
    from mkio._expr import ArrayLiteral
    node = parse("[]")
    assert isinstance(node, ArrayLiteral)
    assert node.elements == ()


def test_parse_array_with_expressions():
    from mkio._expr import ArrayLiteral, BinOp
    node = parse("[1 + 2, 3 * 4]")
    assert isinstance(node, ArrayLiteral)
    assert len(node.elements) == 2
    assert all(isinstance(e, BinOp) for e in node.elements)


# -- Parser: maps -------------------------------------------------------------

def test_parse_map_literal():
    from mkio._expr import MapLiteral
    node = parse("{a: 1, b: 2}")
    assert isinstance(node, MapLiteral)
    assert node.keys == ("a", "b")
    assert len(node.values) == 2


def test_parse_map_string_keys():
    from mkio._expr import MapLiteral
    node = parse("{'a': 1, 'b': 2}")
    assert isinstance(node, MapLiteral)
    assert node.keys == ("a", "b")


def test_parse_empty_map():
    from mkio._expr import MapLiteral
    node = parse("{}")
    assert isinstance(node, MapLiteral)
    assert node.keys == ()
    assert node.values == ()


def test_parse_map_bad_key():
    with pytest.raises(ExprError, match="Map key"):
        parse("{123: 'x'}")


# -- Parser: index/dot access ------------------------------------------------

def test_parse_index_bracket():
    from mkio._expr import Index, FieldRef, Literal
    node = parse("tags[0]")
    assert isinstance(node, Index)
    assert isinstance(node.target, FieldRef)
    assert isinstance(node.key, Literal)
    assert node.key.value == 0


def test_parse_index_dot():
    from mkio._expr import Index, FieldRef, Literal
    node = parse("meta.region")
    assert isinstance(node, Index)
    assert isinstance(node.target, FieldRef)
    assert isinstance(node.key, Literal)
    assert node.key.value == "region"


def test_parse_chained_access():
    from mkio._expr import Index
    node = parse("a.b[0].c")
    assert isinstance(node, Index)
    assert node.key.value == "c"
    assert isinstance(node.target, Index)
    assert node.target.key.value == 0
    assert isinstance(node.target.target, Index)
    assert node.target.target.key.value == "b"


def test_parse_dot_number_access():
    from mkio._expr import Index, Literal
    node = parse("arr.0")
    assert isinstance(node, Index)
    assert isinstance(node.key, Literal)
    assert node.key.value == 0


# -- Parser: LET -------------------------------------------------------------

def test_parse_let_simple():
    from mkio._expr import Let
    node = parse("LET x = 1 IN x + 1")
    assert isinstance(node, Let)
    assert len(node.bindings) == 1
    assert node.bindings[0][0] == "x"


def test_parse_let_multiple():
    from mkio._expr import Let
    node = parse("LET a = 1, b = 2 IN a + b")
    assert isinstance(node, Let)
    assert len(node.bindings) == 2
    assert node.bindings[0][0] == "a"
    assert node.bindings[1][0] == "b"


def test_parse_let_case_insensitive():
    from mkio._expr import Let
    node = parse("let x = 1 in x")
    assert isinstance(node, Let)


def test_parse_let_missing_in():
    with pytest.raises(ExprError, match="IN"):
        parse("LET x = 1 x")


def test_register_let_as_function_raises():
    with pytest.raises(ExprError, match="keyword"):
        register_function("LET", lambda: None)


# -- Evaluator: arrays -------------------------------------------------------

def test_eval_array_literal():
    assert evaluate(parse("[1, 2, 3]"), {}) == [1, 2, 3]


def test_eval_array_with_fields():
    assert evaluate(parse("[name, qty]"), ROW) == ["Alice", 100]


def test_eval_empty_array():
    assert evaluate(parse("[]"), {}) == []


def test_eval_array_in_array():
    assert evaluate(parse("[[1, 2], [3, 4]]"), {}) == [[1, 2], [3, 4]]


# -- Evaluator: maps ---------------------------------------------------------

def test_eval_map_literal():
    result = evaluate(parse("{symbol: name, total: qty * price}"), ROW)
    assert result == {"symbol": "Alice", "total": 1500.0}


def test_eval_empty_map():
    assert evaluate(parse("{}"), {}) == {}


def test_eval_map_with_string_keys():
    result = evaluate(parse("{'x': 1, 'y': 2}"), {})
    assert result == {"x": 1, "y": 2}


def test_eval_nested_map():
    result = evaluate(parse("{inner: {a: 1}}"), {})
    assert result == {"inner": {"a": 1}}


# -- Evaluator: index access -------------------------------------------------

def test_eval_index_list():
    assert evaluate(parse("[10, 20, 30][1]"), {}) == 20


def test_eval_index_dict():
    row = {"meta": {"region": "US", "tier": 1}}
    assert evaluate(parse("meta['region']"), row) == "US"
    assert evaluate(parse("meta['tier']"), row) == 1


def test_eval_dot_access():
    row = {"meta": {"region": "US"}}
    assert evaluate(parse("meta.region"), row) == "US"


def test_eval_chained_index():
    row = {"data": {"items": [{"name": "first"}, {"name": "second"}]}}
    assert evaluate(parse("data.items[0].name"), row) == "first"
    assert evaluate(parse("data.items[1].name"), row) == "second"


def test_eval_index_out_of_range():
    with pytest.raises(ExprError, match="Index error"):
        evaluate(parse("[1, 2][99]"), {})


def test_eval_index_missing_key():
    row = {"meta": {"a": 1}}
    with pytest.raises(ExprError, match="Unknown key"):
        evaluate(parse("meta['missing']"), row)


def test_eval_index_null():
    with pytest.raises(ExprError, match="Cannot index into NULL"):
        evaluate(parse("tag[0]"), ROW)


def test_eval_index_non_indexable():
    with pytest.raises(ExprError, match="Cannot index into"):
        evaluate(parse("qty[0]"), ROW)


# -- Evaluator: LET ----------------------------------------------------------

def test_eval_let_simple():
    result = evaluate(parse("LET total = qty * price IN total > 1000"), ROW)
    assert result is True


def test_eval_let_multiple_bindings():
    result = evaluate(parse("LET a = 1, b = a + 1 IN b"), {})
    assert result == 2


def test_eval_let_shadows_field():
    result = evaluate(parse("LET name = 'Bob' IN name"), ROW)
    assert result == "Bob"


def test_eval_let_no_leak():
    result = evaluate(parse("LET x = 99 IN x"), ROW)
    assert result == 99
    assert "x" not in ROW


def test_eval_let_with_functions():
    result = evaluate(parse("LET u = UPPER(name) IN u"), ROW)
    assert result == "ALICE"


def test_eval_let_in_formatter():
    fmt = compile_formatter({"label": "LET t = qty * price IN IF(t > 1000, 'big', 'small')"})
    assert fmt(ROW) == {"label": "big"}


def test_eval_let_nested():
    result = evaluate(parse("LET x = 1 IN LET y = x + 1 IN y"), {})
    assert result == 2


# -- Evaluator: IN with array literal ----------------------------------------

def test_eval_in_with_array_literal():
    assert evaluate(parse("status IN ['active', 'pending']"), ROW) is True
    assert evaluate(parse("status IN ['filled', 'cancelled']"), ROW) is False


def test_eval_in_empty_array():
    assert evaluate(parse("name IN []"), ROW) is False


# -- New functions ------------------------------------------------------------

def test_len_string():
    assert evaluate(parse("LEN(name)"), ROW) == 5


def test_len_list():
    assert evaluate(parse("LEN([1, 2, 3])"), {}) == 3


def test_len_map():
    assert evaluate(parse("LEN({a: 1, b: 2})"), {}) == 2


def test_keys():
    row = {"meta": {"x": 1, "y": 2}}
    result = evaluate(parse("KEYS(meta)"), row)
    assert sorted(result) == ["x", "y"]


def test_values():
    row = {"meta": {"x": 1, "y": 2}}
    result = evaluate(parse("VALUES(meta)"), row)
    assert sorted(result) == [1, 2]


def test_flatten():
    result = evaluate(parse("FLATTEN([[1, 2], [3], 4])"), {})
    assert result == [1, 2, 3, 4]


def test_merge():
    result = evaluate(parse("MERGE({a: 1}, {b: 2, a: 3})"), {})
    assert result == {"a": 3, "b": 2}


def test_merge_empty():
    result = evaluate(parse("MERGE({})"), {})
    assert result == {}


# -- Compile helpers with new features ----------------------------------------

def test_compile_filter_with_array():
    pred = compile_filter("status IN ['active', 'pending']")
    assert pred(ROW) is True
    assert pred({"status": "filled"}) is False


def test_compile_formatter_with_map():
    fmt = compile_formatter({"output": "{total: qty * price, label: UPPER(name)}"})
    result = fmt(ROW)
    assert result == {"output": {"total": 1500.0, "label": "ALICE"}}
