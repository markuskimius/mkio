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
    # This should be parsed as field ref "name" then unexpected "."
    with pytest.raises(ExprError):
        parse("name.__class__")
