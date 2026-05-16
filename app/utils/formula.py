"""Safe arithmetic expression evaluator for composed-indicator formulas.

The user provides a free-form expression like `a / b` or `(a - b) / c`. We
parse it with `ast.parse` and walk the tree, rejecting anything outside a
small whitelist: numeric literals, named inputs, binary/unary arithmetic ops,
and a handful of stdlib math functions. No attribute access, no comprehensions,
no `eval`/`exec`, no module lookups — so even a maliciously-crafted formula
can't reach the host.
"""

from __future__ import annotations

import ast
import math
import operator
from typing import Dict, Iterable, Set


_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.FloorDiv: operator.floordiv,
}

_UNARY_OPS = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}

_FUNCS = {
    "min": min,
    "max": max,
    "abs": abs,
    "round": round,
    "log": math.log,
    "log2": math.log2,
    "log10": math.log10,
    "sqrt": math.sqrt,
    "exp": math.exp,
    "pow": math.pow,
}


class FormulaError(ValueError):
    """Raised when a formula is invalid or cannot be evaluated."""


def _validate(node: ast.AST, allowed_names: Set[str]) -> None:
    """Walk the AST and reject anything outside the whitelist.

    Raises FormulaError on first disallowed construct.
    """
    if isinstance(node, ast.Expression):
        _validate(node.body, allowed_names)
        return
    if isinstance(node, ast.Constant):
        if not isinstance(node.value, (int, float)) or isinstance(node.value, bool):
            raise FormulaError("Only numeric constants are allowed")
        return
    if isinstance(node, ast.Name):
        # Function names are only valid in the `ast.Call.func` position — that
        # branch handles them directly without recursing here. A bare reference
        # like `min + a` would otherwise pass validation and crash at eval.
        if node.id not in allowed_names:
            raise FormulaError(f"Unknown identifier: {node.id}")
        return
    if isinstance(node, ast.BinOp):
        if type(node.op) not in _BIN_OPS:
            raise FormulaError(f"Disallowed operator: {type(node.op).__name__}")
        _validate(node.left, allowed_names)
        _validate(node.right, allowed_names)
        return
    if isinstance(node, ast.UnaryOp):
        if type(node.op) not in _UNARY_OPS:
            raise FormulaError(f"Disallowed unary operator: {type(node.op).__name__}")
        _validate(node.operand, allowed_names)
        return
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name) or node.func.id not in _FUNCS:
            raise FormulaError("Only whitelisted functions are allowed")
        if node.keywords:
            raise FormulaError("Keyword arguments are not allowed")
        for a in node.args:
            _validate(a, allowed_names)
        return
    raise FormulaError(f"Disallowed expression: {type(node).__name__}")


def compile_formula(formula: str, allowed_names: Iterable[str]) -> ast.Expression:
    """Parse + whitelist-validate `formula`. Returns the AST for reuse."""
    if not formula or not formula.strip():
        raise FormulaError("Formula is empty")
    try:
        tree = ast.parse(formula, mode="eval")
    except SyntaxError as e:
        raise FormulaError(f"Invalid syntax: {e.msg}") from e
    _validate(tree, set(allowed_names))
    return tree


def eval_formula(tree: ast.Expression, env: Dict[str, float]) -> float:
    """Evaluate a pre-compiled formula AST against `env` (name → number)."""

    def _eval(node: ast.AST) -> float:
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.Constant):
            return float(node.value)
        if isinstance(node, ast.Name):
            if node.id in env:
                v = env[node.id]
                if v is None:
                    raise FormulaError(f"Missing value for {node.id}")
                return float(v)
            if node.id in _FUNCS:
                # Bare reference to a function name (no call) doesn't make
                # sense in arithmetic — reject.
                raise FormulaError(f"Cannot use function `{node.id}` as a value")
            raise FormulaError(f"Unknown name: {node.id}")
        if isinstance(node, ast.BinOp):
            op = _BIN_OPS[type(node.op)]
            return op(_eval(node.left), _eval(node.right))
        if isinstance(node, ast.UnaryOp):
            op = _UNARY_OPS[type(node.op)]
            return op(_eval(node.operand))
        if isinstance(node, ast.Call):
            fn = _FUNCS[node.func.id]  # type: ignore[union-attr]
            args = [_eval(a) for a in node.args]
            return fn(*args)
        raise FormulaError(f"Disallowed: {type(node).__name__}")

    return _eval(tree)


def evaluate(formula: str, env: Dict[str, float]) -> float:
    """One-shot parse + validate + evaluate. Convenience wrapper."""
    tree = compile_formula(formula, env.keys())
    return eval_formula(tree, env)
