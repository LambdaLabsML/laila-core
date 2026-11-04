#!/usr/bin/env python3
"""py2rs: Python statement skeleton -> Tier 2 Rust for `laila::python! { .. }`.

Converts a Python file's *statement* layer (def/class/blocks/assignments/
try/with/decorators) to Rust item syntax and leaves every *expression*
verbatim in the Python-shaped form that the `laila::python!` proc-macro
understands. The output is deterministic: CI regenerates and diffs.

    python tools/py2rs/py2rs.py tests/functional/entry/unit_tests/test_entry.py \
        -o laila-rs/tests/functional/entry/unit_tests/test_entry.rs
    python tools/py2rs/py2rs.py tests/functional -o laila-rs/tests/functional

Rules are numbered in laila-rs/docs/PORT_RULES.md; the comments below cite
them as `[P<n>]`.
"""

from __future__ import annotations

import argparse
import ast
import copy
import io
import os
import re
import sys
import tokenize
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pytest_desugar

# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

RUST_KEYWORDS = {
    "as",
    "break",
    "const",
    "continue",
    "crate",
    "dyn",
    "else",
    "enum",
    "extern",
    "false",
    "fn",
    "for",
    "if",
    "impl",
    "in",
    "let",
    "loop",
    "match",
    "mod",
    "move",
    "mut",
    "pub",
    "ref",
    "return",
    "self",
    "Self",
    "static",
    "struct",
    "super",
    "trait",
    "true",
    "type",
    "unsafe",
    "use",
    "where",
    "while",
    "abstract",
    "become",
    "box",
    "do",
    "final",
    "macro",
    "override",
    "priv",
    "typeof",
    "unsized",
    "virtual",
    "yield",
    "try",
    "gen",
}
# Names that are both Python and Rust keywords and that the macro reads as Python.
PY_SHARED = {
    "in",
    "if",
    "else",
    "for",
    "while",
    "return",
    "break",
    "continue",
    "as",
    "async",
    "await",
}

# Python modules the `laila::python!` rewriter resolves by name to
# `laila::python::stdlib::<module>` (no `use` needed) [P2].
PRELUDE_MODULES = {
    "json",
    "uuid",
    "time",
    "os",
    "sys",
    "threading",
    "asyncio",
    "tempfile",
    "pytest",
    "math",
    "random",
    "base64",
    "hashlib",
    "io",
    "copy",
    "pathlib",
    "shutil",
    "socket",
    "struct",
    "subprocess",
    "logging",
    "urllib",
    "re",
    "datetime",
    "functools",
    "itertools",
    "collections",
    "typing",
    "unittest",
    "numpy",
    "np",
    "cryptography",
    "gc",
    "builtins",
    "sqlite3",
    "h5py",
    "textwrap",
    "statistics",
    "inspect",
    "contextlib",
    "tomllib",
    "xml",
}

# Base classes that are Rust *traits* (`laila.atomic` mixins): subclasses stay
# `py_class!` structs with `impl Mixin for C {}` [P13], never `py_model!`.
ATOMIC_MIXINS = {
    "_LAILA_LOCALLY_ATOMIC_OBJECT",
    "_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT",
    "_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT",
}

# Base classes that are Rust `ModelClass`es (pydantic models in Python):
# a test-local subclass -- even one with only plain class attributes
# (`class _Scoped(_LAILA_IDENTIFIABLE_OBJECT): _DEFAULT_SCOPES = [..]`) --
# is a `py_model!` so the class variables and private attributes resolve
# through the model descriptor [P14b].
MODEL_BASES = {
    "BaseModel",
    "_LAILA_OBJECT",
    "_LAILA_IDENTIFIABLE_OBJECT",
    "_LAILA_CLI_CAPABLE_CLASS",
    "_LAILA_IDENTIFIABLE_POOL",
}

# Python annotation -> Rust type for `x: T = v` [P8]. `bytes` stays a
# dynamic value: a `Vec<u8>` would turn into a *list* of ints when passed
# to a dynamic call (`IntoArg for Vec<T>`), `Arg::Bytes` keeps the type.
TYPE_MAP = {"int": "i64", "float": "f64", "str": "String", "bool": "bool", "bytes": "Arg"}
# The future hierarchy is one Rust type tagged by `FutureKind`; the class
# names are constructor / `isinstance` markers, the *value* type is `Future`.
FUTURE_CLASSES = {
    "Future",
    "GroupFuture",
    "ComplexFuture",
    "ConcurrentPackageFuture",
    "_LAILA_IDENTIFIABLE_FUTURE",
}
for _n in FUTURE_CLASSES:
    TYPE_MAP[_n] = "laila::policy::central::command::Future"
# `from concurrent.futures import X`: the Rust twins of the names.
# Python implementation modules the Rust crate flattens into their package
# (`laila/utils/args/args.py` is `laila/src/utils/args.rs`) [P2].
LAILA_MODULE_COLLAPSE = {
    "laila.utils.args.args": "laila.utils.args",
}

CONCURRENT_FUTURES_NAMES = {
    "Future": "laila::python::stdlib::NativeFuture",
    "ThreadPoolExecutor": "laila::python::stdlib::concurrent::ThreadPoolExecutor",
    "TimeoutError": "laila::python::errors::TimeoutError",
    "CancelledError": "laila::python::errors::CancelledError",
}

# `from <prelude module> import name`: the names that are not in the prelude
# under their bare spelling (the module itself is a value: `copy.deepcopy`)
# and need a `use` of their free-function twin [P2].
PRELUDE_FROM_NAMES = {
    ("copy", "deepcopy"): "laila::python::stdlib::deepcopy",
    ("functools", "reduce"): "laila::python::stdlib::reduce",
    ("collections", "deque"): "laila::python::stdlib::collections::deque",
    # `from threading import Thread, Barrier, ..`: the shim's classes, whose
    # `new(Args)` constructors the macro calls for `Thread(target=..)`.
    ("threading", "Thread"): "laila::python::stdlib::threading::Thread",
    ("threading", "Barrier"): "laila::python::stdlib::threading::Barrier",
    ("threading", "Lock"): "laila::python::stdlib::threading::Lock",
    ("threading", "RLock"): "laila::python::stdlib::threading::RLock",
    ("threading", "Event"): "laila::python::stdlib::threading::Event",
    ("threading", "Semaphore"): "laila::python::stdlib::threading::Semaphore",
    ("threading", "Condition"): "laila::python::stdlib::threading::Condition",
}

# Operator precedence (higher binds tighter), Python's table.
PREC = {
    "tuple": 0,
    "yield": 1,
    "lambda": 2,
    "ifexp": 2,
    "or": 3,
    "and": 4,
    "not": 5,
    "cmp": 6,
    "bor": 7,
    "bxor": 8,
    "band": 9,
    "shift": 10,
    "arith": 11,
    "term": 12,
    "factor": 13,
    "power": 14,
    "await": 15,
    "atom": 16,
}
BINOP = {
    ast.Add: ("+", "arith"),
    ast.Sub: ("-", "arith"),
    ast.Mult: ("*", "term"),
    ast.Div: ("/", "term"),
    ast.Mod: ("%", "term"),
    ast.LShift: ("<<", "shift"),
    ast.RShift: (">>", "shift"),
    ast.BitOr: ("|", "bor"),
    ast.BitXor: ("^", "bxor"),
    ast.BitAnd: ("&", "band"),
    ast.Pow: ("**", "power"),
    ast.FloorDiv: ("//", "term"),
    ast.MatMult: ("@", "term"),
}
CMPOP = {
    ast.Eq: "==",
    ast.NotEq: "!=",
    ast.Lt: "<",
    ast.LtE: "<=",
    ast.Gt: ">",
    ast.GtE: ">=",
    ast.Is: "is",
    ast.IsNot: "is not",
    ast.In: "in",
    ast.NotIn: "not in",
}


class Unsupported(Exception):
    pass


def rs_name(name: str) -> str:
    """Rust-keyword names become raw identifiers [P9]."""
    if name in RUST_KEYWORDS and name not in PY_SHARED and name not in ("self", "Self"):
        return f"r#{name}"
    return name


def rs_str(s: str, prefix: str = "") -> str:
    """Python string -> Rust double-quoted literal [P5]."""
    out = []
    for ch in s:
        if ch == "\\":
            out.append("\\\\")
        elif ch == '"':
            out.append('\\"')
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif ch == "\0":
            out.append("\\0")
        elif ord(ch) < 0x20 or ch == "\x7f":
            out.append(f"\\u{{{ord(ch):x}}}")
        else:
            out.append(ch)
    return prefix + '"' + "".join(out) + '"'


def rs_bytes(b: bytes) -> str:
    out = []
    for c in b:
        ch = chr(c)
        if ch == "\\":
            out.append("\\\\")
        elif ch == '"':
            out.append('\\"')
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif 0x20 <= c < 0x7F:
            out.append(ch)
        else:
            out.append(f"\\x{c:02x}")
    return 'b"' + "".join(out) + '"'


def rs_float(v: float) -> str:
    if v != v:
        return "f64::NAN"
    if v == float("inf"):
        return "f64::INFINITY"
    if v == float("-inf"):
        return "f64::NEG_INFINITY"
    r = repr(v)
    if "e" in r or "E" in r:
        mant, _, exp = r.partition("e")
        if "." not in mant:
            mant += ".0"
        return f"{mant}e{exp}"
    return r


# ---------------------------------------------------------------------------
# Expressions: Python AST -> Tier 2 text (verbatim Python shape) [P4..P7]
# ---------------------------------------------------------------------------


class ExprPrinter:
    """Prints expressions in Tier 2 syntax.

    Layout follows the source: a container literal or call that the Python
    author exploded over several lines is exploded the same way (one element
    per line, trailing comma), so Black-formatted input stays verbatim.
    """

    # `builder_tokens` override keyed by a *string literal*: constitution
    # source written inline in a test body (`Entry.variable(constitution=
    # "def build(m): ..")`). The literal is emitted through
    # `builder_token(<literal>, <fn>)`, which registers the native twin
    # under the exact text and yields the text [P31b]. Set per file.
    inline_tokens: dict[str, str] = {}

    # `hoist(lambda_node) -> name`: set by the emitter inside a function body.
    # A lambda the expression layer cannot spell (`*args` / `**kw` / a
    # partial default list) is emitted as a nested `#[pysig]` `fn` *before*
    # the statement that uses it and referenced by name [P26b].
    hoist: Any = None
    # Hoist *every* lambda (a module that imports `inspect` may introspect
    # one: `inspect.signature(lambda: churn())` [P15b]).
    hoist_all: bool = False

    def __init__(
        self,
        ctx: FnCtx | None = None,
        indent: int = 0,
        hoist: Any = None,
        hoist_all: bool = False,
    ):
        self.ctx = ctx
        self.indent = indent
        self.hoist = hoist
        self.hoist_all = hoist_all

    def __call__(self, node: ast.expr, prec: int = 0) -> str:
        meth = getattr(self, "e_" + type(node).__name__, None)
        if meth is None:
            raise Unsupported(f"expression {type(node).__name__}")
        text, p = meth(node)
        if p < prec:
            return f"({text})"
        return text

    @staticmethod
    def multiline(node: ast.AST) -> bool:
        return getattr(node, "end_lineno", None) is not None and node.end_lineno != node.lineno

    def exploded(self, open_: str, items: list[str], close: str) -> str:
        pad = "    " * (self.indent + 1)
        end = "    " * self.indent
        inner = "".join(f"{pad}{it},\n" for it in items)
        return f"{open_}\n{inner}{end}{close}"

    def nested(self) -> ExprPrinter:
        return ExprPrinter(self.ctx, self.indent + 1)

    # -- atoms --------------------------------------------------------------

    def e_Constant(self, n: ast.Constant):
        v = n.value
        if v is None:
            return "None", PREC["atom"]
        if v is True:
            return "True", PREC["atom"]
        if v is False:
            return "False", PREC["atom"]
        if isinstance(v, str):
            builder = self.inline_tokens.get(v)
            if builder is not None:
                return f"laila::entry::constitution::builder_token({rs_str(v)}, {builder})", PREC[
                    "atom"
                ]
            return rs_str(v), PREC["atom"]
        if isinstance(v, bytes):
            return rs_bytes(v), PREC["atom"]
        if isinstance(v, bool):
            return ("True" if v else "False"), PREC["atom"]
        if isinstance(v, int):
            return (str(v) if v >= 0 else f"-{-v}"), (PREC["atom"] if v >= 0 else PREC["factor"])
        if isinstance(v, float):
            return rs_float(v), PREC["atom"]
        if isinstance(v, complex):
            # An imaginary literal (`2j`) is not a Rust token: the `complex`
            # builtin spells the same value.
            return f"complex({rs_float(v.real)}, {rs_float(v.imag)})", PREC["atom"]
        if v is Ellipsis:
            raise Unsupported("Ellipsis")
        raise Unsupported(f"constant {v!r}")

    # `from laila.. import _LAILA_X as ALIAS`: a CONST-case alias of a laila
    # base class is spelled by the class name (the macro reads CONST-case
    # names as module values) [P2].
    class_renames: dict[str, str] = {}

    def e_Name(self, n: ast.Name):
        if self.ctx is not None and n.id in self.ctx.renames:
            return self.ctx.renames[n.id], PREC["atom"]
        if n.id in self.class_renames:
            return self.class_renames[n.id], PREC["atom"]
        return rs_name(n.id), PREC["atom"]

    def e_Attribute(self, n: ast.Attribute):
        return f"{self(n.value, PREC['atom'])}.{rs_name(n.attr)}", PREC["atom"]

    def e_Subscript(self, n: ast.Subscript):
        return f"{self(n.value, PREC['atom'])}[{self.subscript(n.slice)}]", PREC["atom"]

    def subscript(self, s: ast.expr) -> str:
        if isinstance(s, ast.Slice):
            lo = self(s.lower) if s.lower is not None else ""
            hi = self(s.upper) if s.upper is not None else ""
            if s.step is not None:
                return f"{lo}:{hi}:{self(s.step)}"
            return f"{lo}:{hi}"
        if isinstance(s, ast.Tuple):
            return ", ".join(self(e) for e in s.elts)
        return self(s)

    def e_Starred(self, n: ast.Starred):
        return f"*{self(n.value, PREC['atom'])}", PREC["atom"]

    def e_Tuple(self, n: ast.Tuple):
        if not n.elts:
            return "()", PREC["atom"]
        if len(n.elts) == 1:
            return f"({self(n.elts[0])},)", PREC["atom"]
        return "(" + ", ".join(self(e) for e in n.elts) + ")", PREC["atom"]

    def e_List(self, n: ast.List):
        if self.multiline(n) and n.elts:
            inner = self.nested()
            return self.exploded("[", [inner(e) for e in n.elts], "]"), PREC["atom"]
        return "[" + ", ".join(self(e) for e in n.elts) + "]", PREC["atom"]

    def e_Set(self, n: ast.Set):
        if self.multiline(n):
            inner = self.nested()
            return self.exploded("{", [inner(e) for e in n.elts], "}"), PREC["atom"]
        if len(n.elts) == 1:
            # `{x}` is a Rust block; the trailing comma marks the set literal
            return "{" + self(n.elts[0]) + ",}", PREC["atom"]
        return "{" + ", ".join(self(e) for e in n.elts) + "}", PREC["atom"]

    def e_Dict(self, n: ast.Dict):
        p = self.nested() if self.multiline(n) and n.keys else self
        items = []
        for k, v in zip(n.keys, n.values):
            if k is None:
                items.append(f"**{p(v, PREC['atom'])}")
            else:
                items.append(f"{p(k)}: {p(v)}")
        if self.multiline(n) and n.keys:
            return self.exploded("{", items, "}"), PREC["atom"]
        return "{" + ", ".join(items) + "}", PREC["atom"]

    # -- calls --------------------------------------------------------------

    def call_args(
        self, args: list[ast.expr], keywords: list[ast.keyword], printer: ExprPrinter | None = None
    ) -> list[str]:
        p = printer or self
        parts = [p(a) for a in args]
        for kw in keywords:
            if kw.arg is None:
                parts.append(f"**{p(kw.value, PREC['atom'])}")
            else:
                parts.append(f"{rs_name(kw.arg)}={p(kw.value)}")
        return parts

    def e_Call(self, n: ast.Call):
        if isinstance(n.func, ast.Name) and n.func.id == "super" and not n.args:
            raise Unsupported("super()")
        if (
            isinstance(n.func, ast.Attribute)
            and n.func.attr == "throw"
            and isinstance(n.func.value, ast.GeneratorExp)
            and len(n.args) == 1
            and self.hoist is not None
        ):
            # `(_ for _ in ()).throw(Exc(..))`: the "raise in an expression"
            # idiom -- a nested `fn` that raises, hoisted and referenced by
            # name (it only ever appears as a lambda body) [P26b].
            fdef = ast.FunctionDef(
                name="__raise",
                args=ast.arguments(
                    posonlyargs=[], args=[], kwonlyargs=[], kw_defaults=[], defaults=[]
                ),
                body=[ast.Raise(exc=n.args[0], cause=None)],
                decorator_list=[],
                returns=None,
                type_comment=None,
            )
            return f"{self.hoist(fdef)}()", PREC["atom"]
        if (
            isinstance(n.func, ast.Attribute)
            and isinstance(n.func.value, ast.Call)
            and isinstance(n.func.value.func, ast.Name)
            and n.func.value.func.id == "super"
            and not n.func.value.args
        ):
            # `super().m(..)` on a test case deriving from a test case of this
            # file: the base method is carried as `<Base>__m` [P12d].
            target = self.ctx.super_methods.get(n.func.attr) if self.ctx is not None else None
            if target is None:
                raise Unsupported(f"super().{n.func.attr}()")
            parts = self.call_args(n.args, n.keywords)
            return f"self.{target}({', '.join(parts)})", PREC["atom"]
        f = self(n.func, PREC["atom"])
        # Exploded only when the *argument list* spans lines (not just a
        # multi-line single argument such as a dict literal).
        args_multiline = (
            self.multiline(n)
            and (
                len(n.args) + len(n.keywords) > 1
                or any(a.lineno != n.func.end_lineno for a in n.args)
                or any(k.value.lineno != n.func.end_lineno for k in n.keywords)
            )
            and not (
                len(n.args) + len(n.keywords) == 1
                and self.multiline((n.args or [k.value for k in n.keywords])[0])
                and (n.args or [k.value for k in n.keywords])[0].lineno == n.func.end_lineno
            )
        )
        if args_multiline:
            parts = self.call_args(n.args, n.keywords, self.nested())
            return f"{f}{self.exploded('(', parts, ')')}", PREC["atom"]
        return f"{f}({', '.join(self.call_args(n.args, n.keywords))})", PREC["atom"]

    # -- operators ----------------------------------------------------------

    def e_BinOp(self, n: ast.BinOp):
        sym, lvl = BINOP[type(n.op)]
        p = PREC[lvl]
        if isinstance(n.op, ast.FloorDiv):
            # `//` is a Rust comment: `a // b` -> `floordiv(a, b)` [P6]
            return f"floordiv({self(n.left)}, {self(n.right)})", PREC["atom"]
        if isinstance(n.op, ast.MatMult):
            return f"matmul({self(n.left)}, {self(n.right)})", PREC["atom"]
        if isinstance(n.op, ast.Pow):
            # right-associative
            return f"{self(n.left, p + 1)} ** {self(n.right, p)}", p
        return f"{self(n.left, p)} {sym} {self(n.right, p + 1)}", p

    def e_UnaryOp(self, n: ast.UnaryOp):
        if isinstance(n.op, ast.Not):
            return f"not {self(n.operand, PREC['not'])}", PREC["not"]
        if isinstance(n.op, ast.USub):
            return f"-{self(n.operand, PREC['factor'])}", PREC["factor"]
        if isinstance(n.op, ast.UAdd):
            return f"+{self(n.operand, PREC['factor'])}", PREC["factor"]
        return f"!{self(n.operand, PREC['factor'])}", PREC["factor"]

    def e_BoolOp(self, n: ast.BoolOp):
        key = "and" if isinstance(n.op, ast.And) else "or"
        p = PREC[key]
        return f" {key} ".join(self(v, p + 1) for v in n.values), p

    def e_Compare(self, n: ast.Compare):
        p = PREC["cmp"]
        if len(n.ops) > 1:
            # `a == b == c` -> `a == b and b == c` (the middle operands are
            # evaluated twice, which is fine for the pure expressions tests
            # chain) -- the rewriter reads a chain left-to-right as Rust does.
            pa = PREC["and"]
            operands = [n.left] + list(n.comparators)
            links = []
            for i, op in enumerate(n.ops):
                links.append(
                    f"{self(operands[i], p + 1)} {CMPOP[type(op)]} {self(operands[i + 1], p + 1)}"
                )
            return " and ".join(links), pa
        parts = [self(n.left, p + 1)]
        for op, c in zip(n.ops, n.comparators):
            parts.append(CMPOP[type(op)])
            parts.append(self(c, p + 1))
        return " ".join(parts), p

    def e_IfExp(self, n: ast.IfExp):
        p = PREC["ifexp"]
        return f"{self(n.body, p + 1)} if {self(n.test, p + 1)} else {self(n.orelse, p)}", p

    def _captures_locals(self, n: ast.Lambda) -> bool:
        """Does the lambda read a local of the enclosing function (or bind
        one through a `name=name` default)? A hoisted `fn` has no captures,
        so such a lambda stays a closure even under `hoist_all` [P15b]."""
        own = {a.arg for a in n.args.args + n.args.kwonlyargs + n.args.posonlyargs}
        if n.args.vararg:
            own.add(n.args.vararg.arg)
        if n.args.kwarg:
            own.add(n.args.kwarg.arg)
        for d in n.args.defaults + [d for d in n.args.kw_defaults if d is not None]:
            if any(isinstance(x, ast.Name) for x in ast.walk(d)):
                return True
        if self.ctx is None:
            return False
        locals_ = set(self.ctx.params) | set(self.ctx.declared) | set(self.ctx.assigned_counts)
        for x in ast.walk(n.body):
            if (
                isinstance(x, ast.Name)
                and isinstance(x.ctx, ast.Load)
                and x.id not in own
                and x.id in locals_
            ):
                return True
        return False

    def e_Lambda(self, n: ast.Lambda):
        args = n.args.args
        defaults = n.args.defaults
        if self.hoist_all and self.hoist is not None and not self._captures_locals(n):
            return self.hoist(n), PREC["atom"]
        # `lambda i=i: body` -- the Python idiom that freezes a loop variable
        # at definition time. A Rust closure clones its captures before the
        # `move`, so the same effect is `lambda: body` over the captured
        # name [P26]; only this bare `name=name` shape (and only when every
        # parameter has such a default) is rewritten.
        if (
            defaults
            and len(defaults) == len(args)
            and all(isinstance(d, ast.Name) and d.id == a.arg for a, d in zip(args, defaults))
        ):
            args, defaults = [], []
        params = ", ".join(rs_name(a.arg) for a in args)
        if n.args.vararg or n.args.kwarg or n.args.kwonlyargs:
            if self.hoist is not None:
                return self.hoist(n), PREC["atom"]
            raise Unsupported("lambda with varargs")
        head = f"lambda {params}" if params else "lambda"
        if defaults and len(defaults) != len(args) and self.hoist is not None:
            return self.hoist(n), PREC["atom"]
        body = f"{head}: {self(n.body, PREC['lambda'])}"
        if defaults:
            # `lambda v=expr: body`: the default is evaluated *now* and
            # bound to the parameter -- `functools.partial(lambda v: body,
            # expr)` says exactly that [P26]. (Only every-parameter
            # defaults, called with no arguments, keep Python's meaning.)
            if len(defaults) != len(args):
                raise Unsupported("lambda with a partial default list")
            bound = ", ".join(self(d, 0) for d in defaults)
            return f"functools.partial(({body}), {bound})", PREC["atom"]
        return body, PREC["lambda"]

    def e_Await(self, n: ast.Await):
        return f"await {self(n.value, PREC['await'])}", PREC["await"]

    def e_NamedExpr(self, n: ast.NamedExpr):
        raise Unsupported("walrus")

    def e_Yield(self, n: ast.Yield):
        raise Unsupported("yield")

    def e_YieldFrom(self, n: ast.YieldFrom):
        raise Unsupported("yield from")

    # -- comprehensions -----------------------------------------------------

    def gens(self, generators: list[ast.comprehension]) -> str:
        out = []
        for g in generators:
            if g.is_async:
                raise Unsupported("async comprehension")
            out.append(f"for {self.target(g.target)} in {self(g.iter, PREC['or'])}")
            for c in g.ifs:
                out.append(f"if {self(c, PREC['or'])}")
        return " ".join(out)

    def target(self, t: ast.expr) -> str:
        if isinstance(t, ast.Name):
            return rs_name(t.id)
        if isinstance(t, ast.Tuple):
            return "(" + ", ".join(self.target(e) for e in t.elts) + ")"
        if isinstance(t, ast.Starred):
            raise Unsupported("starred target")
        return self(t)

    def e_ListComp(self, n: ast.ListComp):
        return f"[{self(n.elt)} {self.gens(n.generators)}]", PREC["atom"]

    def e_SetComp(self, n: ast.SetComp):
        return f"{{{self(n.elt)} {self.gens(n.generators)}}}", PREC["atom"]

    def e_GeneratorExp(self, n: ast.GeneratorExp):
        return f"({self(n.elt)} {self.gens(n.generators)})", PREC["atom"]

    def e_DictComp(self, n: ast.DictComp):
        return f"{{{self(n.key)}: {self(n.value)} {self.gens(n.generators)}}}", PREC["atom"]

    # -- f-strings ----------------------------------------------------------

    def e_JoinedStr(self, n: ast.JoinedStr):
        # `f"..."` -> `f "..."` (a space: `f"` is not a Rust literal prefix) [P7].
        # Placeholders keep their Python expression; nested quotes are `\"`.
        body = []
        for part in n.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                body.append(rs_str(part.value)[1:-1].replace("{", "{{").replace("}", "}}"))
            elif isinstance(part, ast.FormattedValue):
                inner = self(part.value)
                inner = inner.replace("\\", "\\\\").replace('"', '\\"')
                conv = {-1: "", 114: "!r", 115: "!s", 97: "!a"}[part.conversion]
                spec = ""
                if part.format_spec is not None:
                    spec_text = "".join(
                        p.value if isinstance(p, ast.Constant) else "{" + self(p.value) + "}"
                        for p in part.format_spec.values
                    )
                    spec = ":" + spec_text
                body.append("{" + inner + conv + spec + "}")
            else:
                raise Unsupported("f-string part")
        return 'f "' + "".join(body) + '"', PREC["atom"]

    def e_TemplateStr(self, n):  # 3.14 t-strings
        raise Unsupported("t-string")


# ---------------------------------------------------------------------------
# Statements
# ---------------------------------------------------------------------------


@dataclass
class FnCtx:
    """Per-function name bookkeeping for `let` / `let mut` [P3]."""

    params: set[str]
    assigned_counts: dict[str, int]
    declared: set[str] = field(default_factory=set)
    # Python name -> Rust name for locals that must not collide with a
    # module static of the same name (the module-block locals, P2d).
    renames: dict[str, str] = field(default_factory=dict)
    is_method: bool = False
    is_async: bool = False
    returns_value: bool = False
    node: ast.AST | None = None
    # `super().m(..)` -> `self.<Base>__m(..)`: the base method the class
    # overrides, copied under its class-qualified name [P12d].
    super_methods: dict[str, str] = field(default_factory=dict)


@dataclass
class ModuleBlock:
    """A module-level `if` / `try` (with the plain assignments it reassigns)
    folded into one lazily-run block [P2d]."""

    stmts: list[ast.stmt]

    @property
    def lineno(self) -> int:
        return self.stmts[0].lineno


class AssignCounter(ast.NodeVisitor):
    """Counts plain-name assignment targets in a function body (not nested defs)."""

    def __init__(self):
        self.counts: dict[str, int] = {}
        self.returns_value = False
        # every `return` returns `self` (`__enter__`): the method is `-> Self`
        self.returns_self = True

    def bump(self, t: ast.expr):
        if isinstance(t, ast.Name):
            self.counts[t.id] = self.counts.get(t.id, 0) + 1
        elif isinstance(t, (ast.Tuple, ast.List)):
            for e in t.elts:
                self.bump(e)

    def visit_Assign(self, n: ast.Assign):
        for t in n.targets:
            self.bump(t)
        self.generic_visit(n)

    def visit_AnnAssign(self, n: ast.AnnAssign):
        self.bump(n.target)
        self.generic_visit(n)

    def visit_AugAssign(self, n: ast.AugAssign):
        self.bump(n.target)
        self.generic_visit(n)

    def visit_For(self, n: ast.For):
        # loop targets are bound by the `for` itself
        self.generic_visit(n)

    def visit_With(self, n: ast.With):
        for it in n.items:
            if it.optional_vars is not None:
                self.bump(it.optional_vars)
        self.generic_visit(n)

    def visit_NamedExpr(self, n: ast.NamedExpr):
        self.bump(n.target)
        self.generic_visit(n)

    def visit_Return(self, n: ast.Return):
        if n.value is not None and not (
            isinstance(n.value, ast.Constant) and n.value.value is None
        ):
            self.returns_value = True
        if not (isinstance(n.value, ast.Name) and n.value.id == "self"):
            self.returns_self = False

    def visit_FunctionDef(self, n):  # do not descend
        pass

    visit_AsyncFunctionDef = visit_FunctionDef
    visit_ClassDef = visit_FunctionDef
    visit_Lambda = visit_FunctionDef


class Emitter:
    def __init__(self, source: str, rel_path: str):
        self.source = source
        self.lines = source.splitlines()
        self.rel_path = rel_path
        self.introspects = False
        self.out: list[str] = []
        # module constant -> (index of its builder's `return` line in `out`,
        # the initializer text), for `NAME.pop(..)` folding [P11b]
        self.const_return_idx: dict[str, tuple[int, str]] = {}
        self.indent = 0
        self.comments = self._collect_comments(source)
        self.last_line = 0
        self.unittest = False
        self.uses: list[str] = []
        self.notes: list[str] = []
        self.unsupported: list[str] = []
        self.fn_stack: list[FnCtx] = []
        self.lambda_counter = 0
        self.class_stack: list[str] = []
        # Plain `py_class!` classes (not TestCases, not models): their
        # instances are dynamic handles, so their methods are only reached
        # through `__call_method__` and every parameter lowers to `Arg`.
        self.dyn_param_classes: set[str] = set()
        self.skip_tests: set[str] = set()
        self.skip_tests_reason: str = ""
        self.skip_imports: set[str] = set()
        # `present_imports` override: optional dependencies the Rust twin
        # satisfies natively (`import serial` -> the UART carrier needs no
        # pyserial): the optional-import `try` takes its *body* branch [P18].
        self.present_imports: set[str] = set()
        # Module-level helpers dropped by the `skip_functions` override [P18].
        self.skip_functions: set[str] = set()
        # Test-local subclasses replaced by a Rust type (`class_aliases` override) [P18].
        self.class_aliases: dict[str, str] = {}
        # `dyn_types` override: annotated class names lowered to `Arg`
        # (the file drives those objects through the dynamic protocol) [P18e].
        self.dyn_types: set[str] = set()
        # Per-name reasons from the mapping form of `skip_tests` / `skip_functions`.
        self.skip_reasons: dict[str, str] = {}
        self.probe_flags: set[str] = set()
        # Module-level `if` / `try` blocks emitted so far [P2d].
        self.block_counter = 0
        # Names already bound by a `use` line: a second `from .. import name`
        # (a function-level import hoisted to module level, or the same
        # class reachable through two module paths) is a comment [P2].
        self.imported_names: set[str] = set()
        # `builder_tokens` override: module constant -> Rust builder path.
        # The constant's text is a complex-constitution source string; the
        # Rust builder is registered under it when the constant is first
        # read [P31].
        self.builder_tokens: dict[str, str] = {}
        # `builder_functions` override: module-level function -> Rust builder
        # path. The function *computes* a complex-constitution source string
        # (one text per argument); every `return <expr>` of it registers the
        # Rust builder under the returned text [P31c].
        self.builder_functions: dict[str, str] = {}
        # `builder_tokens` override keyed `<function>.<local>`: a local of
        # that function assigned a *computed* source text (an f-string
        # carrying a gid); the assignment registers the Rust builder under
        # the text it evaluates to [P31d].
        self.builder_locals: dict[str, str] = {}
        # `import_twins` override: imported laila name -> Rust path of a
        # Python-shaped twin (`fn(Args) -> Result<Arg>` / a value) that the
        # `use` binds under the Python name instead of the typed original.
        self.import_twins: dict[str, str] = {}
        # `--script`: top-level statements become `fn main()` [P33].
        self.script = False
        # `child_scripts` override: module constant -> Rust child function.
        # The constant's text is Python source the test runs in a child
        # interpreter (`subprocess.run([sys.executable, "-c", SRC.format(..)])`);
        # the Rust twin re-executes the test binary on a generated child
        # entry [P32].
        self.child_scripts: dict[str, str] = {}
        # `#[test]` items emitted after the `laila::python!` block (child
        # entries [P32]).
        self.trailer: list[str] = []
        # Python *modules* imported as values (`from laila.macros import strings`):
        # the macro reads their attributes through `__getattr__` [P2].
        self.modules: list[str] = []
        self.tree: ast.Module = ast.Module(body=[], type_ignores=[])
        # Dynamic test registration [P19]: `class -> [(module-level def, registered name)]`,
        # the registration helper(s) and the module-level defs they attach.
        self.registered: dict[str, list[tuple[ast.FunctionDef, str]]] = {}
        self.registration_helpers: dict[str, str] = {}
        self.registered_defs: set[str] = set()
        # Module-level statements consumed by a registration idiom [P19c]
        # (class-attribute exposures, decorator registrars, unrolled matrix
        # loops), by `id(stmt)`.
        self.registration_stmts: set[int] = set()
        # Optional-dependency flags that are `False` in Rust (`HAVE_TORCH`
        # from a `try: import torch` whose module has no Rust twin): a
        # module-level `if FLAG:` block never runs [P2e].
        self.dead_flags: set[str] = set()
        # `unittest.TestCase` subclasses of this file, by name: a class that
        # derives from one is a test case too and inherits its methods [P12b].
        self.testcase_classes: dict[str, ast.ClassDef] = {}
        # Where the source lives and where the overrides are: a sibling
        # helper module (`from _helpers import ..`) is resolved next to the
        # file and its override may name a hand-written Rust twin [P18c].
        self.src_path: str | None = None
        self.overrides_dir: str | None = None
        self.root: str | None = None
        # `skip_unless_x = unittest.skipUnless(FLAG, reason)` aliases (of
        # this file or a sibling helper): `@skip_unless_x` on a test method
        # becomes a leading `self.skipTest(reason)` guard [P2f].
        self.skip_aliases: dict[str, tuple[ast.expr, str, bool]] = {}
        # Mixin classes taken from a sibling helper module [P12e].
        self.sibling_classes: set[str] = set()

    # -- plumbing -----------------------------------------------------------

    @staticmethod
    def _collect_comments(source: str) -> dict[int, str]:
        comments: dict[int, str] = {}
        try:
            for tok in tokenize.generate_tokens(io.StringIO(source).readline):
                if tok.type == tokenize.COMMENT:
                    comments[tok.start[0]] = tok.string
        except tokenize.TokenError:
            pass
        return comments

    def line(self, s: str = ""):
        self.out.append(("    " * self.indent + s) if s else "")

    def flush_comments(self, upto: int, inline_line: int | None = None):
        """Emit standalone `# ...` lines before `upto` as `// ...` [P10]."""
        for ln in sorted(self.comments):
            if self.last_line < ln < upto:
                text = self.comments.pop(ln)
                src = self.lines[ln - 1] if ln - 1 < len(self.lines) else ""
                if src.strip().startswith("#"):
                    self.line("//" + text[1:])
        # blank-line preservation: one blank line if the source had a gap
        if self.last_line and upto > self.last_line + 1 and self.out and self.out[-1] != "":
            gap_lines = self.lines[self.last_line : upto - 1]
            if any(not ln.strip() for ln in gap_lines):
                self.out.append("")

    def trailing_comment(self, node: ast.stmt) -> str:
        end = getattr(node, "end_lineno", node.lineno)
        c = self.comments.pop(end, None)
        if c is None:
            return ""
        src = self.lines[end - 1]
        if src.strip().startswith("#"):
            # it was a standalone comment on the last line of a block; keep it
            self.comments[end] = c
            return ""
        return "  //" + c[1:]

    def mark(self, node: ast.stmt):
        self.last_line = getattr(node, "end_lineno", node.lineno)

    def expr(self, e: ast.expr) -> str:
        hoist = self.hoist_lambda if self.fn_stack else None
        return ExprPrinter(
            self.fn_stack[-1] if self.fn_stack else None, self.indent, hoist, self.introspects
        )(e)

    def hoist_lambda(self, n: ast.Lambda) -> str:
        """`lambda *args, **kw: body` inside a function body: a nested
        `#[pysig]` `fn __lambda_<n>(..)` emitted before the statement that
        uses it; the expression becomes the name [P26b]. (The statement's
        own lines are emitted after its expressions are printed, so the
        `fn` lands first.)"""
        self.lambda_counter += 1
        name = f"__lambda_{self.lambda_counter}"
        if isinstance(n, ast.FunctionDef):
            fdef = n
            fdef.name = name
        else:
            fdef = ast.FunctionDef(
                name=name,
                args=n.args,
                body=[ast.Return(value=n.body)],
                decorator_list=[],
                returns=None,
                type_comment=None,
            )
        if hasattr(ast.FunctionDef, "type_params"):
            fdef.type_params = []
        ast.copy_location(fdef, n)
        for node in ast.walk(fdef):
            if not hasattr(node, "lineno"):
                ast.copy_location(node, n)
        ast.fix_missing_locations(fdef)
        self.function(fdef, in_class=None)
        return name

    def unsupported_stmt(self, node: ast.stmt, reason: str):
        self.unsupported.append(f"{self.rel_path}:{node.lineno}: {reason}")
        self.line(f"unsupported!({rs_str(reason)});")

    # -- module -------------------------------------------------------------

    def module(self, tree: ast.Module) -> str:
        self.tree = tree
        # `import inspect` anywhere in the file: nested defs are function
        # values with signatures [P15b].
        self.introspects = any(
            isinstance(node, ast.Import) and any(a.name == "inspect" for a in node.names)
            for node in ast.walk(tree)
        )
        # Functions / methods this file calls with keyword arguments
        # (`self._run(pair, "tcp", per_nal=False)`): they bind their
        # parameters through `#[pysig]` so the call can pass an `Args` bag [P15].
        self.kw_called: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and node.keywords:
                if (
                    isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id in ("self", "cls")
                ):
                    self.kw_called.add(node.func.attr)
                elif isinstance(node.func, ast.Name):
                    self.kw_called.add(node.func.id)
        body = list(tree.body)
        # module docstring
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            doc = body.pop(0)
            for ln in doc.value.value.strip("\n").splitlines():
                self.line(f"//! {ln}".rstrip())
            self.mark(doc)
        # imports first (they decide the header)
        items: list[ast.stmt] = []
        for st in body:
            if isinstance(st, (ast.Import, ast.ImportFrom)):
                self.import_(st)
                self.mark(st)
            else:
                items.append(st)
        self._scan_dead_flags(items)
        self._scan_registrations(items)
        if self.script:
            # `--script` [P33]: a program, not a module. Its top-level
            # *statements* (what a notebook cell or a script runs at import
            # time) become the body of `fn main()`, in order, after the
            # module's `def`s / `class`es; the rewriter gives `main` the
            # `Result<()>` return that lets `?` propagate like an uncaught
            # Python exception.
            top: list[ast.stmt] = []
            prog: list[ast.stmt] = []
            # Names bound by program statements so far. A `def` written
            # after one of those that reads such a name closes over a
            # module global in Python; it becomes a nested `fn` inside
            # `main` (closing over the local) rather than a module item.
            bound: set[str] = set()
            for st in items:
                if isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef)) and bound & {
                    n.id
                    for n in ast.walk(st)
                    if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
                }:
                    prog.append(st)
                elif isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    top.append(st)
                elif isinstance(st, ast.If) and self._is_main_guard(st):
                    prog.extend(st.body)
                    for s in st.body:
                        bound |= {
                            n.id
                            for n in ast.walk(s)
                            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)
                        }
                else:
                    prog.append(st)
                    bound |= {
                        n.id
                        for n in ast.walk(st)
                        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)
                    }
            if prog:
                main = ast.FunctionDef(
                    name="main",
                    args=ast.arguments(
                        posonlyargs=[],
                        args=[],
                        vararg=None,
                        kwonlyargs=[],
                        kw_defaults=[],
                        kwarg=None,
                        defaults=[],
                    ),
                    body=prog,
                    decorator_list=[],
                    returns=None,
                    type_params=[],
                )
                ast.copy_location(main, prog[0])
                main.end_lineno = getattr(prog[-1], "end_lineno", prog[-1].lineno)
                top.append(main)
            items = top
        inner: list[str] = []
        saved = self.out
        self.out = inner
        self.indent = 1
        for st in self._fold_module_blocks(items):
            if isinstance(st, ModuleBlock):
                self.module_block(st)
                continue
            if self._is_registration_stmt(st):
                # folded into the class it registers on [P19]
                self.mark(st)
                continue
            self.module_stmt(st)
        self.out = saved
        self.indent = 0

        header = [
            f"//! Generated by py2rs from `{self.rel_path}`. Do not edit; regenerate.",
            "#![allow(non_snake_case, non_upper_case_globals, dead_code, unused_variables, unused_imports, unused_mut, unused_assignments, clippy::all)]",
        ]
        header += [
            "",
            "use laila::prelude::*;",
        ]
        header += sorted(set(self.uses))
        header.append("")
        while inner and inner[-1] == "":
            inner.pop()
        opts = ["    #![unittest]"] if self.unittest else []
        if self.modules:
            opts.append(f"    #![modules({', '.join(sorted(set(self.modules)))})]")
        if self.class_aliases:
            # instances of an aliased class are attribute-dynamic (the
            # Python subclass's own fields are read through `__getattr__`)
            opts.append(f"    #![dyn_classes({', '.join(sorted(self.class_aliases))})]")
        block = (
            ["laila::python! {"]
            + opts
            + ([""] if opts and inner and inner[0] != "" else [])
            + inner
            + ["}"]
        )
        text = "\n".join(header + block + self.trailer) + "\n"
        if self.script:
            # A program is its own crate root: no `#[rustfmt::skip] mod x;`
            # declaration keeps `cargo fmt --check` out of it, so hand the
            # header to rustfmt (it leaves the brace-delimited macro body
            # alone). Idempotent, so `--check` stays byte-exact.
            text = rustfmt(text)
        return text

    def import_(self, st: ast.Import | ast.ImportFrom):
        """`import`/`from .. import` -> `use` lines or prelude no-ops [P2]."""
        if isinstance(st, ast.Import):
            for a in st.names:
                root = a.name.split(".")[0]
                if a.name == "unittest":
                    self.unittest = True
                elif a.name in self.skip_imports:
                    self.uses.append(f"// py2rs: `import {a.name}` skipped (override)")
                elif root in self.present_imports:
                    self.uses.append(
                        f"// py2rs: `import {a.name}`: the Rust twin needs no such dependency (override)"
                    )
                elif a.name == "laila" or a.name.startswith("laila."):
                    if a.name != "laila":
                        # `import laila.macros.defaults as d`: the alias names
                        # the module *and* its value twin (a `static` of the
                        # same name next to the `mod`), so `dir(d)` works [P2].
                        alias = (
                            f" as {rs_name(a.asname)}" if a.asname and a.asname != a.name else ""
                        )
                        self.uses.append(f"use {a.name.replace('.', '::')}{alias};")
                elif root in PRELUDE_MODULES:
                    # `import shutil as _shutil`: the stdlib shim is a module
                    # *value* (`laila::python::stdlib::shutil`), so the alias
                    # is a `use .. as ..` of it [P2]. (`import numpy as np` is
                    # the prelude's own `np`.)
                    # The alias is renamed back to the module at every use
                    # (`_uuid.UUID` -> `uuid.UUID`) so the rewriter's
                    # stdlib resolution (`isinstance(x, uuid.UUID)`,
                    # `uuid.uuid4()`) sees the canonical name.
                    if a.asname and a.asname != a.name and a.asname != "np":
                        # a dotted module (`xml.etree.ElementTree as ET`)
                        # resolves through its root (`xml.ParseError`)
                        ExprPrinter.class_renames[a.asname] = root
                elif (twin := self._sibling_twin(a.name)) is not None:
                    # `import _hevc as H`: a sibling helper module with a
                    # hand-written twin that is both a `mod` and a value of
                    # the same name (`laila_tests::hevc`), so `H.generate(..)`
                    # is a method of the module value [P18c].
                    bound = rs_name(a.asname or a.name)
                    self.uses.append(
                        f"use {twin} as {bound}; // `import {a.name}`: the hand-written twin [P18c]"
                    )
                    self.imported_names.add(bound)
                else:
                    self.notes.append(f"import {a.name}: not in prelude")
                    self.uses.append(f"// py2rs: `import {a.name}` has no Rust twin")
            return
        mod = st.module or ""
        if st.level:
            mod = "super::" * st.level + mod.replace(".", "::")
            self.uses.append(
                f"use {mod}::{{{', '.join(rs_name(a.asname or a.name) for a in st.names)}}};"
            )
            return
        if mod == "unittest" or mod.startswith("unittest."):
            self.unittest = True
            return
        if mod in self.skip_imports:
            # `skip_imports` override: the module has no Rust twin *yet* [P18]
            self.uses.append(f"// py2rs: `from {mod} import ...` skipped (override)")
            return
        if mod == "laila" or mod.startswith("laila."):
            mod = LAILA_MODULE_COLLAPSE.get(mod, mod)
            names = []
            for a in st.names:
                n = rs_name(a.name)
                if a.asname and a.name.startswith("_LAILA_") and a.asname.isupper():
                    # `_LAILA_IDENTIFIABLE_OBJECT as _IO`: keep the class name
                    ExprPrinter.class_renames[a.asname] = a.name
                    a = ast.alias(name=a.name, asname=None)
                bound = rs_name(a.asname or a.name)
                if bound in self.imported_names:
                    # the same name through another path (or hoisted twice)
                    self.uses.append(f"// py2rs: `from {mod} import {a.name}` already imported")
                    continue
                self.imported_names.add(bound)
                if a.name in self.import_twins:
                    # `import_twins` override: the Python-shaped twin
                    self.uses.append(
                        f"use {self.import_twins[a.name]} as {bound}; // `{a.name}`: Python-shaped twin (override)"
                    )
                    continue
                names.append(f"{n} as {rs_name(a.asname)}" if a.asname else n)
                if self._is_laila_module(mod, a.name):
                    self.modules.append(rs_name(a.asname or a.name))
            if not names:
                return
            self.uses.append(
                f"use {mod.replace('.', '::')}::{{{', '.join(names)}}};"
                if len(names) > 1
                else f"use {mod.replace('.', '::')}::{names[0]};"
            )
            return
        if mod.split(".")[0] in PRELUDE_MODULES:
            for a in st.names:
                twin = PRELUDE_FROM_NAMES.get((mod, a.name))
                bound = rs_name(a.asname or a.name)
                if twin is None or bound in self.imported_names:
                    continue
                self.imported_names.add(bound)
                self.uses.append(f"use {twin} as {bound};" if a.asname else f"use {twin};")
            return
        if mod == "concurrent.futures":
            for a in st.names:
                twin = CONCURRENT_FUTURES_NAMES.get(a.name)
                if twin is None:
                    self.uses.append(f"// py2rs: `from {mod} import {a.name}` has no Rust twin")
                    continue
                bound = rs_name(a.asname or a.name)
                if bound in self.imported_names:
                    continue
                self.imported_names.add(bound)
                self.uses.append(f"use {twin} as {bound};" if a.asname else f"use {twin};")
            return
        if mod == "tests.functional.pools.base.compdata_roundtrip":
            # The hand-written twin `laila_tests::compdata_roundtrip` [P19b]:
            # `register_dtype_matrix` is the `dtype_matrix!` macro (folded into
            # the registration statement); the helpers are plain functions.
            helpers = [
                rs_name(a.asname or a.name) for a in st.names if a.name != "register_dtype_matrix"
            ]
            if helpers:
                self.uses.append(
                    f"use laila_tests::compdata_roundtrip::{{{', '.join(helpers)}}};"
                    if len(helpers) > 1
                    else f"use laila_tests::compdata_roundtrip::{helpers[0]};"
                )
            return
        if self._sibling_import(mod, st):
            return
        self.notes.append(f"from {mod} import ..: not in prelude")
        self.uses.append(f"// py2rs: `from {mod} import ...` has no Rust twin")

    @staticmethod
    def _skip_alias(st: ast.stmt) -> tuple[str, ast.expr, str, bool] | None:
        """`name = unittest.skipUnless(FLAG, "reason")` / `unittest.skipIf(..)`
        at module level -> `(name, FLAG, reason, unless)` [P2f]."""
        if not (
            isinstance(st, ast.Assign)
            and len(st.targets) == 1
            and isinstance(st.targets[0], ast.Name)
        ):
            return None
        v = st.value
        if not (
            isinstance(v, ast.Call)
            and isinstance(v.func, ast.Attribute)
            and isinstance(v.func.value, ast.Name)
            and v.func.value.id == "unittest"
            and v.func.attr in ("skipUnless", "skipIf")
            and len(v.args) == 2
            and isinstance(v.args[1], ast.Constant)
            and isinstance(v.args[1].value, str)
        ):
            return None
        return st.targets[0].id, v.args[0], v.args[1].value, v.func.attr == "skipUnless"

    def _sibling_twin(self, mod: str) -> str | None:
        """The `twin:` override of a sibling module `mod.py` (next to this
        file), or `None` when there is no such file / twin [P18c]."""
        if self.src_path is None or "." in mod:
            return None
        sibling = os.path.join(os.path.dirname(self.src_path), mod + ".py")
        if not os.path.isfile(sibling):
            return None
        root = os.path.abspath(self.root) if self.root else os.getcwd()
        rel = os.path.relpath(os.path.abspath(sibling), root).replace(os.sep, "/")
        twin = load_override(self.overrides_dir, rel).get("twin")
        return twin if isinstance(twin, str) and twin else None

    def _sibling_import(self, mod: str, st: ast.ImportFrom) -> bool:
        """`from _helpers import A, f, skip_unless_x` where `_helpers.py` sits
        next to this file and its override names a hand-written Rust twin
        (`twin: laila_tests::helpers`) [P18c]: the twin's items come in with
        a glob `use`; a base-less class of the sibling is a mixin whose
        methods are copied into the test cases deriving from it [P12e]; a
        `unittest.skipUnless` alias becomes a method guard [P2f]."""
        if self.src_path is None or "." in mod:
            return False
        sibling = os.path.join(os.path.dirname(self.src_path), mod + ".py")
        if not os.path.isfile(sibling):
            return False
        root = os.path.abspath(self.root) if self.root else os.getcwd()
        rel = os.path.relpath(os.path.abspath(sibling), root).replace(os.sep, "/")
        ov = load_override(self.overrides_dir, rel)
        twin = ov.get("twin")
        if not isinstance(twin, str) or not twin:
            return False
        try:
            tree = ast.parse(open(sibling, encoding="utf-8").read(), filename=sibling)
        except SyntaxError:
            return False
        self.uses.append(f"use {twin}::*; // `from {mod} import ..`: the hand-written twin [P18c]")
        for a in st.names:
            self.imported_names.add(rs_name(a.asname or a.name))
        for s in tree.body:
            if (
                isinstance(s, ast.ClassDef)
                and not s.bases
                and not any(isinstance(b, ast.FunctionDef) and b.name == "__init__" for b in s.body)
            ):
                self.testcase_classes[s.name] = s
                self.sibling_classes.add(s.name)
            alias = self._skip_alias(s)
            if alias is not None:
                name, flag, reason, unless = alias
                self.skip_aliases[name] = (flag, reason, unless)
        return True

    def _is_laila_module(self, mod: str, name: str) -> bool:
        """`from laila.pkg import name` names a *module* (not a function or a
        class) when the file reads attributes of `name` (`name.attr`) and never
        calls it directly; the Rust twin is both the module and a module-object
        value of the same name [P2]. Decided from the source alone so the
        output does not depend on the file system."""
        if name != name.lower():
            return False
        used_as_base = False
        for node in ast.walk(self.tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == name
            ):
                return False
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == name
            ):
                used_as_base = True
        return used_as_base

    def skip_comment(self, name: str) -> str:
        """`// py2rs: `name` skipped (override: reason)` for a dropped def [P18]."""
        reason = self.skip_reasons.get(name) or self.skip_tests_reason
        reason = f": {reason}" if reason else ""
        return f"// py2rs: `{name}` skipped (override{reason})"

    def module_stmt(self, st: ast.stmt):
        self.flush_comments(st.lineno)
        if isinstance(st, ast.ClassDef) and st.name in self.class_aliases:
            # `class_aliases` override: a test-local subclass whose overrides
            # have no Rust twin stands in for the aliased Rust type (the
            # `use .. as Name;` is emitted with the imports) [P18].
            self.line(
                f"// py2rs: class `{st.name}` aliased to `{self.class_aliases[st.name]}` (override)"
            )
        elif isinstance(st, ast.ClassDef) and st.name in self.skip_functions:
            # `skip_functions` override on a module-level class: a test-local
            # subclass with no Rust twin (its tests are `skip_tests`) [P18].
            self.line(self.skip_comment(st.name))
        elif isinstance(st, ast.ClassDef):
            self.class_(st)
        elif (
            isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef))
            and st.name in self.skip_functions
        ):
            # `skip_functions` override: a module-level helper with no Rust twin yet [P18].
            self.line(self.skip_comment(st.name))
        elif isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef)):
            self.function(st, in_class=None)
        elif isinstance(st, ast.If) and self._is_main_guard(st):
            pass  # `if __name__ == "__main__":` has no Rust twin
        elif self._is_dead_if(st):
            # `if HAVE_TORCH:` on a dead optional-dependency flag [P2e].
            self.line(
                f"// py2rs: `if {st.test.id}:` block never runs (`{st.test.id}` is False: the import has no Rust twin)"
            )  # type: ignore[attr-defined]
        elif self._is_matrix_registration(st):
            self.matrix_registration(st)
        elif (
            isinstance(st, ast.If)
            and not st.orelse
            and st.body
            and all(self._is_matrix_registration(s) for s in st.body)
        ):
            # `if HAVE_X: register_dtype_matrix(...)` -- the guard is the
            # optional-import flag, always true for a Rust twin [P19b]; a
            # runtime-probe flag [P2c] gates the generated tests instead.
            guard = (
                st.test.id
                if isinstance(st.test, ast.Name) and st.test.id in self.probe_flags
                else None
            )
            for inner in st.body:
                self.matrix_registration(inner, guard)
        elif (alias := self._skip_alias(st)) is not None:
            # `skip_unless_x = unittest.skipUnless(FLAG, reason)`: a decorator
            # alias, applied as a guard on the methods it decorates [P2f].
            name, flag, reason, unless = alias
            self.skip_aliases[name] = (flag, reason, unless)
            self.line(
                f"// py2rs: `{name}` = `unittest.{'skipUnless' if unless else 'skipIf'}({ast.unparse(flag)}, ..)`: a guard on the methods it decorates [P2f]"
            )
            self.mark(st)
        elif (
            isinstance(st, ast.Assign)
            and len(st.targets) == 1
            and isinstance(st.targets[0], ast.Name)
        ):
            self.module_constant(st)
        elif isinstance(st, ast.Try) and (branch := self._optional_import_try(st)) is not None:
            # `try: import x ... except: x = None ...` -- the optional-dependency
            # idiom. The branch that applies is decided by whether the import
            # has a Rust twin [P2].
            for inner in branch:
                self.module_stmt(inner)
        elif isinstance(st, ast.Try) and (probe := self._probe_try(st)) is not None:
            self.probe_try(st, *probe)
        elif (
            isinstance(st, ast.Expr)
            and isinstance(st.value, ast.Constant)
            and isinstance(st.value.value, str)
        ):
            for ln in st.value.value.strip("\n").splitlines():
                self.line(f"// {ln}".rstrip())
        elif isinstance(st, ast.Expr) and self.const_mutation(st):
            pass
        elif isinstance(st, ast.Expr) and self._is_sys_path_call(st.value):
            # `sys.path.insert(0, ...)` / `sys.path.append(...)`: import-path
            # plumbing of the Python test runner, no Rust twin [P2].
            self.line(f"// py2rs: `{ast.unparse(st.value)}` has no Rust twin")
        else:
            self.unsupported_stmt(st, f"module-level {type(st).__name__}")
        self.mark(st)

    def module_constant(self, st: ast.Assign):
        """`NAME = value` at module level [P11].

        Python evaluates the value once, at import time: the Rust twin is a
        `static NAME: LazyLock<Arg>` whose initializer calls a generated
        `fn __laila_const_NAME() -> Arg { return value; }` -- a function, so
        the value is written in the Python expression syntax the
        `laila::python!` rewriter lowers (`static` items pass through the
        rewriter untouched).
        """
        name = rs_name(st.targets[0].id)
        try:
            v = self.expr(st.value)
        except Unsupported as u:
            self.unsupported_stmt(st, str(u))
            return
        const_fn = f"__laila_const_{name}"
        builder = self.builder_tokens.get(st.targets[0].id)
        child = self.child_scripts.get(st.targets[0].id)
        if child is not None:
            # `child_scripts` override: the constant is a child-interpreter
            # source template; its Rust twin is a `ChildScript` whose
            # `.format(..)` binds the arguments and whose child entry is the
            # `#[test]` emitted after the block [P32].
            self.line(
                f"static {name}: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| laila::python::stdlib::ChildScript::value({rs_str(name)}, &{const_fn}().expect({rs_str('module constant ' + name)}).__str__()));"
            )
            self.line(f"fn {const_fn}() -> Arg {{")
            self.line(f"    return {v};")
            self.line("}")
            self.trailer += [
                "",
                f"/// Child entry of `{name}` [P32]: a no-op unless `subprocess.run` spawned this binary on it.",
                "#[test]",
                f"fn __laila_child_{name}() {{",
                f"    laila::python::stdlib::subprocess::child_main({rs_str(name)}, {child});",
                "}",
            ]
            return
        if builder is not None:
            # `builder_tokens` override: the constant is a complex-constitution
            # source string; the Rust builder is registered under that exact
            # text the first time the constant is read [P31].
            self.line(
                f"static {name}: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| {{"
            )
            self.line(
                f"    let __laila_token = {const_fn}().expect({rs_str('module constant ' + name)});"
            )
            self.line(
                f"    laila::entry::constitution::register_builder_token(&__laila_token.__str__(), ::std::sync::Arc::new({builder}));"
            )
            self.line("    __laila_token")
            self.line("});")
        else:
            self.line(
                f"static {name}: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| {const_fn}().expect({rs_str('module constant ' + name)}));"
            )
        self.line(f"fn {const_fn}() -> Arg {{")
        self.line(f"    return {v};")
        # a later module-level `NAME.pop(..)` / `NAME.update(..)` folds into
        # this builder (see `const_mutation`) [P11b]
        self.const_return_idx[st.targets[0].id] = (len(self.out) - 1, v)
        self.line("}")

    def const_mutation(self, st: ast.Expr) -> bool:
        """`_CASES.pop("tls")` at module level, after `_CASES = {..}`: Python
        mutates the constant at import time, so the call folds into the
        constant's builder (`let mut _CASES = {..}; _CASES.pop("tls"); return
        _CASES;`) [P11b]. `False` when the statement is not such a call."""
        v = st.value
        if not (
            isinstance(v, ast.Call)
            and isinstance(v.func, ast.Attribute)
            and isinstance(v.func.value, ast.Name)
            and v.func.value.id in self.const_return_idx
        ):
            return False
        name = v.func.value.id
        idx, init = self.const_return_idx[name]
        # the builder's local (a `let` cannot shadow the `static`)
        rs = f"__laila_{rs_name(name).lstrip('_')}"
        shown = ast.unparse(v)
        v = copy.deepcopy(v)
        v.func.value.id = rs
        try:
            text = self.expr(v)
        except Unsupported:
            return False
        line = self.out[idx]
        pad = line[: len(line) - len(line.lstrip())]
        if line.lstrip().startswith("return ") and not line.lstrip().startswith(f"return {rs};"):
            self.out[idx : idx + 1] = [f"{pad}let mut {rs} = {init};", f"{pad}return {rs};"]
            idx += 1
        self.out.insert(idx, f"{pad}{text};")
        self.const_return_idx[name] = (idx + 1, init)
        self.line(f"// py2rs: `{shown}` folded into the builder of `{name}` [P11b]")
        return True

    @staticmethod
    def _matrix_call(st: ast.stmt) -> ast.Call | None:
        """The `register_dtype_matrix(Cls, make_pool, **kw)` call of a module-level
        statement (`f(..)` or `_N = f(..)`), or `None`."""
        value = st.value if isinstance(st, (ast.Expr, ast.Assign)) else None
        if (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id == "register_dtype_matrix"
        ):
            return value
        return None

    def _is_matrix_registration(self, st: ast.stmt) -> bool:
        return self._matrix_call(st) is not None

    def matrix_registration(self, st: ast.stmt, guard: str | None = None):
        """`register_dtype_matrix(Cls, make_pool, *, store_as_json_string=..)` --
        the dtype-travel matrix of `tests/functional/pools/base/compdata_roundtrip.py`
        attaches ~40 generated methods to *Cls*. The Rust twin of the helper
        is the hand-written `laila_tests::compdata_roundtrip`; its
        `dtype_matrix!` macro emits the same test names as `#[test]`s [P19b].
        """
        call = self._matrix_call(st)
        assert call is not None
        if len(call.args) < 2 or not all(isinstance(a, ast.Name) for a in call.args[:2]):
            self.unsupported_stmt(st, "register_dtype_matrix(Cls, make_pool, ..) needs two names")
            return
        cls, make = rs_name(call.args[0].id), rs_name(call.args[1].id)
        opts = {"store_as_json_string": "true", "include_torch": "true"}
        for kw in call.keywords:
            if (
                kw.arg in opts
                and isinstance(kw.value, ast.Constant)
                and isinstance(kw.value.value, bool)
            ):
                opts[kw.arg] = "true" if kw.value.value else "false"
            else:
                self.unsupported_stmt(st, f"register_dtype_matrix keyword {kw.arg}")
                return
        self.line(
            f"// py2rs: `{ast.unparse(st)}` -> the dtype matrix of `laila_tests::compdata_roundtrip` [P19b]"
        )
        skip = f", skip_unless = {rs_name(guard)}" if guard else ""
        self.line(
            f"laila_tests::dtype_matrix!({cls}, {make}, store_as_json_string = {opts['store_as_json_string']}, include_torch = {opts['include_torch']}{skip});"
        )

    def _probe_try(self, st: ast.Try) -> tuple[str, list[ast.stmt]] | None:
        """The runtime-probe idiom at module level [P2c]:

            try:
                from laila.data import FilesystemPool
                _probe = FilesystemPool()
                ...
                HAVE_FILESYSTEMPOOL = True
            except Exception:
                FilesystemPool = None
                HAVE_FILESYSTEMPOOL = False

        The body ends by setting a flag to `True`, the handler sets it to
        `False`: the flag is whether the body *runs*. Returns the flag name
        and the body statements between the imports and the flag.
        """
        if not st.handlers or st.orelse or st.finalbody or len(st.body) < 2:
            return None
        last = st.body[-1]
        if not (
            isinstance(last, ast.Assign)
            and len(last.targets) == 1
            and isinstance(last.targets[0], ast.Name)
            and isinstance(last.value, ast.Constant)
            and last.value.value is True
        ):
            return None
        flag = last.targets[0].id
        sets_false = any(
            isinstance(h, ast.Assign)
            and len(h.targets) == 1
            and isinstance(h.targets[0], ast.Name)
            and h.targets[0].id == flag
            and isinstance(h.value, ast.Constant)
            and h.value.value is False
            for handler in st.handlers
            for h in handler.body
        )
        if not sets_false:
            return None
        body = [s for s in st.body[:-1] if not isinstance(s, (ast.Import, ast.ImportFrom))]
        if not body:
            return None  # the plain optional-import idiom
        return flag, body

    def probe_try(self, st: ast.Try, flag: str, body: list[ast.stmt]):
        """`try: <probe>; FLAG = True except: FLAG = False` [P2c]: the imports
        bind their names; the probe runs once, at first read of the flag, as
        `fn __laila_probe_FLAG() -> Arg` (its statements in Python syntax),
        and the flag is whether it ran without raising."""
        for imp in st.body:
            if isinstance(imp, (ast.Import, ast.ImportFrom)):
                self.import_(imp)
                self.mark(imp)
        name = rs_name(flag)
        probe_fn = f"__laila_probe_{name}"
        self.probe_flags.add(flag)
        self.line(
            f"// py2rs: `try: ... {flag} = True / except: {flag} = False` -- a runtime probe: the flag is whether the body runs [P2c]"
        )
        self.line(
            f"static {name}: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| {probe_fn}().map(|_| Arg::Bool(true)).unwrap_or(Arg::Bool(false)));"
        )
        self.line(f"fn {probe_fn}() -> Arg {{")
        self.indent += 1
        counter = AssignCounter()
        for inner in body:
            counter.visit(inner)
        ctx = FnCtx(params=set(), assigned_counts=counter.counts, returns_value=True)
        self.fn_stack.append(ctx)
        for inner in body:
            self.stmt(inner)
            self.mark(inner)
        self.line("return True;")
        self.fn_stack.pop()
        self.indent -= 1
        self.line("}")

    def _optional_import_try(self, st: ast.Try) -> list[ast.stmt] | None:
        """The optional-dependency idiom at module level:

            try:
                import torch
                HAVE_TORCH = True
            except Exception:
                torch = None
                HAVE_TORCH = False

        Returns the statements of the branch that applies in Rust: the
        handler's when some imported module has no Rust twin (neither
        `laila` nor a prelude module), the body's otherwise. `None` when
        the statement is not of this shape.
        """
        imports = [s for s in st.body if isinstance(s, (ast.Import, ast.ImportFrom))]
        if not imports or not st.handlers or st.orelse or st.finalbody:
            return None
        if not all(isinstance(s, (ast.Import, ast.ImportFrom, ast.Assign)) for s in st.body):
            return None
        missing = False
        for imp in imports:
            if isinstance(imp, ast.Import):
                roots = [a.name.split(".")[0] for a in imp.names]
                full = [a.name for a in imp.names]
            else:
                roots = [(imp.module or "").split(".")[0]]
                full = [imp.module or ""]
            for root in roots:
                if (
                    root != "laila"
                    and root not in PRELUDE_MODULES
                    and root not in self.skip_imports
                    and root not in self.present_imports
                ):
                    missing = True
            # A `skip_imports` module has no Rust twin *yet*: the `except`
            # branch applies, as for a missing optional dependency [P18].
            if any(m in self.skip_imports for m in full):
                missing = True
        if missing:
            self.line(
                f"// py2rs: `{ast.unparse(imports[0]).strip()}` has no Rust twin: the `except` branch applies"
            )
            return [s for h in st.handlers for s in h.body if not isinstance(s, ast.Pass)]
        # The body applies: its imports bind names (`... as MemoryPool`) [P2].
        for imp in imports:
            self.import_(imp)
        return [s for s in st.body if not isinstance(s, (ast.Import, ast.ImportFrom))]

    def _optional_import_missing(self, st: ast.Try) -> bool:
        """Whether the `except` branch of an optional-import `try` applies
        (some imported module has no Rust twin) -- `_optional_import_try`
        without the emission."""
        imports = [s for s in st.body if isinstance(s, (ast.Import, ast.ImportFrom))]
        for imp in imports:
            if isinstance(imp, ast.Import):
                roots = [a.name.split(".")[0] for a in imp.names]
                full = [a.name for a in imp.names]
            else:
                roots = [(imp.module or "").split(".")[0]]
                full = [imp.module or ""]
            if any(
                r != "laila"
                and r not in PRELUDE_MODULES
                and r not in self.skip_imports
                and r not in self.present_imports
                for r in roots
            ):
                return True
            if any(m in self.skip_imports for m in full):
                return True
        return False

    def _scan_dead_flags(self, items: list[ast.stmt]) -> None:
        """`try: import torch; HAVE_TORCH = True / except: HAVE_TORCH = False`
        whose import has no Rust twin: every flag the handler binds to
        `False` is dead -- a module-level `if HAVE_TORCH:` never runs [P2e]."""
        for st in items:
            if not (isinstance(st, ast.Try) and self._is_optional_import_try_shape(st)):
                continue
            if not self._optional_import_missing(st):
                continue
            for h in st.handlers:
                for s in h.body:
                    if (
                        isinstance(s, ast.Assign)
                        and len(s.targets) == 1
                        and isinstance(s.targets[0], ast.Name)
                        and isinstance(s.value, ast.Constant)
                        and s.value.value is False
                    ):
                        self.dead_flags.add(s.targets[0].id)

    def _is_dead_if(self, st: ast.stmt) -> bool:
        """`if HAVE_TORCH:` (no `else`) on a dead flag [P2e]."""
        return (
            isinstance(st, ast.If)
            and not st.orelse
            and isinstance(st.test, ast.Name)
            and st.test.id in self.dead_flags
        )

    @staticmethod
    def _is_optional_import_try_shape(st: ast.Try) -> bool:
        """Whether `st` has the shape `_optional_import_try` recognises
        (a side-effect-free check: the body is imports and assignments)."""
        imports = [s for s in st.body if isinstance(s, (ast.Import, ast.ImportFrom))]
        if not imports or not st.handlers or st.orelse or st.finalbody:
            return False
        return all(isinstance(s, (ast.Import, ast.ImportFrom, ast.Assign)) for s in st.body)

    def _is_module_block_stmt(self, st: ast.stmt) -> bool:
        """A module-level `if` / `try` that is none of the recognised idioms
        (main guard, matrix registration, optional import, runtime probe):
        plain import-time control flow binding module names [P2d]."""
        if isinstance(st, ast.If):
            if self._is_main_guard(st) or self._is_dead_if(st):
                return False
            if not st.orelse and st.body and all(self._is_matrix_registration(s) for s in st.body):
                return False
            return True
        if isinstance(st, ast.Try):
            return not self._is_optional_import_try_shape(st) and self._probe_try(st) is None
        return False

    @staticmethod
    def _is_plain_assign(st: ast.stmt) -> bool:
        return (
            isinstance(st, ast.Assign)
            and len(st.targets) == 1
            and isinstance(st.targets[0], ast.Name)
        )

    def _fold_module_blocks(self, items: list[ast.stmt]) -> list[ast.stmt | ModuleBlock]:
        """Fold each module-level `if` / `try` [P2d] -- together with the run
        of plain `NAME = value` statements right before it that it reassigns
        (`_key_id = None` / `if ..: _key_id = ..`) -- into a `ModuleBlock`."""
        out: list[ast.stmt | ModuleBlock] = []
        run_start = 0  # index in `out` where the current run of plain assignments begins
        for st in items:
            if self._is_module_block_stmt(st):
                counter = AssignCounter()
                counter.visit(st)
                assigned = set(counter.counts)
                start = len(out)
                for i in range(run_start, len(out)):
                    s = out[i]
                    if isinstance(s, ast.Assign) and s.targets[0].id in assigned:  # type: ignore[attr-defined]
                        start = i
                        break
                block = [s for s in out[start:] if isinstance(s, ast.stmt)] + [st]
                del out[start:]
                out.append(ModuleBlock(block))
                run_start = len(out)
                continue
            out.append(st)
            if not self._is_plain_assign(st):
                run_start = len(out)
        return out

    def module_block(self, blk: ModuleBlock):
        """A module-level `if` / `try` [P2d]: Python runs it once, at import
        time, binding module names in whichever branch applies. The Rust
        twin runs the statements (in Python syntax) once, at the first read
        of any name they bind, as `fn __laila_block_N_init() -> Arg`
        returning the tuple of those names; each name is a `static
        NAME: LazyLock<Arg>` reading its slot (an unbound name reads
        `None`)."""
        self.flush_comments(blk.lineno)
        counter = AssignCounter()
        for st in blk.stmts:
            counter.visit(st)
        names = list(counter.counts)
        self.block_counter += 1
        n = self.block_counter
        block = f"__laila_block_{n}"
        kind = "try" if isinstance(blk.stmts[-1], ast.Try) else "if"
        self.line(
            f"// py2rs: module-level `{kind}:` binding {', '.join(names)} -- runs once, at the first read of any of them [P2d]"
        )
        self.line(
            f"static {block}: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| {block}_init().expect({rs_str('module block ' + str(n))}));"
        )
        self.line(f"fn {block}_init() -> Arg {{")
        self.indent += 1
        # The locals carry a prefix: a `let` may not shadow the `static` of
        # the same name that the block defines below.
        renames = {name: f"__laila_local_{rs_name(name)}" for name in names}
        for name in names:
            self.line(f"let mut {renames[name]} = None;")
        ctx = FnCtx(
            params=set(names),
            assigned_counts={k: v + 1 for k, v in counter.counts.items()},
            returns_value=True,
        )
        ctx.declared |= set(names)
        ctx.renames = renames
        self.fn_stack.append(ctx)
        for st in blk.stmts:
            self.stmt(st)
            self.mark(st)
        self.fn_stack.pop()
        tup = ", ".join(renames[name] for name in names) + ("," if len(names) == 1 else "")
        self.line(f"return ({tup});")
        self.indent -= 1
        self.line("}")
        for i, name in enumerate(names):
            self.line(
                f"static {rs_name(name)}: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| {block}.__getitem__({i}).expect({rs_str('module constant ' + name)}));"
            )

    @staticmethod
    def _is_sys_path_call(e: ast.expr) -> bool:
        return (
            isinstance(e, ast.Call)
            and isinstance(e.func, ast.Attribute)
            and isinstance(e.func.value, ast.Attribute)
            and isinstance(e.func.value.value, ast.Name)
            and e.func.value.value.id == "sys"
            and e.func.value.attr == "path"
        )

    # -- dynamic test registration [P19] -----------------------------------

    def _scan_registrations(self, items: list[ast.stmt]) -> None:
        """Fold `def _case(name, fn): setattr(Cls, name, fn)` + `_case("test_x", _t1)`
        + module-level `def _t1(self): ...` into a `test_x` method of `Cls`."""
        # 1. helpers: a two-parameter def whose body is `setattr(Cls, <p0>, <p1>)`
        for st in items:
            if not isinstance(st, ast.FunctionDef) or len(st.args.args) != 2:
                continue
            body = [
                s
                for s in st.body
                if not (
                    isinstance(s, ast.Expr)
                    and isinstance(s.value, ast.Constant)
                    and isinstance(s.value.value, str)
                )
            ]
            if (
                len(body) != 1
                or not isinstance(body[0], ast.Expr)
                or not isinstance(body[0].value, ast.Call)
            ):
                continue
            call = body[0].value
            if not (
                isinstance(call.func, ast.Name)
                and call.func.id == "setattr"
                and len(call.args) == 3
            ):
                continue
            cls, a1, a2 = call.args
            p0, p1 = st.args.args[0].arg, st.args.args[1].arg
            if (
                isinstance(cls, ast.Name)
                and isinstance(a1, ast.Name)
                and isinstance(a2, ast.Name)
                and a1.id == p0
                and a2.id == p1
            ):
                self.registration_helpers[st.name] = cls.id
        if not self.registration_helpers:
            return
        # 2. registrations: `_case("test_x", _t1)` at module level
        defs = {st.name: st for st in items if isinstance(st, ast.FunctionDef)}
        for st in items:
            if not (isinstance(st, ast.Expr) and isinstance(st.value, ast.Call)):
                continue
            call = st.value
            if not (
                isinstance(call.func, ast.Name)
                and call.func.id in self.registration_helpers
                and len(call.args) == 2
            ):
                continue
            name, fn = call.args
            if (
                isinstance(name, ast.Constant)
                and isinstance(name.value, str)
                and isinstance(fn, ast.Name)
                and fn.id in defs
            ):
                cls = self.registration_helpers[call.func.id]
                self.registered.setdefault(cls, []).append((defs[fn.id], name.value))
                self.registered_defs.add(fn.id)
        # 3. exposures: `Cls._helper = _helper` -- a module-level `def _helper(self, ..)`
        #    attached as a method under its own name [P19c]
        for st in items:
            if not (
                isinstance(st, ast.Assign)
                and len(st.targets) == 1
                and isinstance(st.targets[0], ast.Attribute)
            ):
                continue
            target, fn = st.targets[0], st.value
            if not (
                isinstance(target.value, ast.Name) and isinstance(fn, ast.Name) and fn.id in defs
            ):
                continue
            d = defs[fn.id]
            if d.args.args and d.args.args[0].arg == "self":
                self.registered.setdefault(target.value.id, []).append((d, target.attr))
                self.registered_defs.add(fn.id)
                self.registration_stmts.add(id(st))
        # 4. decorator registrars: `def _edge(name): def _d(fn): _case(f"test_{name}", fn); return fn; return _d`
        #    + `@_edge("x") def _fn(self)` [P19c]
        registrars: dict[str, tuple[str, ast.expr, str]] = {}
        for st in items:
            if not (
                isinstance(st, ast.FunctionDef) and len(st.args.args) == 1 and len(st.body) == 2
            ):
                continue
            inner, ret = st.body
            if not (
                isinstance(inner, ast.FunctionDef)
                and len(inner.args.args) == 1
                and len(inner.body) == 2
            ):
                continue
            if not (
                isinstance(ret, ast.Return)
                and isinstance(ret.value, ast.Name)
                and ret.value.id == inner.name
            ):
                continue
            reg, inner_ret = inner.body
            if not (
                isinstance(reg, ast.Expr)
                and isinstance(reg.value, ast.Call)
                and isinstance(reg.value.func, ast.Name)
            ):
                continue
            call = reg.value
            if call.func.id not in self.registration_helpers or len(call.args) != 2:
                continue
            if not (
                isinstance(call.args[1], ast.Name) and call.args[1].id == inner.args.args[0].arg
            ):
                continue
            if not (
                isinstance(inner_ret, ast.Return)
                and isinstance(inner_ret.value, ast.Name)
                and inner_ret.value.id == inner.args.args[0].arg
            ):
                continue
            registrars[st.name] = (
                st.args.args[0].arg,
                call.args[0],
                self.registration_helpers[call.func.id],
            )
            self.registered_defs.add(st.name)
        if registrars:
            for st in items:
                if not (isinstance(st, ast.FunctionDef) and len(st.decorator_list) == 1):
                    continue
                dec = st.decorator_list[0]
                if not (
                    isinstance(dec, ast.Call)
                    and isinstance(dec.func, ast.Name)
                    and dec.func.id in registrars
                    and len(dec.args) == 1
                ):
                    continue
                param, name_expr, cls = registrars[dec.func.id]
                try:
                    value = ast.literal_eval(dec.args[0])
                    test_name = self._eval_static(name_expr, {param: value})
                except Exception as e:
                    self.notes.append(
                        f"{self.rel_path}:{st.lineno}: registrar name not static ({e})"
                    )
                    continue
                if not isinstance(test_name, str):
                    continue
                plain = copy.deepcopy(st)
                plain.decorator_list = []
                self.registered.setdefault(cls, []).append((plain, test_name))
                self.registered_defs.add(st.name)
        # 5. parametric matrices: module-level `for` nests over literal /
        #    constant iterables calling a factory that registers a nested
        #    `_fn(self, _p=p, ..)` -- unrolled, one method per case [P19c]
        for st in items:
            if isinstance(st, ast.For) and self._unroll_matrix(st, items):
                self.registration_stmts.add(id(st))

    # -- parametric test matrices [P19c] ------------------------------------

    def _static_namespace(self, items: list[ast.stmt]) -> dict[str, Any]:
        """The module's constants (`_SHAPES = [..]`, literal values) and
        its plain helper functions (`def _shape_tag(shape): ..`), for
        evaluating registration names at transpile time. Helpers are
        executed from their own source only -- no module is imported."""
        if getattr(self, "_static_ns", None) is not None:
            return self._static_ns  # type: ignore[attr-defined]
        ns: dict[str, Any] = {}
        for st in items:
            if self._is_plain_assign(st):
                try:
                    ns[st.targets[0].id] = ast.literal_eval(st.value)  # type: ignore[attr-defined]
                except Exception:
                    pass
            elif (
                isinstance(st, ast.FunctionDef)
                and not st.decorator_list
                and not (st.args.args and st.args.args[0].arg == "self")
            ):
                try:
                    exec(
                        compile(
                            ast.Module(body=[st], type_ignores=[]), f"<{self.rel_path}>", "exec"
                        ),
                        ns,
                    )
                except Exception:
                    pass
        self._static_ns = ns  # type: ignore[attr-defined]
        return ns

    def _eval_static(self, e: ast.expr, env: dict[str, Any]) -> Any:
        ns = dict(self._static_namespace(self.tree.body))
        ns.update(env)
        return eval(compile(ast.Expression(body=e), f"<{self.rel_path}>", "eval"), ns)

    @staticmethod
    def _literal_ast(v: Any) -> ast.expr | None:
        """A transpile-time value as a Python literal expression."""
        if isinstance(v, (str, int, float, bool)) or v is None:
            return ast.Constant(value=v)
        if isinstance(v, tuple):
            elts = [Emitter._literal_ast(x) for x in v]
            return None if any(x is None for x in elts) else ast.Tuple(elts=elts, ctx=ast.Load())  # type: ignore[arg-type]
        if isinstance(v, list):
            elts = [Emitter._literal_ast(x) for x in v]
            return None if any(x is None for x in elts) else ast.List(elts=elts, ctx=ast.Load())  # type: ignore[arg-type]
        return None

    def _matrix_factory(
        self, fn: ast.FunctionDef
    ) -> tuple[list[ast.Assign], ast.FunctionDef, ast.expr, str] | None:
        """A matrix factory: `def _make(p, ..): [tag = ..]* ; def _fn(self, _p=p, ..): ..; _case(<name expr>, _fn)`.
        Returns `(assigns, _fn, name expr, class)` or `None`."""
        body = [
            s
            for s in fn.body
            if not (
                isinstance(s, ast.Expr)
                and isinstance(s.value, ast.Constant)
                and isinstance(s.value.value, str)
            )
        ]
        if len(body) < 2:
            return None
        reg = body[-1]
        if not (
            isinstance(reg, ast.Expr)
            and isinstance(reg.value, ast.Call)
            and isinstance(reg.value.func, ast.Name)
        ):
            return None
        call = reg.value
        if (
            call.func.id not in self.registration_helpers
            or len(call.args) != 2
            or not isinstance(call.args[1], ast.Name)
        ):
            return None
        inner = body[-2]
        if not (isinstance(inner, ast.FunctionDef) and inner.name == call.args[1].id):
            return None
        if not (inner.args.args and inner.args.args[0].arg == "self"):
            return None
        assigns = body[:-2]
        if not all(self._is_plain_assign(s) for s in assigns):
            return None
        return assigns, inner, call.args[0], self.registration_helpers[call.func.id]  # type: ignore[return-value]

    def _unroll_matrix(self, loop: ast.For, items: list[ast.stmt]) -> bool:
        """Unroll a module-level `for` nest whose innermost body defines
        and/or calls a matrix factory; each case becomes a method whose
        body binds the captured parameters as locals and continues with
        the generated function's body, under the registered name [P19c]."""
        defs = {st.name: st for st in items if isinstance(st, ast.FunctionDef)}
        cases: list[tuple[dict[str, Any], ast.FunctionDef, list[ast.FunctionDef]]] = []

        def walk(st: ast.stmt, env: dict[str, Any], local: list[ast.FunctionDef]) -> bool:
            if isinstance(st, ast.For) and isinstance(st.target, ast.Name) and not st.orelse:
                try:
                    values = list(self._eval_static(st.iter, env))
                except Exception as e:
                    self.notes.append(
                        f"{self.rel_path}:{st.lineno}: matrix iterable not static ({e})"
                    )
                    return False
                for v in values:
                    scope = dict(env)
                    scope[st.target.id] = v
                    inner_local = list(local)
                    for s in st.body:
                        if isinstance(s, ast.FunctionDef):
                            inner_local.append(s)
                            continue
                        if not walk(s, scope, inner_local):
                            return False
                return True
            if (
                isinstance(st, ast.Expr)
                and isinstance(st.value, ast.Call)
                and isinstance(st.value.func, ast.Name)
            ):
                call = st.value
                name = call.func.id
                factory = next((f for f in reversed(local) if f.name == name), None) or defs.get(
                    name
                )
                if factory is None or call.keywords:
                    return False
                try:
                    args = [self._eval_static(a, env) for a in call.args]
                except Exception as e:
                    self.notes.append(
                        f"{self.rel_path}:{st.lineno}: matrix argument not static ({e})"
                    )
                    return False
                params = [a.arg for a in factory.args.args]
                if len(params) != len(args):
                    return False
                cases.append((dict(zip(params, args)), factory, local))
                return True
            return False

        if not walk(loop, {}, []):
            return False
        for env, factory, local in cases:
            shape = self._matrix_factory(factory)
            if shape is None:
                self.notes.append(
                    f"{self.rel_path}:{factory.lineno}: `{factory.name}` is not a matrix factory"
                )
                return False
            assigns, inner, name_expr, cls = shape
            scope = dict(env)
            try:
                for a in assigns:
                    scope[a.targets[0].id] = self._eval_static(a.value, scope)  # type: ignore[attr-defined]
                test_name = self._eval_static(name_expr, scope)
                captured = [
                    (a.arg, self._eval_static(d, scope))
                    for a, d in zip(
                        inner.args.args[len(inner.args.args) - len(inner.args.defaults) :],
                        inner.args.defaults,
                    )
                ]
            except Exception as e:
                self.notes.append(f"{self.rel_path}:{factory.lineno}: matrix case not static ({e})")
                return False
            if not isinstance(test_name, str):
                return False
            used = {n.id for s in inner.body for n in ast.walk(s) if isinstance(n, ast.Name)}
            bindings: list[tuple[str, Any]] = [
                (k, v) for k, v in env.items() if k in used
            ] + captured
            prelude: list[ast.stmt] = []
            for k, v in bindings:
                lit = self._literal_ast(v)
                if lit is None:
                    self.notes.append(
                        f"{self.rel_path}:{factory.lineno}: matrix value {v!r} is not a literal"
                    )
                    return False
                prelude.append(
                    ast.copy_location(
                        ast.Assign(targets=[ast.Name(id=k, ctx=ast.Store())], value=lit), inner
                    )
                )
            method = copy.deepcopy(inner)
            method.name = test_name
            method.args = ast.arguments(
                posonlyargs=[],
                args=[ast.arg(arg="self")],
                vararg=None,
                kwonlyargs=[],
                kw_defaults=[],
                kwarg=None,
                defaults=[],
            )
            method.decorator_list = []
            method.body = [ast.fix_missing_locations(p) for p in prelude] + method.body
            self.registered.setdefault(cls, []).append((method, test_name))
            self.registered_defs.add(factory.name)
        return True

    def _is_registration_stmt(self, st: ast.stmt) -> bool:
        if id(st) in self.registration_stmts:
            return True
        if isinstance(st, ast.FunctionDef):
            return st.name in self.registration_helpers or st.name in self.registered_defs
        if (
            isinstance(st, ast.Expr)
            and isinstance(st.value, ast.Call)
            and isinstance(st.value.func, ast.Name)
        ):
            return st.value.func.id in self.registration_helpers
        return False

    @staticmethod
    def _is_main_guard(st: ast.If) -> bool:
        t = st.test
        return (
            isinstance(t, ast.Compare)
            and isinstance(t.left, ast.Name)
            and t.left.id == "__name__"
            and len(t.comparators) == 1
            and isinstance(t.comparators[0], ast.Constant)
            and t.comparators[0].value == "__main__"
        )

    # -- classes ------------------------------------------------------------

    def _inherited_methods(
        self, c: ast.ClassDef, own: set[str]
    ) -> list[tuple[str, ast.FunctionDef]]:
        """Methods a test case inherits from the test-case classes of this file
        (base-most first, each name once, names the class defines itself
        excluded) -- Python's MRO for the single-inheritance chains the suites
        use [P12b]."""
        out: list[tuple[str, ast.FunctionDef]] = []
        seen = set(own)
        for b in c.bases:
            bname = ast.unparse(b)
            base = self.testcase_classes.get(bname)
            if base is None:
                continue
            base_own = {
                st.name
                for st in base.body
                if isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
            for origin, fn in self._inherited_methods(base, base_own):
                if fn.name not in seen:
                    seen.add(fn.name)
                    out.append((origin, fn))
            for st in base.body:
                if isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef)) and st.name not in seen:
                    seen.add(st.name)
                    out.append((base.name, st))
        return out

    @staticmethod
    def _class_attr_name(st: ast.stmt) -> str | None:
        """The name of a class-level `x = v` / `x: T = v`, else `None`."""
        if (
            isinstance(st, ast.Assign)
            and len(st.targets) == 1
            and isinstance(st.targets[0], ast.Name)
        ):
            return st.targets[0].id
        if (
            isinstance(st, ast.AnnAssign)
            and isinstance(st.target, ast.Name)
            and st.value is not None
        ):
            return st.target.id
        return None

    def _inherited_attrs(self, c: ast.ClassDef, own: set[str | None]) -> list[tuple[str, ast.stmt]]:
        """Class attributes a test case inherits from this file's test-case
        bases (nearest definition wins, names it defines itself excluded) [P12b]."""
        out: list[tuple[str, ast.stmt]] = []
        seen = set(own)
        for b in c.bases:
            base = self.testcase_classes.get(ast.unparse(b))
            if base is None:
                continue
            base_own = {self._class_attr_name(st) for st in base.body}
            for st in base.body:
                name = self._class_attr_name(st)
                if name is not None and name not in seen:
                    seen.add(name)
                    out.append((base.name, st))
            for origin, st in self._inherited_attrs(base, base_own):
                name = self._class_attr_name(st)
                if name not in seen:
                    seen.add(name)
                    out.append((origin, st))
        return out

    def _base_defining(self, c: ast.ClassDef, m: str) -> tuple[str, ast.FunctionDef] | None:
        """The nearest test-case base of `c` (in this file) defining `m`."""
        for b in c.bases:
            base = self.testcase_classes.get(ast.unparse(b))
            if base is None:
                continue
            for st in base.body:
                if isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef)) and st.name == m:
                    return base.name, st
            found = self._base_defining(base, m)
            if found is not None:
                return found
        return None

    @staticmethod
    def _super_calls(fn: ast.FunctionDef) -> list[str]:
        """Method names `fn` reaches through `super().m(..)`."""
        out: list[str] = []
        for n in ast.walk(fn):
            if (
                isinstance(n, ast.Call)
                and isinstance(n.func, ast.Attribute)
                and isinstance(n.func.value, ast.Call)
                and isinstance(n.func.value.func, ast.Name)
                and n.func.value.func.id == "super"
                and n.func.attr not in out
            ):
                out.append(n.func.attr)
        return out

    def _super_methods(
        self, c: ast.ClassDef, fns: list[ast.FunctionDef]
    ) -> list[tuple[str, ast.FunctionDef, str]]:
        """`(origin, fn, rust_name)` for every base method the methods of `c`
        reach through `super().m(..)` -- transitively, as the copied base
        method may itself call `super()` [P12d]."""
        out: list[tuple[str, ast.FunctionDef, str]] = []
        seen: set[str] = set()
        pending: list[tuple[ast.ClassDef, ast.FunctionDef]] = [(c, fn) for fn in fns]
        while pending:
            owner, fn = pending.pop(0)
            for m in self._super_calls(fn):
                found = self._base_defining(owner, m)
                if found is None:
                    continue
                origin, base_fn = found
                rs = f"{origin}__{m}"
                if rs in seen:
                    continue
                seen.add(rs)
                out.append((origin, base_fn, rs))
                pending.append((self.testcase_classes[origin], base_fn))
        return out

    def super_table(self, owner: str, fn: ast.FunctionDef) -> dict[str, str]:
        """`m -> <Base>__m` for the `super().m(..)` calls of `fn` as a method of `owner`."""
        table: dict[str, str] = {}
        c = self.testcase_classes.get(owner)
        if c is None:
            return table
        for m in self._super_calls(fn):
            found = self._base_defining(c, m)
            if found is not None:
                table[m] = f"{found[0]}__{m}"
        return table

    @staticmethod
    def _is_test_name(name: str) -> bool:
        """unittest's default `test*` method prefix."""
        return name.startswith("test")

    def _dead_skip_flag(self, f: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
        """The dead optional-dependency flag of a `@unittest.skipUnless(FLAG, ..)`
        decorator on *f* (`None` when there is none) [P2e]."""
        for d in f.decorator_list:
            if (
                isinstance(d, ast.Call)
                and isinstance(d.func, ast.Attribute)
                and isinstance(d.func.value, ast.Name)
                and d.func.value.id == "unittest"
                and d.func.attr == "skipUnless"
                and d.args
                and isinstance(d.args[0], ast.Name)
                and d.args[0].id in self.dead_flags
            ):
                return d.args[0].id
        return None

    def _class_skip_attr(self, d: ast.expr) -> str | None:
        """`unittest.skipUnless(cond, reason)` / `unittest.skipIf(cond, reason)`
        as a class decorator -> `#[skip_unless(cond, "reason")]` /
        `#[skip_if(cond, "reason")]` on the `impl` [P12b]."""
        if not (
            isinstance(d, ast.Call)
            and isinstance(d.func, ast.Attribute)
            and isinstance(d.func.value, ast.Name)
            and d.func.value.id == "unittest"
            and d.func.attr in ("skipUnless", "skipIf")
            and len(d.args) == 2
        ):
            return None
        cond = self.expr(d.args[0])
        reason = d.args[1]
        if not (isinstance(reason, ast.Constant) and isinstance(reason.value, str)):
            return None
        attr = "skip_unless" if d.func.attr == "skipUnless" else "skip_if"
        return f"#[{attr}({cond}, {rs_str(reason.value)})]"

    def class_(self, c: ast.ClassDef):
        bases = [self.expr(b) for b in c.bases]
        has_init = any(isinstance(st, ast.FunctionDef) and st.name == "__init__" for st in c.body)
        # A class defined inside a function body is never a test case / mixin.
        nested = bool(self.fn_stack)
        is_testcase = not nested and (
            any(b.endswith("TestCase") for b in bases)
            or any(ast.unparse(b) in self.testcase_classes for b in c.bases)
            # a base-less helper class of a unittest module is a test mixin,
            # unless it is a plain object with a constructor
            or (self.unittest and not bases and not has_init)
        )
        # A base-less class that other classes of the file derive from is a
        # mixin (`class _CloseRaceMixin:` + `class TestX(_CloseRaceMixin,
        # unittest.TestCase)`): its methods are copied into the test cases
        # [P12b] and, as unittest collects no tests from it, its own
        # `test_*` methods are not emitted [P12c].
        mixin = (
            is_testcase
            and not bases
            and any(
                isinstance(other, ast.ClassDef)
                and other is not c
                and any(ast.unparse(b) == c.name for b in other.bases)
                for other in self.tree.body
            )
        )
        name = rs_name(c.name)
        class_skips: list[str] = []
        for d in c.decorator_list:
            skip = self._class_skip_attr(d)
            if skip is not None:
                # `@unittest.skipUnless(FLAG, reason)` on a TestCase: every
                # generated `#[test]` skips when the flag is false [P12b].
                class_skips.append(skip)
                continue
            self.line(f"// @{self.expr(d)}")
        # MERGE: subclasses of the `laila.atomic` mixin traits are plain
        # `py_class!` structs with `impl Mixin for C {}` (the mixins are Rust
        # traits, not `ModelBase` classes); their annotated fields follow P14
        # with the `#[class_attr]` marker. Fold into `py_model!` once the
        # atomic mixins become model classes.
        atomic_mixin = any(b in ATOMIC_MIXINS for b in bases)
        # A base-less class of plain attributes only (`class _Stub:
        # global_id = ".."`): a value object whose attributes are read
        # dynamically (`getattr(obj, "global_id")`), so a model too.
        plain_attrs = (
            not bases
            and not has_init
            and any(isinstance(st, ast.Assign) for st in c.body)
            and all(
                isinstance(st, (ast.Assign, ast.AnnAssign, ast.Pass))
                or (isinstance(st, ast.Expr) and isinstance(st.value, ast.Constant))
                for st in c.body
            )
        )
        model_base = any(b in MODEL_BASES for b in bases)
        if (
            not is_testcase
            and not atomic_mixin
            and (any(isinstance(st, ast.AnnAssign) for st in c.body) or plain_attrs or model_base)
        ):
            # Annotated class attributes are pydantic fields: the class is a
            # model, declared with `py_model!` so the field descriptors exist
            # at runtime (`Cls.model_fields`, `laila.args` resolution) [P14b].
            self.model_class(c, name, bases)
            return
        if is_testcase:
            # unittest.TestCase -> dynamic struct + generated #[test] wrappers [P12]
            self.unittest = True
            self.testcase_classes[c.name] = c
            mixins = [
                b for b in bases if not b.endswith("TestCase") and b not in self.testcase_classes
            ]
            self.line(f"py_class!({name});")
            for m in mixins:
                self.line(f"impl {m} for {name} {{}}")
        else:
            # A class defining `__init__` runs it from `Name(..)` (`py_class!`'s
            # `init` form; the method binds its arguments with `#[pysig]`).
            init = "; init" if has_init else ""
            self.line(f"py_class!({name}{init});")
            self.dyn_param_classes.add(c.name)
            if bases and bases != ["object"]:
                # `class C(Base)` -> dynamic struct + `impl Base for C` [P13]
                for b in bases:
                    self.line(f"impl {b} for {name} {{}}")
        for skip in class_skips:
            self.line(skip)
        self.line(f"impl {name} {{")
        self.indent += 1
        self.class_stack.append(c.name)
        body = list(c.body)
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            # The class docstring is a plain comment (like function docstrings):
            # a `///` doc comment would attach to the first method -- and is a
            # syntax error when every method is dropped by `skip_tests`.
            doc = body.pop(0)
            for ln in doc.value.value.strip("\n").splitlines():
                self.line(f"// {ln}".rstrip())
            self.mark(doc)
        if is_testcase:
            # Methods of the test-case base classes defined in this file
            # (`class TestX(_Case)`): Rust has no inheritance between
            # `py_class!` structs, so the subclass carries a copy of each
            # method it does not override [P12b].
            own = {
                st.name for st in body if isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
            saved_last = self.last_line
            # Class attributes the base defines and the subclass does not
            # override (`factory = DefaultTCPProtocol`) are copied too [P12b].
            own_attrs = {self._class_attr_name(st) for st in body}
            for origin, st in self._inherited_attrs(c, own_attrs):
                self.line(f"// py2rs: inherited from `{origin}`")
                self.line("#[class_attr]")
                self.line(f"fn {rs_name(self._class_attr_name(st))}(&self) -> Arg {{")
                self.line(f"    return {self.expr(st.value)};")
                self.line("}")
            for origin, fn in self._inherited_methods(c, own):
                self.line(f"// py2rs: inherited from `{origin}`")
                # A sibling mixin's method carries the sibling's line numbers:
                # this file's comments must not be flushed into it [P12e].
                comments = self.comments
                if origin in self.sibling_classes:
                    self.comments = {}
                self.function(fn, in_class=c.name, super_origin=origin)
                self.comments = comments
            # A method the class overrides *and* reaches through
            # `super().m(..)` keeps the base's version too, under the
            # class-qualified name `<Base>__m` (Python's private-name
            # mangling shape) [P12d].
            for origin, fn, rs in self._super_methods(
                c, [st for st in body if isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef))]
            ):
                self.line(
                    f"// py2rs: `{origin}.{fn.name}`, reached through `super().{fn.name}()` [P12d]"
                )
                self.function(fn, in_class=c.name, name=rs, super_origin=origin)
            self.last_line = saved_last
        first = True
        for st in body:
            if not first:
                self.flush_comments(st.lineno)
            else:
                self.last_line = c.lineno
                self.flush_comments(st.lineno)
            first = False
            if isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if mixin and self._is_test_name(st.name):
                    self.line(
                        f"// py2rs: `{st.name}` runs on the test cases deriving from this mixin [P12c]"
                    )
                else:
                    self.function(st, in_class=c.name)
            elif (
                isinstance(st, ast.Assign)
                and len(st.targets) == 1
                and isinstance(st.targets[0], ast.Name)
            ) or (
                isinstance(st, ast.AnnAssign)
                and isinstance(st.target, ast.Name)
                and st.value is not None
            ):
                # class attribute (`x = 1`, pydantic field `n: int = 0`) -> an
                # associated const of `Arg` is not expressible; use a fn, marked
                # `#[class_attr]` (a value, not a callable) [P14]
                target = st.targets[0] if isinstance(st, ast.Assign) else st.target
                try:
                    v = self.expr(st.value)
                except Unsupported as u:
                    self.unsupported_stmt(st, str(u))
                else:
                    self.line("#[class_attr]")
                    self.line(f"fn {rs_name(target.id)}(&self) -> Arg {{")
                    self.line(f"    return {v};")
                    self.line("}")
            elif isinstance(st, ast.Pass):
                pass
            else:
                self.unsupported_stmt(st, f"class-level {type(st).__name__}")
            self.mark(st)
        # methods attached dynamically (`_case("test_x", _tN)`) [P19]
        for fn, test_name in self.registered.get(c.name, []):
            self.flush_comments(fn.lineno)
            self.function(fn, in_class=c.name, name=test_name)
            self.mark(fn)
        self.class_stack.pop()
        self.indent -= 1
        self.line("}")

    def model_class(self, c: ast.ClassDef, name: str, bases: list[str]):
        """`class C(Base): x: T = Field(..)` -> `py_model! { C(Base) { x: T = Field(..), } }` [P14b].

        Members keep their Python spelling (`name: annotation = value`); the
        macro lowers each value expression and builds the class descriptor.
        Methods (rare on config models) follow in an `impl` block.
        """
        self.line("py_model! {")
        self.indent += 1
        self.line(f"{name}({', '.join(bases)}) {{")
        self.indent += 1
        self.class_stack.append(c.name)
        body = list(c.body)
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            doc = body.pop(0)
            for ln in doc.value.value.strip("\n").splitlines():
                self.line(f"// {ln}".rstrip())
            self.mark(doc)
        methods: list[ast.stmt] = []
        self.last_line = c.lineno
        for st in body:
            self.flush_comments(st.lineno)
            if isinstance(st, ast.AnnAssign) and isinstance(st.target, ast.Name):
                ann = ast.unparse(st.annotation)
                if st.value is None:
                    self.line(
                        f"{rs_name(st.target.id)}: {ann} = PydanticUndefined(),{self.trailing_comment(st)}"
                    )
                else:
                    self.line(
                        f"{rs_name(st.target.id)}: {ann} = {self.expr(st.value)},{self.trailing_comment(st)}"
                    )
            elif (
                isinstance(st, ast.Assign)
                and len(st.targets) == 1
                and isinstance(st.targets[0], ast.Name)
            ):
                self.line(
                    f"{rs_name(st.targets[0].id)} = {self.expr(st.value)},{self.trailing_comment(st)}"
                )
            elif isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef)):
                methods.append(st)
            elif isinstance(st, ast.Pass):
                pass
            else:
                self.unsupported_stmt(st, f"class-level {type(st).__name__}")
            self.mark(st)
        self.indent -= 1
        self.line("}")
        self.indent -= 1
        self.line("}")
        if methods:
            self.notes.append(
                f"class {c.name}: methods on a py_model! class are emitted but have no instance (`self`) binding yet"
            )
            self.line(f"impl {name} {{")
            self.indent += 1
            for st in methods:
                self.flush_comments(st.lineno)
                self.function(st, in_class=c.name)
                self.mark(st)
            self.indent -= 1
            self.line("}")
        self.class_stack.pop()

    # -- functions ----------------------------------------------------------

    def function(
        self,
        f: ast.FunctionDef | ast.AsyncFunctionDef,
        in_class: str | None,
        name: str | None = None,
        super_origin: str | None = None,
    ):
        fname = name or f.name
        # `skip_tests` override: drop the method (its body has no Rust twin
        # yet) [P18]; `Class.method` names one class's method when several
        # classes share the method name (`test_name`).
        qualified = f"{in_class}.{fname}" if in_class else fname
        if fname in self.skip_tests or qualified in self.skip_tests:
            self.line(self.skip_comment(qualified if qualified in self.skip_tests else fname))
            return
        dead = self._dead_skip_flag(f)
        if dead is not None:
            # `@unittest.skipUnless(HAVE_TORCH, ..)` on a dead flag: the body
            # is never run and uses the missing module -- no Rust twin [P2e].
            self.line(
                f"// py2rs: `{fname}` skipped (`unittest.skipUnless({dead}, ..)`: `{dead}` is False, the import has no Rust twin)"
            )
            return
        args = f.args
        params: list[str] = []
        names: set[str] = set()
        has_self = in_class is not None and args.args and args.args[0].arg in ("self", "cls")
        is_static = any(
            isinstance(d, ast.Name) and d.id == "staticmethod" for d in f.decorator_list
        )
        is_classmethod = any(
            isinstance(d, ast.Name) and d.id == "classmethod" for d in f.decorator_list
        )
        plain = list(args.posonlyargs) + list(args.args)
        renames: dict[str, str] = {}
        if has_self and not is_static:
            plain = plain[1:]
            # A classmethod's `cls` is the class handle, `Self::__class__()`
            # (a `Self` whose `__dict__` is the class `__dict__`), passed as
            # `&self`; `cls.x` / `cls.x = v` are then class attribute reads /
            # writes [P12c].
            params.append("&self")
            if is_classmethod and args.args[0].arg != "self":
                renames[args.args[0].arg] = "self"
        # `#[pysig]` when defaults/varargs/kw-only are present [P15]
        needs_sig = bool(args.defaults or args.vararg or args.kwarg or args.kwonlyargs)
        if fname in getattr(self, "kw_called", ()) and not fname.startswith("test_"):
            needs_sig = True
        if fname == "__init__" and in_class is not None and has_self:
            # `Name(..)` passes the constructor's `Args` to `__init__` [P12e].
            needs_sig = True
        # A decorated nested `def` is a Python function *value* wrapped by
        # the decorator (`@ensure_list("items") def f(items)`): it lowers to
        # `#[decorate(..)] #[pysig(..)] fn` so the macro builds a signature
        # carrying `Function` and applies the decorators innermost-first.
        nested = in_class is None and bool(self.fn_stack)
        nested_decorators = [self.expr(d) for d in f.decorator_list] if nested else []
        if nested_decorators or (
            nested
            and not isinstance(f, ast.AsyncFunctionDef)
            and self._decorated_by_hand(fname, self.fn_stack[-1].node)
        ):
            # Likewise a nested `def` decorated by hand
            # (`wrapped = deco("items")(original)`): the decorator may
            # introspect it (`__name__`, `inspect.signature`).
            needs_sig = True
        if nested and not isinstance(f, ast.AsyncFunctionDef) and self.introspects:
            # A module that imports `inspect` introspects function values
            # (`inspect.signature(target).parameters`): every nested `def`
            # is a `Function` carrying its signature [P15b].
            needs_sig = True
        sig_parts: list[str] = []
        if has_self and not is_static:
            sig_parts.append(args.args[0].arg)
        ndef = len(args.defaults)
        for i, a in enumerate(plain):
            di = i - (len(plain) - ndef)
            if di >= 0:
                sig_parts.append(f"{a.arg}={self.expr(args.defaults[di])}")
            else:
                sig_parts.append(a.arg)
            names.add(a.arg)
        if args.vararg:
            sig_parts.append(f"*{args.vararg.arg}")
            names.add(args.vararg.arg)
        elif args.kwonlyargs:
            sig_parts.append("*")
        for a, d in zip(args.kwonlyargs, args.kw_defaults):
            sig_parts.append(f"{a.arg}={self.expr(d)}" if d is not None else a.arg)
            names.add(a.arg)
        if args.kwarg:
            sig_parts.append(f"**{args.kwarg.arg}")
            names.add(args.kwarg.arg)
        counter = AssignCounter()
        for st in f.body:
            counter.visit(st)
        if needs_sig:
            params.append("args: Args")
        else:
            for a in plain:
                # A parameter rebound in the body (`v = v.data`) is `mut`.
                mut = "mut " if counter.counts.get(a.arg, 0) >= 1 else ""
                # A `py_class!` method is only reached dynamically (its
                # instances are `Arg` handles), so a typed annotation
                # (`payload: bytes`) would make it undispatchable: `Arg`.
                dyn_params = in_class in self.dyn_param_classes and has_self and not is_static
                params.append(
                    f"{mut}{rs_name(a.arg)}: {'Arg' if dyn_params else self.param_type(a)}"
                )
        ctx = FnCtx(
            params=names,
            assigned_counts=counter.counts,
            is_method=has_self,
            is_async=isinstance(f, ast.AsyncFunctionDef),
            returns_value=counter.returns_value,
            node=f,
        )
        ctx.declared |= names
        ctx.renames = renames
        if in_class is not None:
            # `super().m(..)` resolves from the class that *defines* the
            # method -- the origin of a copied base method, else the class
            # being emitted [P12d].
            ctx.super_methods = self.super_table(super_origin or in_class, f)

        ret = ""
        if f.returns is not None:
            ret = f" -> {self.annotation(f.returns)}"
        elif fname == "__enter__" and counter.returns_value and counter.returns_self:
            # `with Cls(..) as x:` binds the typed instance: `x.method(..)`
            # is a typed call and the class's `__exit__` runs at the end.
            ret = " -> Self"
        elif counter.returns_value:
            ret = " -> Arg"

        guards: list[tuple[ast.expr, str, bool]] = []
        for d in f.decorator_list:
            dt = self.expr(d)
            if nested_decorators:
                self.line(f"#[decorate({dt})]")
                continue
            if dt == "classmethod":
                self.line("#[classmethod]")
                continue
            if dt == "property":
                # read as an attribute on an instance passed as a value [P14]
                self.line("#[property]")
                continue
            if dt == "staticmethod":
                continue
            if (
                isinstance(d, ast.Call)
                and isinstance(d.func, ast.Attribute)
                and isinstance(d.func.value, ast.Name)
                and d.func.value.id == "unittest"
                and d.func.attr in ("skipUnless", "skipIf")
                and len(d.args) == 2
                and isinstance(d.args[0], ast.Name)
                and isinstance(d.args[1], ast.Constant)
                and isinstance(d.args[1].value, str)
                and in_class is not None
            ):
                # `@unittest.skipUnless(HAVE_SERIAL, ..)` on a method, with a
                # module flag that is *not* dead: a runtime guard, as for the
                # `skip_unless_x` aliases [P2f].
                guards.append((d.args[0], d.args[1].value, d.func.attr == "skipUnless"))
                self.line(f"// @{dt} [P2f]")
                continue
            if dt.startswith("unittest.skip") or dt.startswith("pytest.mark.skip"):
                self.line(f"#[ignore = {rs_str(dt)}]")
                continue
            if isinstance(d, ast.Name) and d.id in self.skip_aliases and in_class is not None:
                # `@skip_unless_x`: `unittest.skipUnless(FLAG, reason)` by
                # another name -- the method skips itself when the flag says
                # so [P2f].
                guards.append(self.skip_aliases[d.id])
                self.line(
                    f"// @{dt}: `unittest.{'skipUnless' if self.skip_aliases[d.id][2] else 'skipIf'}({ast.unparse(self.skip_aliases[d.id][0])}, ..)` [P2f]"
                )
                continue
            self.line(f"// @{dt}")
        if needs_sig:
            # the Python signature text, inside a Rust string literal [P15]
            sig_text = f"{fname}({', '.join(sig_parts)})".replace("\\", "\\\\").replace('"', '\\"')
            self.line(f'#[pysig("{sig_text}")]')
        asyn = "async " if isinstance(f, ast.AsyncFunctionDef) else ""
        self.line(f"{asyn}fn {rs_name(fname)}({', '.join(params)}){ret} {{")
        self.indent += 1
        self.fn_stack.append(ctx)
        body = list(f.body)
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            doc = body.pop(0)
            for ln in doc.value.value.strip("\n").splitlines():
                self.line(f"// {ln}".rstrip())
            self.mark(doc)
        self.last_line = max(self.last_line, f.lineno)
        for flag, reason, unless in guards:
            cond = self.expr(flag)
            cond = f"not {cond}" if unless else cond
            self.line(f"if {cond} {{")
            self.line(f"    self.skipTest({rs_str(reason)});")
            self.line("}")
        self.block(body)
        self.fn_stack.pop()
        self.indent -= 1
        self.line("}")

    @staticmethod
    def _decorated_by_hand(fname: str, scope: ast.AST | None) -> bool:
        """Is `fname` passed to a decorator factory's result (`deco(..)(fname)`) in *scope*?"""
        if scope is None:
            return False
        return any(
            isinstance(n, ast.Call)
            and isinstance(n.func, ast.Call)
            and any(isinstance(a, ast.Name) and a.id == fname for a in n.args)
            for n in ast.walk(scope)
        )

    def param_type(self, a: ast.arg) -> str:
        if a.annotation is not None:
            return self.annotation(a.annotation)
        return "Arg"

    def annotation(self, ann: ast.expr) -> str:
        if isinstance(ann, ast.Name):
            if ann.id in self.dyn_types:
                # `dyn_types` override: the class is handled through the
                # dynamic object protocol in this file [P18e].
                return "Arg"
            return TYPE_MAP.get(ann.id, ann.id if ann.id[:1].isupper() else "Arg")
        if isinstance(ann, ast.Constant) and ann.value is None:
            return "()"
        return "Arg"

    # -- blocks and statements ----------------------------------------------

    def block(self, stmts: list[ast.stmt]):
        if not stmts:
            return
        for st in stmts:
            self.flush_comments(st.lineno)
            try:
                self.stmt(st)
            except Unsupported as u:
                self.unsupported_stmt(st, str(u))
            self.mark(st)
        # comments on the block's last lines
        end = getattr(stmts[-1], "end_lineno", stmts[-1].lineno)
        for ln in sorted(self.comments):
            if ln <= end and ln > self.last_line - 1 and self.lines[ln - 1].strip().startswith("#"):
                self.line("//" + self.comments.pop(ln)[1:])

    def stmt(self, st: ast.stmt):
        meth = getattr(self, "s_" + type(st).__name__, None)
        if meth is None:
            raise Unsupported(f"statement {type(st).__name__}")
        meth(st)

    def s_Pass(self, st: ast.Pass):
        pass

    _TESTCASE_NOOPS = ("setUp", "tearDown", "setUpClass", "tearDownClass")

    def _is_super_noop(self, e: ast.expr) -> str | None:
        """`super().setUp()` (and the other `unittest.TestCase` hooks) when
        no base of this file defines the method: the base version is
        `unittest.TestCase`'s, which does nothing [P12d]."""
        if not (
            isinstance(e, ast.Call)
            and isinstance(e.func, ast.Attribute)
            and isinstance(e.func.value, ast.Call)
            and isinstance(e.func.value.func, ast.Name)
            and e.func.value.func.id == "super"
            and not e.func.value.args
            and e.func.attr in self._TESTCASE_NOOPS
            and not e.args
            and not e.keywords
        ):
            return None
        ctx = self.fn_stack[-1] if self.fn_stack else None
        if ctx is not None and e.func.attr in ctx.super_methods:
            return None
        return e.func.attr

    def s_Expr(self, st: ast.Expr):
        if isinstance(st.value, ast.Constant) and isinstance(st.value.value, str):
            for ln in st.value.value.strip("\n").splitlines():
                self.line(f"// {ln}".rstrip())
            return
        noop = self._is_super_noop(st.value)
        if noop is not None:
            self.line(f"// py2rs: `super().{noop}()` is `unittest.TestCase.{noop}` (a no-op)")
            return
        self.line(f"{self.expr(st.value)};{self.trailing_comment(st)}")

    def s_Assign(self, st: ast.Assign):
        if len(st.targets) != 1:
            # a = b = v  -> two statements
            v = self.expr(st.value)
            for t in st.targets:
                self.assign(t, v, st)
            return
        self.assign(st.targets[0], self.expr(st.value), st)

    def assign(self, target: ast.expr, value: str, st: ast.stmt):
        ctx = self.fn_stack[-1] if self.fn_stack else None
        tc = self.trailing_comment(st)
        if isinstance(target, ast.Name):
            name = target.id
            rs = ctx.renames.get(name, rs_name(name)) if ctx is not None else rs_name(name)
            fname = (
                ctx.node.name
                if ctx is not None and isinstance(ctx.node, (ast.FunctionDef, ast.AsyncFunctionDef))
                else None
            )
            builder = self.builder_locals.get(f"{fname}.{name}") if fname else None
            if builder is not None:
                # `builder_tokens` keyed `<function>.<local>`: the computed
                # source text is a constitution token; the Rust builder runs
                # under it [P31d].
                value = f"laila::entry::constitution::builder_token_dyn({value}, {builder})"
            if ctx is not None and name not in ctx.declared:
                ctx.declared.add(name)
                mut = "mut " if ctx.assigned_counts.get(name, 0) > 1 and name != "_" else ""
                self.line(f"let {mut}{rs} = {value};{tc}")
            else:
                self.line(f"{rs} = {value};{tc}")
            return
        if isinstance(target, (ast.Tuple, ast.List)):
            names = [e for e in target.elts]

            def leaves(t: ast.expr) -> list[ast.Name] | None:
                # the leaf names of a (nested) tuple target, None when any
                # element is not a plain name
                out: list[ast.Name] = []
                for e in t.elts:  # type: ignore[attr-defined]
                    if isinstance(e, ast.Name):
                        out.append(e)
                    elif isinstance(e, (ast.Tuple, ast.List)):
                        sub = leaves(e)
                        if sub is None:
                            return None
                        out.extend(sub)
                    else:
                        return None
                return out

            leaf_names = leaves(target)
            if leaf_names is not None and ctx is not None:
                # `_` is a discard pattern, never a binding: it is fresh in
                # every tuple target (`p1, _ = f()` then `p2, _ = f()`).
                fresh = [e.id for e in leaf_names if e.id == "_" or e.id not in ctx.declared]
                if len(fresh) == len(leaf_names):
                    for e in leaf_names:
                        if e.id != "_":
                            ctx.declared.add(e.id)
                    anymut = any(ctx.assigned_counts.get(e.id, 0) > 1 for e in leaf_names)

                    def pat_of(t: ast.expr) -> str:
                        if isinstance(t, ast.Name):
                            return (
                                "_" if t.id == "_" else ("mut " if anymut else "") + rs_name(t.id)
                            )
                        inner = ", ".join(pat_of(e) for e in t.elts)  # type: ignore[attr-defined]
                        if len(t.elts) == 1:  # type: ignore[attr-defined]
                            inner += ","
                        return f"({inner})"

                    self.line(f"let {pat_of(target)} = {value};{tc}")
                    return
            pat = ", ".join(self.expr(e) for e in names)
            self.line(f"({pat}) = {value};{tc}")
            return
        # attribute / subscript targets are expressions the macro rewrites
        self.line(f"{self.expr(target)} = {value};{tc}")

    def s_AnnAssign(self, st: ast.AnnAssign):
        ctx = self.fn_stack[-1] if self.fn_stack else None
        ty = self.annotation(st.annotation)
        name = self.expr(st.target)
        if st.value is None:
            self.line(f"let mut {name}: {ty};")
            if ctx is not None and isinstance(st.target, ast.Name):
                ctx.declared.add(st.target.id)
            return
        v = self.expr(st.value)
        if ctx is not None and isinstance(st.target, ast.Name) and st.target.id not in ctx.declared:
            ctx.declared.add(st.target.id)
            mut = "mut " if ctx.assigned_counts.get(st.target.id, 0) > 1 else ""
            self.line(f"let {mut}{name}: {ty} = {v};{self.trailing_comment(st)}")
        else:
            self.line(f"{name} = {v};{self.trailing_comment(st)}")

    def s_AugAssign(self, st: ast.AugAssign):
        sym, _ = BINOP[type(st.op)]
        if isinstance(st.op, (ast.FloorDiv, ast.Pow, ast.MatMult)):
            fn = {ast.FloorDiv: "floordiv", ast.Pow: "pow", ast.MatMult: "matmul"}[type(st.op)]
            t = self.expr(st.target)
            self.line(f"{t} = {fn}({t}, {self.expr(st.value)});{self.trailing_comment(st)}")
            return
        self.line(
            f"{self.expr(st.target)} {sym}= {self.expr(st.value)};{self.trailing_comment(st)}"
        )

    def s_Return(self, st: ast.Return):
        if st.value is None:
            self.line(f"return;{self.trailing_comment(st)}")
        else:
            value = self.expr(st.value)
            ctx = self.fn_stack[-1] if self.fn_stack else None
            fname = (
                ctx.node.name
                if ctx is not None and isinstance(ctx.node, (ast.FunctionDef, ast.AsyncFunctionDef))
                else None
            )
            builder = self.builder_functions.get(fname or "")
            if builder is not None:
                # `builder_functions` override: the returned source text is a
                # constitution token; the Rust builder runs under it [P31c].
                value = f"laila::entry::constitution::builder_token_dyn({value}, {builder})"
            self.line(f"return {value};{self.trailing_comment(st)}")

    def s_Assert(self, st: ast.Assert):
        if st.msg is not None:
            self.line(
                f"assert({self.expr(st.test)}, {self.expr(st.msg)});{self.trailing_comment(st)}"
            )
        else:
            self.line(f"assert({self.expr(st.test)});{self.trailing_comment(st)}")

    def s_Raise(self, st: ast.Raise):
        if st.exc is None:
            self.line(f"raise;{self.trailing_comment(st)}")
        elif st.cause is not None:
            self.line(
                f"raise {self.expr(st.exc)} from {self.expr(st.cause)};{self.trailing_comment(st)}"
            )
        else:
            self.line(f"raise {self.expr(st.exc)};{self.trailing_comment(st)}")

    def s_Delete(self, st: ast.Delete):
        for t in st.targets:
            self.line(f"del {self.expr(t)};")

    def s_Global(self, st: ast.Global):
        raise Unsupported("global")

    def s_Nonlocal(self, st: ast.Nonlocal):
        raise Unsupported("nonlocal")

    def s_Break(self, st: ast.Break):
        self.line("break;")

    def s_Continue(self, st: ast.Continue):
        self.line("continue;")

    def s_Import(self, st: ast.Import):
        # A function-level import binds a local name in Python; its Rust twin
        # is the module-level `use` (hoisted), the comment keeps the position [P2].
        self.line(f"// {ast.unparse(st)}")
        self.import_(st)

    s_ImportFrom = s_Import

    def _predeclare_escaping(self, inner_blocks: list[list[ast.stmt]]) -> None:
        """Names first bound inside *inner_blocks* (a loop body, the
        branches of an `if`) and read after the statement: Python binds
        them in the function scope, so they are declared here [P16b]."""
        for name in self._escaping_names_of(inner_blocks):
            self.fn_stack[-1].declared.add(name)
            self.line(f"let mut {rs_name(name)} = None;")

    def s_If(self, st: ast.If):
        # bound in a branch, read after the `if`: declared here [P16b]
        self._predeclare_escaping(self._if_branches(st))
        self.line(f"if {self.expr(st.test)} {{")
        self.indent += 1
        self._scoped_block(st.body)
        self.indent -= 1
        node = st
        while node.orelse:
            if len(node.orelse) == 1 and isinstance(node.orelse[0], ast.If):
                node = node.orelse[0]
                self.flush_comments(node.lineno)
                self.line(f"}} else if {self.expr(node.test)} {{")
                self.indent += 1
                self._scoped_block(node.body)
                self.indent -= 1
            else:
                self.line("} else {")
                self.indent += 1
                self._scoped_block(node.orelse)
                self.indent -= 1
                break
        self.line("}")

    def _class_loop(self, st: ast.For) -> list[str] | None:
        """`for cls in (AtomicInt, AtomicDict, ..):` -- a loop over imported
        *classes* [P19d]: Rust has no class values to iterate, so the body
        is unrolled once per class with `cls` -> the class and
        `cls.__name__` -> its name."""
        if isinstance(st, ast.AsyncFor) or st.orelse or not isinstance(st.target, ast.Name):
            return None
        if not isinstance(st.iter, (ast.Tuple, ast.List)) or not st.iter.elts:
            return None
        names = []
        for e in st.iter.elts:
            if not (isinstance(e, ast.Name) and e.id in self.imported_names and e.id[:1].isupper()):
                return None
            names.append(e.id)
        return names

    def s_For(self, st: ast.For):
        if st.orelse:
            raise Unsupported("for/else")
        classes = self._class_loop(st)
        if classes is not None:
            var = st.target.id  # type: ignore[attr-defined]

            class Bind(ast.NodeTransformer):
                def __init__(self, cls: str):
                    self.cls = cls

                def visit_Attribute(self, n: ast.Attribute):
                    if isinstance(n.value, ast.Name) and n.value.id == var and n.attr == "__name__":
                        return ast.copy_location(ast.Constant(value=self.cls), n)
                    return self.generic_visit(n)

                def visit_Name(self, n: ast.Name):
                    if n.id == var and isinstance(n.ctx, ast.Load):
                        return ast.copy_location(ast.Name(id=self.cls, ctx=ast.Load()), n)
                    return n

            self.line(
                f"// py2rs: `for {var} in ({', '.join(classes)})` unrolled, one block per class [P19d]"
            )
            fctx = self.fn_stack[-1] if self.fn_stack else None
            for cls in classes:
                body = [Bind(cls).visit(copy.deepcopy(s)) for s in st.body]
                # each block is its own Rust scope: a name first bound in
                # the body is `let` again in the next copy
                declared = set(fctx.declared) if fctx is not None else None
                self.line("{")
                self.indent += 1
                self.block(body)
                self.indent -= 1
                self.line("}")
                if fctx is not None:
                    fctx.declared = declared  # type: ignore[assignment]
            return
        asyn = "async " if isinstance(st, ast.AsyncFor) else ""
        self._predeclare_escaping([st.body])
        self.line(f"{asyn}for {ExprPrinter().target(st.target)} in {self.expr(st.iter)} {{")
        self.indent += 1
        self._scoped_block(st.body)
        self.indent -= 1
        self.line("}")

    s_AsyncFor = s_For

    def s_While(self, st: ast.While):
        if st.orelse:
            raise Unsupported("while/else")
        self._predeclare_escaping([st.body])
        self.line(f"while {self.expr(st.test)} {{")
        self.indent += 1
        self._scoped_block(st.body)
        self.indent -= 1
        self.line("}")

    def s_With(self, st: ast.With):
        asyn = "async " if isinstance(st, ast.AsyncWith) else ""
        # nested `with a, b:` -> nested blocks. The macro inlines a `with`
        # body in the enclosing block (Python's `with` opens no scope), so
        # the names it binds stay declared after it.
        items = st.items
        for i, it in enumerate(items):
            cm = self.expr(it.context_expr)
            tgt = (
                f" as {ExprPrinter().target(it.optional_vars)}"
                if it.optional_vars is not None
                else ""
            )
            if (
                it.optional_vars is not None
                and self.fn_stack
                and isinstance(it.optional_vars, ast.Name)
            ):
                self.fn_stack[-1].declared.add(it.optional_vars.id)
            self.line(f"{asyn}with {cm}{tgt} {{")
            self.indent += 1
        self.block(st.body)
        for _ in items:
            self.indent -= 1
            self.line("}")

    s_AsyncWith = s_With

    def _escaping_names(self, st: ast.Try) -> list[str]:
        """Names first bound inside a `try` body / handler and read outside
        it (in a handler, `finally`, or later in the function) [P16b].
        Python's `try` opens no scope; the Rust twin runs the body as a
        closure, so such names are declared before the statement
        (`let mut name = None;`) and assigned inside it."""
        return self._escaping_names_of([st.body] + [h.body for h in st.handlers])

    def _if_branches(self, st: ast.If) -> list[list[ast.stmt]]:
        """The bodies of an `if` / `elif` / `else` chain."""
        blocks = [st.body]
        node = st
        while node.orelse:
            if len(node.orelse) == 1 and isinstance(node.orelse[0], ast.If):
                node = node.orelse[0]
                blocks.append(node.body)
            else:
                blocks.append(node.orelse)
                break
        return blocks

    @staticmethod
    def _walk_scope(node: ast.AST):
        """`ast.walk` that does not descend into nested scopes (a `def`,
        `lambda` or `class` body binds its own names)."""
        stack = [node]
        while stack:
            n = stack.pop()
            yield n
            for c in ast.iter_child_nodes(n):
                if isinstance(c, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
                    continue
                stack.append(c)

    def _escaping_names_of(self, inner_blocks: list[list[ast.stmt]]) -> list[str]:
        """Names first bound inside *inner_blocks* (branches of one
        statement, a loop body) whose first use after the statement is a
        *read* (a plain read, an `x += ..`, or a read inside a nested
        function defined later): Python binds them in the function scope,
        a Rust `let` inside the block would not outlive it [P16b]. A name
        re-bound after the statement before being read is simply `let`
        again there."""
        ctx = self.fn_stack[-1] if self.fn_stack else None
        if ctx is None or ctx.node is None:
            return []
        bound: list[str] = []
        for block in inner_blocks:
            for node in block:
                for sub in self._walk_scope(node):
                    targets: list[ast.expr] = []
                    if isinstance(sub, ast.Assign):
                        targets = list(sub.targets)
                    elif isinstance(sub, (ast.AnnAssign, ast.AugAssign)):
                        targets = [sub.target]
                    elif isinstance(sub, (ast.With, ast.AsyncWith)):
                        targets = [
                            it.optional_vars for it in sub.items if it.optional_vars is not None
                        ]
                    for t in targets:
                        elts = t.elts if isinstance(t, (ast.Tuple, ast.List)) else [t]
                        for n in elts:
                            if (
                                isinstance(n, ast.Name)
                                and n.id not in ctx.declared
                                and n.id not in bound
                            ):
                                bound.append(n.id)
        if not bound:
            return []
        inside: set[int] = set()
        end = 0
        for block in inner_blocks:
            for node in block:
                inside.update(id(n) for n in ast.walk(node))
                end = max(end, getattr(node, "end_lineno", None) or node.lineno)
        # the first occurrence of each name after the statement
        first: dict[str, tuple[tuple[int, int], bool]] = {}
        for node in ast.walk(ctx.node):
            if id(node) in inside or node is ctx.node:
                continue
            if (
                isinstance(node, ast.AugAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id in bound
            ):
                t = node.target
                if t.lineno > end:
                    key = (t.lineno, t.col_offset)
                    if t.id not in first or key <= first[t.id][0]:
                        first[t.id] = (key, True)
                continue
            if isinstance(node, ast.Name) and node.id in bound and node.lineno > end:
                key = (node.lineno, node.col_offset)
                if node.id not in first or key < first[node.id][0]:
                    first[node.id] = (key, isinstance(node.ctx, ast.Load))
        return [n for n in bound if n in first and first[n][1]]

    def _scoped_block(self, body: list[ast.stmt]) -> None:
        """Emit *body* as a Rust block: the names it is the first to bind
        are block-local (`let` inside `{ .. }`), so a later binding of the
        same name outside is a fresh `let` again [P16b]."""
        ctx = self.fn_stack[-1] if self.fn_stack else None
        before = set(ctx.declared) if ctx is not None else None
        self.block(body)
        if ctx is not None and before is not None:
            ctx.declared = before

    def s_Try(self, st: ast.Try):
        # try/except/finally -> `try { } except E as e { } finally { }` [P16];
        # the macro lowers it to a Result closure + isinstance match.
        if st.orelse:
            raise Unsupported("try/else")
        for name in self._escaping_names(st):
            # bound in the body, read after it: declared here [P16b]
            self.fn_stack[-1].declared.add(name)
            self.line(f"let mut {rs_name(name)} = None;")
        self.line("try {")
        self.indent += 1
        self._scoped_block(st.body)
        self.indent -= 1
        for h in st.handlers:
            self.flush_comments(h.lineno)
            ty = self.expr(h.type) if h.type is not None else "Exception"
            name = f" as {rs_name(h.name)}" if h.name else ""
            self.line(f"}} except {ty}{name} {{")
            self.indent += 1
            if h.name and self.fn_stack:
                # the bound exception is local to its handler block
                before = set(self.fn_stack[-1].declared)
                self.fn_stack[-1].declared.add(h.name)
                self.block(h.body)
                self.fn_stack[-1].declared = before
            else:
                self._scoped_block(h.body)
            self.indent -= 1
        if st.finalbody:
            self.line("} finally {")
            self.indent += 1
            self._scoped_block(st.finalbody)
            self.indent -= 1
        self.line("}")

    s_TryStar = s_Try

    def s_FunctionDef(self, st: ast.FunctionDef):
        # nested def -> local fn item inside the body (closures over locals are
        # not expressible as items; the macro treats inner `fn` as an item)
        self.function(st, in_class=None)

    s_AsyncFunctionDef = s_FunctionDef

    def s_ClassDef(self, st: ast.ClassDef):
        # A class defined inside a function is an item inside the Rust body
        # (it cannot capture locals, as in Rust).
        if st.name in self.class_aliases:
            # `class_aliases` override on a method-local subclass [P18].
            self.line(
                f"// py2rs: class `{st.name}` aliased to `{self.class_aliases[st.name]}` (override)"
            )
            return
        self.class_(st)

    def s_Match(self, st: ast.Match):
        raise Unsupported("match statement")


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def rustfmt(text: str) -> str:
    """Run `rustfmt` over *text* (the workspace `rustfmt.toml` applies)."""
    import shutil
    import subprocess

    exe = shutil.which("rustfmt")
    if exe is None:
        print("py2rs: rustfmt not found; script output left unformatted", file=sys.stderr)
        return text
    cfg = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "rustfmt.toml")
    cmd = [exe, "--edition", "2021", "--emit", "stdout"]
    if os.path.exists(cfg):
        cmd += ["--config-path", cfg]
    out = subprocess.run(cmd, input=text, capture_output=True, text=True)
    if out.returncode != 0:
        print(f"py2rs: rustfmt failed: {out.stderr.strip()}", file=sys.stderr)
        return text
    return out.stdout


def convert(
    source: str,
    rel_path: str,
    skip_tests: set[str] | None = None,
    skip_tests_reason: str = "",
    skip_imports: set[str] | None = None,
    present_imports: set[str] | None = None,
    skip_functions: set[str] | None = None,
    skip_reasons: dict[str, str] | None = None,
    builder_tokens: dict[str, str] | None = None,
    extra_uses: list[str] | None = None,
    child_scripts: dict[str, str] | None = None,
    class_aliases: dict[str, str] | None = None,
    builder_functions: dict[str, str] | None = None,
    import_twins: dict[str, str] | None = None,
    dyn_types: set[str] | None = None,
    src_path: str | None = None,
    overrides_dir: str | None = None,
    root: str | None = None,
    script: bool = False,
) -> tuple[str, Emitter]:
    tree = ast.parse(source, filename=rel_path)
    # A pytest-style module (bare `test_*` functions, `Test*` classes,
    # fixtures, `parametrize`) is first rewritten into unittest shape [P32].
    tree, _was_pytest = pytest_desugar.desugar(tree, src_path)
    em = Emitter(source, rel_path)
    em.script = script
    em.src_path = src_path
    em.overrides_dir = overrides_dir
    em.root = root
    em.skip_tests = skip_tests or set()
    em.skip_tests_reason = skip_tests_reason
    em.skip_imports = skip_imports or set()
    em.present_imports = present_imports or set()
    em.skip_functions = skip_functions or set()
    em.skip_reasons = skip_reasons or {}
    em.builder_tokens = {k: v for k, v in (builder_tokens or {}).items() if k.isidentifier()}
    em.builder_locals = {
        k: v
        for k, v in (builder_tokens or {}).items()
        if "." in k and k.replace(".", "").isidentifier()
    }
    ExprPrinter.inline_tokens = {
        k: v
        for k, v in (builder_tokens or {}).items()
        if not k.isidentifier() and k not in em.builder_locals
    }
    ExprPrinter.class_renames = {}
    em.child_scripts = child_scripts or {}
    em.class_aliases = class_aliases or {}
    em.builder_functions = builder_functions or {}
    em.import_twins = import_twins or {}
    em.dyn_types = dyn_types or set()
    em.uses.extend(f"use {path} as {name};" for name, path in em.class_aliases.items())
    # `use` override: extra `use` lines (a Rust-side helper the Python file
    # reaches through `conftest.py` / fixtures) [P18].
    em.uses.extend(f"use {u};" if not u.startswith("use ") else u for u in (extra_uses or []))
    text = em.module(tree)
    return text, em


def load_override(overrides_dir: str | None, rel_path: str) -> dict[str, object]:
    """Sidecar `overrides/<rel path>.yaml` (subset: `key: value`, `key: [a, b]`) [P18].

    Keys: `skip: <reason>` (whole file has no Rust twin; nothing is emitted),
    `handwritten: <reason>` (the `.rs` twin beside the generated files is
    written by hand -- declared in `main.rs`, never regenerated, P18d),
    `skip_tests: [name, ...]` (those methods are dropped, with an optional
    `skip_tests_reason: <text>`), `skip_imports: [module, ...]` (those
    `import` / `from .. import` lines become comments: the module has no
    Rust twin yet), `present_imports: [module, ...]` (optional dependencies
    the Rust twin satisfies natively: the optional-import `try` takes its
    body branch), `use: [path, ...]` (extra `use` lines),
    `builder_tokens:` (mapping: module constant -> Rust builder path, see
    P31; a quoted key is the inline source literal itself, see P31b; a
    `<function>.<local>` key is a local assigned a computed text, P31d),
    `child_scripts:` (mapping: module constant holding child-interpreter
    source -> Rust child function, see P32), `class_aliases:` (mapping:
    test-local subclass -> Rust type path; the class body is dropped and
    the name imported as that type), `builder_functions:` (mapping:
    module-level function computing constitution source -> Rust builder
    `fn(&str, &Arg) -> Result<Arg>` registered under each returned text,
    see P31c), `import_twins:` (mapping: imported laila name -> Rust path
    of a Python-shaped twin bound under that name by the `use`), `dyn_types: [Class, ...]` (annotations naming those classes
    lower to `Arg`: the file drives them through the dynamic protocol,
    P18e). `skip_tests` and `skip_functions` (module-level defs) also take
    the mapping form with a one-line reason per name:

        skip_tests:
          test_name: reason
    """
    if overrides_dir is None:
        return {}
    path = os.path.join(overrides_dir, os.path.splitext(rel_path)[0] + ".yaml")
    if not os.path.exists(path):
        return {}
    out: dict[str, object] = {}
    reasons: dict[str, str] = {}
    section: str | None = None
    for line in open(path, encoding="utf-8"):
        if line.lstrip()[:1] not in ('"', "'"):
            line = line.split("#", 1)[0]
        line = line.rstrip()
        if not line.strip() or ":" not in line:
            continue
        if line[0] in " \t" and section in ("skip_tests", "skip_functions"):
            # mapping form: `  name: one-line reason`
            k, _, v = line.strip().partition(":")
            names = out.setdefault(section, [])
            assert isinstance(names, list)
            names.append(k.strip())
            reasons[k.strip()] = v.strip().strip("'\"")
            continue
        if line[0] in " \t" and section in (
            "builder_tokens",
            "child_scripts",
            "class_aliases",
            "builder_functions",
            "import_twins",
        ):
            # mapping form: `  CONSTANT: rust::path::to::fn`, or, for source
            # written inline in a test body, the literal itself as the key:
            # `  "def build(m):\n    ..": rust::path::to::fn` [P31b]
            mapping = out.setdefault(section, {})
            assert isinstance(mapping, dict)
            stripped = line.strip()
            m = re.match(r'^("(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\')\s*:\s*(.*)$', stripped)
            if m is not None:
                mapping[ast.literal_eval(m.group(1))] = m.group(2).strip().strip("'\"")
                continue
            k, _, v = stripped.partition(":")
            mapping[k.strip()] = v.strip().strip("'\"")
            continue
        k, _, v = line.partition(":")
        v = v.strip()
        section = k.strip()
        if v.startswith("[") and v.endswith("]"):
            out[k.strip()] = [x.strip().strip("'\"") for x in v[1:-1].split(",") if x.strip()]
        elif v == "":
            out.setdefault(
                k.strip(),
                {}
                if section
                in (
                    "builder_tokens",
                    "child_scripts",
                    "class_aliases",
                    "builder_functions",
                    "import_twins",
                )
                else [],
            )
        else:
            out[k.strip()] = v.strip("'\"")
    if reasons:
        out["skip_reasons"] = reasons
    return out


def iter_py_files(path: str) -> Iterable[str]:
    if os.path.isfile(path):
        yield path
        return
    for root, dirs, files in os.walk(path):
        dirs.sort()
        for f in sorted(files):
            if f.endswith(".py") and f != "__init__.py" and f != "conftest.py":
                yield os.path.join(root, f)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("inputs", nargs="+", help="Python files or directories")
    ap.add_argument(
        "-o", "--output", required=True, help="output .rs file (single input file) or directory"
    )
    ap.add_argument(
        "--root", default=None, help="repository root for relative paths in headers (default: cwd)"
    )
    ap.add_argument(
        "--check", action="store_true", help="do not write; exit 1 if any output differs"
    )
    ap.add_argument(
        "--overrides",
        default=None,
        help="sidecar directory (overrides/<source rel path>.yaml); default: <repo root>/laila-rs/tests/overrides",
    )
    ap.add_argument(
        "--script",
        action="store_true",
        help="program mode: top-level statements become `fn main()` (examples, tutorials) [P33]",
    )
    ap.add_argument("-q", "--quiet", action="store_true")
    ns = ap.parse_args(argv)
    root = os.path.abspath(ns.root or os.getcwd())

    files: list[str] = []
    for inp in ns.inputs:
        files.extend(iter_py_files(inp))
    single = (
        len(files) == 1
        and os.path.isfile(ns.inputs[0])
        and (ns.output.endswith(".rs") or not os.path.isdir(ns.output))
    )

    overrides_dir = ns.overrides
    if overrides_dir is None:
        cand = os.path.join(root, "laila-rs", "tests", "overrides")
        overrides_dir = cand if os.path.isdir(cand) else None

    changed = 0
    total_unsupported = 0
    skipped: list[str] = []
    outputs: list[tuple[str, str]] = []  # (path, text)
    handwritten: list[str] = []  # module paths kept by hand (listed in main.rs, never regenerated)
    for f in files:
        src = open(f, encoding="utf-8").read()
        rel = os.path.relpath(os.path.abspath(f), root).replace(os.sep, "/")
        ov = load_override(overrides_dir, rel)
        if "handwritten" in ov:
            # `handwritten: <reason>` override: the `.rs` beside the generated
            # files is a hand-written twin of the whole test module (its
            # fakes are Rust driver hooks); it is declared in `main.rs` like
            # a generated one but never written by py2rs [P18d].
            skipped.append(f"{rel}: hand-written twin ({ov['handwritten']})")
            if not single:
                base = (
                    os.path.relpath(f, ns.inputs[0])
                    if os.path.isdir(ns.inputs[0])
                    else os.path.basename(f)
                )
                handwritten.append(os.path.join(ns.output, os.path.splitext(base)[0] + ".rs"))
            continue
        if "skip" in ov:
            skipped.append(f"{rel}: {ov['skip']}")
            continue
        text, em = convert(
            src,
            rel,
            skip_tests=set(ov.get("skip_tests", []) or []),
            skip_tests_reason=str(ov.get("skip_tests_reason", "") or ""),
            skip_imports=set(ov.get("skip_imports", []) or []),
            present_imports=set(ov.get("present_imports", []) or []),
            skip_functions=set(ov.get("skip_functions", []) or []),
            skip_reasons=ov.get("skip_reasons", {}) or {},  # type: ignore[arg-type]
            builder_tokens=ov.get("builder_tokens", {}) or {},  # type: ignore[arg-type]
            extra_uses=list(ov.get("use", []) or []),  # type: ignore[arg-type]
            child_scripts=ov.get("child_scripts", {}) or {},  # type: ignore[arg-type]
            class_aliases=ov.get("class_aliases", {}) or {},  # type: ignore[arg-type]
            builder_functions=ov.get("builder_functions", {}) or {},  # type: ignore[arg-type]
            import_twins=ov.get("import_twins", {}) or {},  # type: ignore[arg-type]
            dyn_types=set(ov.get("dyn_types", []) or []),
            src_path=os.path.abspath(f),
            overrides_dir=overrides_dir,
            root=root,
            script=ns.script,
        )
        total_unsupported += len(em.unsupported)
        if single:
            out_path = ns.output
        else:
            base = (
                os.path.relpath(f, ns.inputs[0])
                if os.path.isdir(ns.inputs[0])
                else os.path.basename(f)
            )
            out_path = os.path.join(ns.output, os.path.splitext(base)[0] + ".rs")
        outputs.append((out_path, text))
        if not ns.quiet:
            for u in em.unsupported:
                print(f"unsupported: {u}", file=sys.stderr)
            for n in em.notes:
                print(f"note: {rel}: {n}", file=sys.stderr)

    # Directory mode: one test binary per Python package directory [P17]
    # (programs are one binary each: no `main.rs`).
    if not single and not ns.script:
        by_dir: dict[str, list[str]] = {}
        for out_path, _ in outputs:
            by_dir.setdefault(os.path.dirname(out_path), []).append(
                os.path.splitext(os.path.basename(out_path))[0]
            )
        for out_path in handwritten:
            by_dir.setdefault(os.path.dirname(out_path), []).append(
                os.path.splitext(os.path.basename(out_path))[0]
            )
        for d, mods in sorted(by_dir.items()):
            rel_dir = os.path.relpath(os.path.abspath(d), os.path.abspath(ns.output)).replace(
                os.sep, "/"
            )
            src_dir = os.path.relpath(
                os.path.abspath(os.path.join(ns.inputs[0], rel_dir)), root
            ).replace(os.sep, "/")
            lines = [f"//! Generated by py2rs: test binary for `{src_dir}`."]
            # `#[rustfmt::skip]` on the declaration keeps rustfmt out of the
            # generated module, so `cargo fmt` and `py2rs --check` agree.
            lines += [f"#[rustfmt::skip]\nmod {m};" for m in sorted(mods)]
            outputs.append((os.path.join(d, "main.rs"), "\n".join(lines) + "\n"))

    for out_path, text in outputs:
        existing = open(out_path, encoding="utf-8").read() if os.path.exists(out_path) else None
        if existing != text:
            changed += 1
            if ns.check:
                print(f"differs: {out_path}", file=sys.stderr)
            else:
                os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
                with open(out_path, "w", encoding="utf-8") as fh:
                    fh.write(text)
    if not ns.quiet:
        for s in skipped:
            print(f"skipped (override): {s}", file=sys.stderr)
        print(
            f"py2rs: {len(files)} file(s), {len(skipped)} skipped, {changed} written/changed, {total_unsupported} unsupported statement(s)",
            file=sys.stderr,
        )
    if ns.check and changed:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
