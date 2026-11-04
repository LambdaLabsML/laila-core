"""pytest-style test modules -> unittest-style, at the Python AST level [P32].

py2rs's statement rules are written for `unittest.TestCase` classes. A
pytest-style module (`tests/deep_eval`) -- bare `test_*` functions, `Test*`
classes without a base, `@pytest.fixture` (including `yield` fixtures,
`params=`, `autouse=True`, fixtures from the sibling `conftest.py`), the
built-in `tmp_path` / `monkeypatch` fixtures and `@pytest.mark.parametrize`
-- is first rewritten into that shape, so the rest of py2rs needs no new
rules:

- `class TestX:` -> `class TestX(unittest.TestCase):`; module-level
  `def test_x(..)` functions are gathered into `class TestModule(unittest.TestCase)`.
- `@pytest.fixture def fx(deps): pre; yield v; post` -> a plain function
  `_fx_fx(deps)` returning `(v, teardown)` where `teardown` is a nested
  `def` running `post` (a `try: .. yield v .. finally: fin` fixture keeps
  the `finally`). A fixture without `yield` keeps its `return`.
  `request.param` of a `params=[..]` fixture becomes the `__param` argument.
- a test taking fixtures resolves them in dependency order at the top of
  its body and runs its teardowns, in reverse, in a `finally`:

      def test_x(self):
          __teardowns = []
          try:
              tmp_path = pytest.tmp_path()
              fresh_policy, __td = _fx_fresh_policy(); __teardowns.append(__td)
              sq, __td = _fx_sq(fresh_policy, tmp_path); __teardowns.append(__td)
              <body>
          finally:
              pytest.run_teardowns(__teardowns)

- `@pytest.mark.parametrize("a,b", [(1, 2), ..], ids=[..])` and a
  `params=[..]` fixture expand the test into one method per case,
  `test_x__<id>`, with `a = 1; b = 2` prepended (pytest's own ids where
  they are plain strings / numbers, the case index otherwise).
- `@pytest.mark.skipif(cond, reason=..)` -> `@unittest.skipIf(cond, reason)`;
  `@pytest.mark.xfail(..)` -> `@unittest.skip("xfail: ..")`.
"""

from __future__ import annotations

import ast
import copy
import itertools
import os
import re
from dataclasses import dataclass

BUILTIN_FIXTURES = {"tmp_path", "monkeypatch", "tmp_path_factory", "capsys", "caplog"}


@dataclass
class Fixture:
    name: str
    node: ast.FunctionDef
    deps: list[str]
    params: list[ast.expr] | None
    autouse: bool
    is_yield: bool
    cls: str | None  # defining class (a method fixture) or None
    ids: list[str] | None = None
    # A *callable* fixture (`def run(): def _run(coro): return
    # asyncio.run(coro); return _run`): `(param names, body expr)`. Calls
    # `run(X)` in a test are inlined to the expression (a Rust nested
    # `async fn`'s future cannot travel through a dynamic callable).
    inline: tuple[list[str], ast.expr] | None = None


def _callable_fixture(fn: ast.FunctionDef) -> tuple[list[str], ast.expr] | None:
    """`def fx(): [doc] def _f(a, b): return EXPR; return _f` -> `([a, b], EXPR)`."""
    body = [
        st
        for st in fn.body
        if not (isinstance(st, ast.Expr) and isinstance(st.value, ast.Constant))
    ]
    if (
        len(body) != 2
        or not isinstance(body[0], ast.FunctionDef)
        or not isinstance(body[1], ast.Return)
    ):
        return None
    inner, ret = body[0], body[1]
    if not (isinstance(ret.value, ast.Name) and ret.value.id == inner.name):
        return None
    if (
        len(inner.body) != 1
        or not isinstance(inner.body[0], ast.Return)
        or inner.body[0].value is None
    ):
        return None
    if inner.args.vararg or inner.args.kwarg or inner.args.kwonlyargs or inner.args.defaults:
        return None
    return ([a.arg for a in inner.args.args], inner.body[0].value)


class _InlineCalls(ast.NodeTransformer):
    """`name(X, ..)` -> the callable fixture's body with its parameters bound."""

    def __init__(self, inlines: dict[str, tuple[list[str], ast.expr]]):
        self.inlines = inlines

    def visit_Call(self, n: ast.Call):
        self.generic_visit(n)
        if isinstance(n.func, ast.Name) and n.func.id in self.inlines and not n.keywords:
            params, expr = self.inlines[n.func.id]
            if len(params) == len(n.args):
                binds = dict(zip(params, n.args))

                class _Sub(ast.NodeTransformer):
                    def visit_Name(self, m: ast.Name):
                        if isinstance(m.ctx, ast.Load) and m.id in binds:
                            return copy.deepcopy(binds[m.id])
                        return m

                return ast.copy_location(_Sub().visit(copy.deepcopy(expr)), n)
        return n


def _dec_name(d: ast.expr) -> str:
    return ast.unparse(d.func if isinstance(d, ast.Call) else d)


def _is_fixture_dec(d: ast.expr) -> bool:
    return _dec_name(d) in ("pytest.fixture", "fixture")


def _kw(call: ast.expr, name: str) -> ast.expr | None:
    if isinstance(call, ast.Call):
        for k in call.keywords:
            if k.arg == name:
                return k.value
    return None


def _has_yield(fn: ast.FunctionDef) -> bool:
    for n in ast.walk(fn):
        if isinstance(n, (ast.Yield, ast.YieldFrom)):
            return True
    return False


def _const_id(v: ast.expr, argname: str, i: int) -> str:
    """pytest's id for one parameter value, as a Rust identifier fragment."""
    if isinstance(v, ast.Constant):
        c = v.value
        if isinstance(c, bool):
            return "True" if c else "False"
        if c is None:
            return "None"
        if isinstance(c, (int, float)):
            s = repr(c).replace("-", "minus").replace(".", "_").replace("+", "")
            return s
        if isinstance(c, str):
            s = re.sub(r"[^A-Za-z0-9_]+", "_", c).strip("_")
            return s or "empty"
        if isinstance(c, bytes):
            s = re.sub(r"[^A-Za-z0-9_]+", "_", c.decode("latin-1")).strip("_")
            return s or "empty"
    return f"{argname}{i}"


class PytestDesugar(ast.NodeTransformer):
    def __init__(self, tree: ast.Module, conftest: ast.Module | None, module_name: str):
        self.tree = tree
        self.conftest = conftest
        self.module_name = module_name
        self.fixtures: dict[str, Fixture] = {}  # module-level (incl. conftest)
        self.class_fixtures: dict[str, dict[str, Fixture]] = {}
        self.conftest_fixtures: dict[str, Fixture] = {}
        self.conftest_imports: list[ast.stmt] = []
        self.used_conftest: set[str] = set()
        self.notes: list[str] = []

    # -- detection ----------------------------------------------------------

    @staticmethod
    def is_pytest_module(tree: ast.Module) -> bool:
        has_testcase = any(
            isinstance(c, ast.ClassDef)
            and any(ast.unparse(b).endswith("TestCase") for b in c.bases)
            for c in tree.body
        )
        if has_testcase:
            return False
        has_tests = any(
            (isinstance(n, ast.FunctionDef) and n.name.startswith("test_"))
            or (isinstance(n, ast.ClassDef) and n.name.startswith("Test"))
            for n in tree.body
        )
        imports_pytest = any(
            (isinstance(n, ast.Import) and any(a.name == "pytest" for a in n.names))
            or (isinstance(n, ast.ImportFrom) and n.module == "pytest")
            for n in tree.body
        )
        return has_tests and imports_pytest

    # -- fixtures -----------------------------------------------------------

    def _collect_fixture(self, fn: ast.FunctionDef, cls: str | None) -> Fixture | None:
        decs = [d for d in fn.decorator_list if _is_fixture_dec(d)]
        if not decs:
            return None
        d = decs[0]
        params = _kw(d, "params")
        params_list = list(params.elts) if isinstance(params, (ast.List, ast.Tuple)) else None
        ids = _kw(d, "ids")
        ids_list = None
        if isinstance(ids, (ast.List, ast.Tuple)) and all(
            isinstance(e, ast.Constant) for e in ids.elts
        ):
            ids_list = [str(e.value) for e in ids.elts]
        autouse = _kw(d, "autouse")
        autouse_v = bool(isinstance(autouse, ast.Constant) and autouse.value)
        deps = [a.arg for a in fn.args.args if a.arg not in ("self", "request")]
        fx = Fixture(fn.name, fn, deps, params_list, autouse_v, _has_yield(fn), cls, ids_list)
        fx.inline = _callable_fixture(fn)
        return fx

    def _rewrite_fixture(self, fx: Fixture) -> ast.FunctionDef:
        """The fixture as a plain function `_fx_<name>` (see the module doc)."""
        fn = copy.deepcopy(fx.node)
        fn.name = f"_fx_{fx.name}"
        fn.decorator_list = [d for d in fn.decorator_list if not _is_fixture_dec(d)]
        # `request` -> `__param`; `request.param` -> `__param`
        for a in fn.args.args:
            if a.arg == "request":
                a.arg = "__param"

        class _Req(ast.NodeTransformer):
            def visit_Attribute(self, n: ast.Attribute):
                self.generic_visit(n)
                if isinstance(n.value, ast.Name) and n.value.id == "request" and n.attr == "param":
                    return ast.copy_location(ast.Name(id="__param", ctx=ast.Load()), n)
                return n

        fn = _Req().visit(fn)
        if fx.is_yield:
            fn.body = self._split_yield(fn.body, fn)
        _locate(fn, fx.node.lineno)
        return fn

    def _split_yield(self, body: list[ast.stmt], fn: ast.FunctionDef) -> list[ast.stmt]:
        """`pre; yield v; post` -> `pre; def __teardown(): post; return (v, __teardown)`."""
        pre: list[ast.stmt] = []
        post: list[ast.stmt] = []
        value: ast.expr | None = None
        found = False
        for i, st in enumerate(body):
            if found:
                post.append(st)
                continue
            y = self._stmt_yield(st)
            if y is not None:
                value = y.value
                found = True
                continue
            if isinstance(st, ast.Try) and any(
                isinstance(n, (ast.Yield, ast.YieldFrom)) for n in ast.walk(st)
            ):
                # `try: a; yield v; b  finally: fin` -> pre += a; post = [try: b finally: fin]
                inner_pre: list[ast.stmt] = []
                inner_post: list[ast.stmt] = []
                seen = False
                for s2 in st.body:
                    if seen:
                        inner_post.append(s2)
                        continue
                    y2 = self._stmt_yield(s2)
                    if y2 is not None:
                        value = y2.value
                        seen = True
                        continue
                    inner_pre.append(s2)
                if not seen:
                    raise ValueError(f"{fn.name}: yield nested too deeply inside try")
                pre.extend(inner_pre)
                if (st.finalbody or st.handlers) and inner_post:
                    t = ast.Try(
                        body=inner_post,
                        handlers=st.handlers,
                        orelse=st.orelse,
                        finalbody=st.finalbody,
                    )
                    post.append(ast.copy_location(t, st))
                else:
                    post.extend(inner_post)
                    post.extend(st.finalbody)
                found = True
                continue
            if any(
                isinstance(n, (ast.Yield, ast.YieldFrom)) for n in ast.walk(st)
            ) and not isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef)):
                raise ValueError(f"{fn.name}: yield inside a {type(st).__name__} is not supported")
            pre.append(st)
        if not found:
            return body
        if value is None:
            value = ast.Constant(value=None)
        td = ast.FunctionDef(
            name="__teardown",
            args=ast.arguments(
                posonlyargs=[],
                args=[],
                vararg=None,
                kwonlyargs=[],
                kw_defaults=[],
                kwarg=None,
                defaults=[],
            ),
            body=post or [ast.Pass()],
            decorator_list=[],
            returns=None,
            type_comment=None,
        )
        if hasattr(ast.FunctionDef, "type_params"):
            td.type_params = []
        ret = ast.Return(
            value=ast.Tuple(elts=[value, ast.Name(id="__teardown", ctx=ast.Load())], ctx=ast.Load())
        )
        return pre + [td, ret]

    @staticmethod
    def _stmt_yield(st: ast.stmt) -> ast.Yield | None:
        if isinstance(st, ast.Expr) and isinstance(st.value, ast.Yield):
            return st.value
        return None

    # -- tests --------------------------------------------------------------

    def _parametrize_cases(
        self, fn: ast.FunctionDef
    ) -> tuple[list[str], list[tuple[str, list[tuple[str, ast.expr]]]]]:
        """Stacked `@pytest.mark.parametrize` -> (param names, [(suffix, [(name, expr)..])..])."""
        decs = [
            d
            for d in fn.decorator_list
            if _dec_name(d) in ("pytest.mark.parametrize", "parametrize")
        ]
        if not decs:
            return [], []
        names_all: list[str] = []
        per_dec: list[list[tuple[str, list[tuple[str, ast.expr]]]]] = []
        for d in decs:
            assert isinstance(d, ast.Call)
            argnames = d.args[0]
            if isinstance(argnames, ast.Constant):
                names = [n.strip() for n in str(argnames.value).split(",") if n.strip()]
            else:
                names = [str(e.value) for e in argnames.elts]  # type: ignore[attr-defined]
            values = d.args[1]
            if not isinstance(values, (ast.List, ast.Tuple)):
                raise ValueError(f"{fn.name}: parametrize values must be a literal list")
            ids = _kw(d, "ids")
            ids_list = None
            if isinstance(ids, (ast.List, ast.Tuple)) and all(
                isinstance(e, ast.Constant) for e in ids.elts
            ):
                ids_list = [str(e.value) for e in ids.elts]
            cases: list[tuple[str, list[tuple[str, ast.expr]]]] = []
            for i, v in enumerate(values.elts):
                if len(names) == 1:
                    binds = [(names[0], v)]
                    sid = _const_id(v, names[0], i)
                else:
                    if not isinstance(v, (ast.Tuple, ast.List)) or len(v.elts) != len(names):
                        raise ValueError(f"{fn.name}: parametrize case {i} does not match {names}")
                    binds = list(zip(names, v.elts))
                    sid = "_".join(_const_id(e, n, i) for n, e in binds)
                if ids_list is not None and i < len(ids_list):
                    sid = re.sub(r"[^A-Za-z0-9_]+", "_", ids_list[i]).strip("_") or f"case{i}"
                cases.append((sid, binds))
            # pytest ids are unique per parameter set; make ours so too
            uniq: list[tuple[str, list[tuple[str, ast.expr]]]] = []
            counts: dict[str, int] = {}
            for sid, _ in cases:
                counts[sid] = counts.get(sid, 0) + 1
            seen_n: dict[str, int] = {}
            for sid, binds in cases:
                if counts[sid] > 1:
                    seen_n[sid] = seen_n.get(sid, 0)
                    sid2 = f"{sid}_{seen_n[sid]}"
                    seen_n[sid] += 1
                    sid = sid2
                if not (sid[0].isalpha() or sid[0] == "_"):
                    sid = f"p{sid}"
                uniq.append((sid, binds))
            names_all.extend(names)
            per_dec.append(uniq)
        combos: list[tuple[str, list[tuple[str, ast.expr]]]] = []
        for prod in itertools.product(*per_dec):
            suffix = "_".join(s for s, _ in prod)
            binds = [b for _, bs in prod for b in bs]
            combos.append((suffix, binds))
        fn.decorator_list = [d for d in fn.decorator_list if d not in decs]
        return names_all, combos

    def _fixture_closure(self, names: list[str], cls: str | None) -> list[Fixture | str]:
        """Fixtures *names* (and their dependencies, and the class's / module's
        autouse fixtures) in setup order; a builtin fixture is its name."""
        order: list[Fixture | str] = []
        seen: set[str] = set()

        def lookup(n: str) -> Fixture | None:
            if cls is not None and n in self.class_fixtures.get(cls, {}):
                return self.class_fixtures[cls][n]
            if n in self.fixtures:
                return self.fixtures[n]
            if n in self.conftest_fixtures:
                self.used_conftest.add(n)
                return self.conftest_fixtures[n]
            return None

        def visit(n: str) -> None:
            if n in seen:
                return
            fx = lookup(n)
            if fx is None:
                if n in BUILTIN_FIXTURES:
                    seen.add(n)
                    order.append(n)
                    return
                raise ValueError(f"unknown fixture {n!r}")
            for d in fx.deps:
                visit(d)
            seen.add(n)
            order.append(fx)

        autouse = [f.name for f in self.fixtures.values() if f.autouse]
        if cls is not None:
            autouse += [f.name for f in self.class_fixtures.get(cls, {}).values() if f.autouse]
        for n in autouse + names:
            visit(n)
        return order

    def _expand_test(self, fn: ast.FunctionDef, cls: str | None) -> list[ast.FunctionDef]:
        pnames, pcases = self._parametrize_cases(fn)
        fixture_names = [a.arg for a in fn.args.args if a.arg != "self" and a.arg not in pnames]
        order = self._fixture_closure(fixture_names, cls)
        # parametrized fixtures -> a case per value
        fx_cases: list[list[tuple[Fixture, str, ast.expr]]] = []
        for item in order:
            if isinstance(item, Fixture) and item.params is not None:
                cases = []
                for i, v in enumerate(item.params):
                    sid = (
                        item.ids[i]
                        if item.ids and i < len(item.ids)
                        else _const_id(v, item.name, i)
                    )
                    sid = re.sub(r"[^A-Za-z0-9_]+", "_", sid).strip("_") or f"{item.name}{i}"
                    cases.append((item, sid, v))
                fx_cases.append(cases)
        variants: list[tuple[str, list[tuple[str, ast.expr]], dict[str, ast.expr]]] = []
        for pc in pcases or [("", [])]:
            for fc in itertools.product(*fx_cases) if fx_cases else [()]:
                suffix_parts = [pc[0]] + [sid for _, sid, _ in fc]
                suffix = "_".join(s for s in suffix_parts if s)
                variants.append((suffix, pc[1], {f.name: v for f, _, v in fc}))
        out: list[ast.FunctionDef] = []
        for suffix, binds, fx_params in variants:
            f2 = copy.deepcopy(fn)
            if suffix:
                f2.name = f"{fn.name}__{suffix}"
            f2.args.args = [a for a in f2.args.args if a.arg == "self"] if cls is not None else []
            if cls is not None and not f2.args.args:
                f2.args.args = [ast.arg(arg="self", annotation=None)]
            prologue: list[ast.stmt] = []
            for name, expr in binds:
                prologue.append(
                    ast.Assign(
                        targets=[ast.Name(id=name, ctx=ast.Store())], value=copy.deepcopy(expr)
                    )
                )
            setup, has_td = self._fixture_setup(order, cls, fx_params)
            inlines = {
                it.name: it.inline
                for it in order
                if isinstance(it, Fixture) and it.inline is not None
            }
            if inlines:
                f2 = _InlineCalls(inlines).visit(f2)
            body = f2.body
            # keep the docstring first
            doc: list[ast.stmt] = []
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                doc = [body[0]]
                body = body[1:]
            if has_td:
                td_init = ast.Assign(
                    targets=[ast.Name(id="__teardowns", ctx=ast.Store())],
                    value=ast.List(elts=[], ctx=ast.Load()),
                )
                fin = ast.Expr(
                    value=ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id="pytest", ctx=ast.Load()),
                            attr="run_teardowns",
                            ctx=ast.Load(),
                        ),
                        args=[ast.Name(id="__teardowns", ctx=ast.Load())],
                        keywords=[],
                    )
                )
                tr = ast.Try(body=setup + body, handlers=[], orelse=[], finalbody=[fin])
                f2.body = doc + prologue + [td_init, tr]
            else:
                f2.body = doc + prologue + setup + body
            _locate(f2, fn.lineno)
            out.append(f2)
        return out

    def _fixture_setup(
        self, order: list[Fixture | str], cls: str | None, fx_params: dict[str, ast.expr]
    ) -> tuple[list[ast.stmt], bool]:
        stmts: list[ast.stmt] = []
        has_td = False
        for item in order:
            if isinstance(item, str):
                # builtin fixture: `tmp_path = pytest.tmp_path()`
                call = ast.Call(
                    func=ast.Attribute(
                        value=ast.Name(id="pytest", ctx=ast.Load()), attr=item, ctx=ast.Load()
                    ),
                    args=[],
                    keywords=[],
                )
                stmts.append(ast.Assign(targets=[ast.Name(id=item, ctx=ast.Store())], value=call))
                if item == "monkeypatch":
                    has_td = True
                    stmts.append(
                        ast.Expr(
                            value=ast.Call(
                                func=ast.Attribute(
                                    value=ast.Name(id="__teardowns", ctx=ast.Load()),
                                    attr="append",
                                    ctx=ast.Load(),
                                ),
                                # `lambda: monkeypatch.undo()` -- a bound method read
                                # as a value is a call on the typed Rust twin.
                                args=[
                                    ast.Lambda(
                                        args=ast.arguments(
                                            posonlyargs=[],
                                            args=[],
                                            kwonlyargs=[],
                                            kw_defaults=[],
                                            defaults=[],
                                        ),
                                        body=ast.Call(
                                            func=ast.Attribute(
                                                value=ast.Name(id="monkeypatch", ctx=ast.Load()),
                                                attr="undo",
                                                ctx=ast.Load(),
                                            ),
                                            args=[],
                                            keywords=[],
                                        ),
                                    )
                                ],
                                keywords=[],
                            )
                        )
                    )
                continue
            fx = item
            if fx.inline is not None:
                # a callable fixture: its calls are inlined in the body
                continue
            args: list[ast.expr] = []
            if fx.params is not None:
                args.append(copy.deepcopy(fx_params[fx.name]))
            args += [ast.Name(id=d, ctx=ast.Load()) for d in fx.deps]
            if fx.cls is not None:
                func: ast.expr = ast.Attribute(
                    value=ast.Name(id="self", ctx=ast.Load()), attr=f"_fx_{fx.name}", ctx=ast.Load()
                )
            else:
                func = ast.Name(id=f"_fx_{fx.name}", ctx=ast.Load())
            call = ast.Call(func=func, args=args, keywords=[])
            if fx.is_yield:
                has_td = True
                # one teardown name per fixture: a tuple assignment declares
                # every target at once in Rust (`let (a, __td_a) = ..`), so a
                # shared `__td` would leave the second fixture's name undeclared
                td = f"__td_{fx.name}"
                target = ast.Tuple(
                    elts=[ast.Name(id=fx.name, ctx=ast.Store()), ast.Name(id=td, ctx=ast.Store())],
                    ctx=ast.Store(),
                )
                stmts.append(ast.Assign(targets=[target], value=call))
                stmts.append(
                    ast.Expr(
                        value=ast.Call(
                            func=ast.Attribute(
                                value=ast.Name(id="__teardowns", ctx=ast.Load()),
                                attr="append",
                                ctx=ast.Load(),
                            ),
                            args=[ast.Name(id=td, ctx=ast.Load())],
                            keywords=[],
                        )
                    )
                )
            else:
                stmts.append(
                    ast.Assign(targets=[ast.Name(id=fx.name, ctx=ast.Store())], value=call)
                )
        return stmts, has_td

    # -- marks ----------------------------------------------------------------

    @staticmethod
    def _rewrite_marks(fn: ast.FunctionDef) -> None:
        new: list[ast.expr] = []
        for d in fn.decorator_list:
            n = _dec_name(d)
            if n == "pytest.mark.skipif" and isinstance(d, ast.Call) and d.args:
                reason = _kw(d, "reason") or (
                    d.args[1] if len(d.args) > 1 else ast.Constant(value="skipif")
                )
                new.append(
                    ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id="unittest", ctx=ast.Load()),
                            attr="skipIf",
                            ctx=ast.Load(),
                        ),
                        args=[d.args[0], reason],
                        keywords=[],
                    )
                )
            elif n == "pytest.mark.xfail":
                reason = _kw(d, "reason")
                text = f"xfail: {reason.value}" if isinstance(reason, ast.Constant) else "xfail"
                new.append(
                    ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id="unittest", ctx=ast.Load()),
                            attr="skip",
                            ctx=ast.Load(),
                        ),
                        args=[ast.Constant(value=text)],
                        keywords=[],
                    )
                )
            elif n.startswith("pytest.mark.") and n not in (
                "pytest.mark.skip",
                "pytest.mark.parametrize",
            ):
                # a custom marker (`suspected_bug`): no Rust meaning
                continue
            else:
                new.append(d)
        fn.decorator_list = new

    # -- driver ---------------------------------------------------------------

    def run(self) -> ast.Module:
        tree = self.tree
        # conftest fixtures and imports
        if self.conftest is not None:
            for st in self.conftest.body:
                if isinstance(st, (ast.Import, ast.ImportFrom)):
                    self.conftest_imports.append(st)
                elif isinstance(st, ast.FunctionDef):
                    fx = self._collect_fixture(st, None)
                    if fx is not None:
                        self.conftest_fixtures[fx.name] = fx
        # module fixtures
        for st in tree.body:
            if isinstance(st, ast.FunctionDef):
                fx = self._collect_fixture(st, None)
                if fx is not None:
                    self.fixtures[fx.name] = fx
            elif isinstance(st, ast.ClassDef):
                for m in st.body:
                    if isinstance(m, ast.FunctionDef):
                        fx = self._collect_fixture(m, st.name)
                        if fx is not None:
                            self.class_fixtures.setdefault(st.name, {})[fx.name] = fx
        new_body: list[ast.stmt] = []
        module_tests: list[ast.FunctionDef] = []
        last_import = 0
        for i, st in enumerate(tree.body):
            if isinstance(st, (ast.Import, ast.ImportFrom)):
                last_import = i + 1
        for i, st in enumerate(tree.body):
            if isinstance(st, ast.FunctionDef) and st.name in self.fixtures:
                if self.fixtures[st.name].inline is not None:
                    continue
                new_body.append(self._rewrite_fixture(self.fixtures[st.name]))
            elif isinstance(st, ast.FunctionDef) and st.name.startswith("test_"):
                self._rewrite_marks(st)
                module_tests.extend(self._expand_test(st, "TestModule"))
            elif isinstance(st, ast.ClassDef) and st.name.startswith("Test"):
                body: list[ast.stmt] = []
                for m in st.body:
                    if isinstance(m, ast.FunctionDef) and m.name in self.class_fixtures.get(
                        st.name, {}
                    ):
                        if self.class_fixtures[st.name][m.name].inline is not None:
                            continue
                        body.append(self._rewrite_fixture(self.class_fixtures[st.name][m.name]))
                    elif isinstance(m, ast.FunctionDef) and m.name.startswith("test_"):
                        self._rewrite_marks(m)
                        body.extend(self._expand_test(m, st.name))
                    else:
                        body.append(m)
                st.body = body
                if not st.bases:
                    st.bases = [
                        ast.Attribute(
                            value=ast.Name(id="unittest", ctx=ast.Load()),
                            attr="TestCase",
                            ctx=ast.Load(),
                        )
                    ]
                new_body.append(st)
            else:
                new_body.append(st)
            if i + 1 == last_import:
                # the conftest's imports and (used) fixtures go right after the module's imports
                new_body.append(_PLACEHOLDER)
        if module_tests:
            cls = ast.ClassDef(
                name="TestModule",
                bases=[
                    ast.Attribute(
                        value=ast.Name(id="unittest", ctx=ast.Load()),
                        attr="TestCase",
                        ctx=ast.Load(),
                    )
                ],
                keywords=[],
                body=module_tests,
                decorator_list=[],
            )
            if hasattr(ast.ClassDef, "type_params"):
                cls.type_params = []
            new_body.append(cls)
        # conftest inlining (fixtures the module reaches, with the conftest's imports)
        inlined: list[ast.stmt] = []
        if self.used_conftest:
            # transitive deps inside conftest
            todo = list(self.used_conftest)
            while todo:
                n = todo.pop()
                for d in self.conftest_fixtures[n].deps:
                    if d in self.conftest_fixtures and d not in self.used_conftest:
                        self.used_conftest.add(d)
                        todo.append(d)
            inlined.extend(copy.deepcopy(self.conftest_imports))
            for name in sorted(self.used_conftest):
                if self.conftest_fixtures[name].inline is not None:
                    continue
                inlined.append(self._rewrite_fixture(self.conftest_fixtures[name]))
            inlined.insert(
                0,
                ast.Expr(
                    value=ast.Constant(
                        value=f"py2rs: fixtures inlined from conftest.py: {', '.join(sorted(self.used_conftest))} [P32]"
                    )
                ),
            )
            # Located on the module's last import line: the emitter attaches
            # the source's comments by line number, and nothing of this
            # module's lies there.
            anchor = tree.body[last_import - 1].end_lineno if last_import else 1
            for st in inlined:
                for n in ast.walk(st):
                    n.lineno = n.end_lineno = anchor
                    n.col_offset = n.end_col_offset = 0
        final: list[ast.stmt] = []
        for st in new_body:
            if st is _PLACEHOLDER:
                final.extend(inlined)
            else:
                final.append(st)
        if _PLACEHOLDER not in new_body:
            final = inlined + final
        tree.body = final
        _locate(tree, 1)
        return tree


_PLACEHOLDER = ast.Pass()


def _locate(node: ast.AST, lineno: int) -> None:
    """Give every synthetic node (no location) a one-line location: the
    expression printer keeps a single-line call on one line."""
    for n in ast.walk(node):
        if not hasattr(n, "lineno") or getattr(n, "lineno", None) is None:
            n.lineno = n.end_lineno = lineno
            n.col_offset = n.end_col_offset = 0
    ast.fix_missing_locations(node)


def desugar(tree: ast.Module, src_path: str | None) -> tuple[ast.Module, bool]:
    """Rewrite a pytest-style module in place; `(tree, was_pytest)`."""
    if not PytestDesugar.is_pytest_module(tree):
        return tree, False
    conftest = None
    if src_path:
        cp = os.path.join(os.path.dirname(src_path), "conftest.py")
        if os.path.exists(cp):
            conftest = ast.parse(open(cp, encoding="utf-8").read(), filename=cp)
    d = PytestDesugar(tree, conftest, os.path.splitext(os.path.basename(src_path or "module"))[0])
    return d.run(), True
