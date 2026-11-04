//! Lower the Python-shaped AST to Tier 1 Rust tokens (the numbered rules in
//! `docs/PORT_RULES.md`).

use std::collections::{HashMap, HashSet};

use proc_macro2::{Ident, Literal, Span, TokenStream, TokenTree};

/// `Ident::new` that accepts a raw identifier's spelling (`r#box`): the
/// captured-variable names are collected as strings and may be raw.
fn ident_maybe_raw(n: &str, span: Span) -> Ident {
    match n.strip_prefix("r#") {
        Some(raw) => Ident::new_raw(raw, span),
        None => Ident::new(n, span),
    }
}
use quote::{quote, quote_spanned};

use super::ast::*;
use super::parse::{ident_is_camel, parse_expr, Cur};

/// What the enclosing function returns, for `return` lowering and the
/// implicit trailing `Ok(..)`.
/// How a `return` inside a `try` converts its value for the enclosing
/// function [P16c].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TryRet {
    /// `-> ()`: `return` carries `()`.
    Unit,
    /// `-> Arg` / a nested-`def` closure: `return e` carries `into_arg(e)`.
    Arg,
    /// `-> T` (a plain Rust type): `return e` carries `RetInto::ret_into(e)?`
    /// (an integer literal takes the type directly, see `ret`).
    Typed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RetKind {
    /// `-> ()` (implicit) -> `Result<()>`
    Unit,
    /// `-> Arg` -> `Result<Arg>`; `return x` converts with `into_arg`.
    Arg,
    /// `-> T` -> `Result<T>`; trailing expression becomes `Ok(expr)`.
    Other,
    /// User already wrote `-> Result<T>`; nothing appended.
    UserResult,
}

/// How a parameter of a local Python-defined function is declared: a
/// dynamic `Arg` (the lowered value converts with `into_arg`), a plain Rust
/// type (`n: i64`: the lowered expression passes as is) or `String`
/// (`key: str`: any `Display` value -- a literal, an f-string -- converts).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawParam {
    Dyn,
    Plain,
    Str,
}

#[derive(Debug, Clone)]
pub struct Ctx {
    /// Method names of the enclosing `impl` block (plus `TestCase` names in unittest mode).
    pub impl_methods: HashSet<String>,
    /// `#[class_attr]` value getters of the enclosing `impl`: `self.x` is
    /// the value, `self.x(..)` calls the value (`factory = Cls`).
    pub class_attrs: HashSet<String>,
    /// Local functions / methods that take `Args` (declared with `#[pysig]`).
    pub pysig_methods: HashSet<String>,
    /// Functions defined in (or declared to) this `python!` block: called
    /// directly. Any other lowercase identifier called like `f(x)` is a
    /// Python callable value and goes through `builtins::call`.
    pub local_fns: HashSet<String>,
    /// Per local function: which positional parameters are declared with
    /// a plain Rust type (`n: i64`, `s: String`) rather than `Arg`. Those
    /// receive the lowered expression as is (no `into_arg`).
    pub raw_params: HashMap<String, Vec<RawParam>>,
    /// `self.x` reads/writes go through `__getattr__`/`__setattr__`.
    pub dynamic_self: bool,
    pub ret: RetKind,
    /// Inside a Rust closure: `return` must not convert (closure's own contract).
    pub in_closure: bool,
    /// Inside a nested `async def`: `try` bodies lower to awaited async
    /// blocks (a closure could not `await`).
    pub in_async: bool,
    /// Names bound in the current function (parameters, `let`, `for`, `with`,
    /// `except .. as`): a bare `json`/`uuid`/`time` that is *not* bound here
    /// is the stdlib shim (`::laila::python::stdlib::json`), exactly as a
    /// Python name resolves to the module unless the function rebinds it.
    pub bound: HashSet<String>,
    /// Locals whose `let` initialiser was a dynamic scalar (`None`, `0`,
    /// `True`): they hold an `Arg`, so later `name = value` converts.
    pub dynamic_vars: HashSet<String>,
    /// The subset of `dynamic_vars` that a nested `def` reads while the
    /// enclosing body rebinds them: Python closure cells, lowered to a
    /// shared `Cell` (`stop.get()` / `stop.set(..)`) so the nested
    /// function observes the rebinding.
    pub cell_vars: HashSet<String>,
    /// Locals bound to attribute-dynamic objects (`d = DotMap(..)`,
    /// `a = laila.args`): `d.x` lowers to `d.__getattr__("x")` and chains.
    pub dyn_objs: HashSet<String>,
    /// `#![dyn_classes(..)]`: imported Rust classes whose instances are
    /// *fully* dynamic -- attribute reads and method calls both go through
    /// the object protocol (`x.verb(..)` -> `x.__laila_call_method__(..)`).
    pub alias_classes: HashSet<String>,
    /// Locals bound to an `alias_classes` instance.
    pub alias_objs: HashSet<String>,
    /// Locals that are certainly `Arg`s (tuple-unpacked), see
    /// [`scan_arg_locals`]: method calls on them dispatch dynamically.
    pub arg_locals: HashSet<String>,
    /// Nested `def`s lowered to plain Rust closures (only ever called
    /// directly): `name(a, b)` calls the closure with owned `Arg`s.
    pub closure_fns: HashSet<String>,
    /// Inside a `try` body / `except` handler: `return` cannot leave the
    /// enclosing function from the `Result` closure the block runs in.
    pub in_try: bool,
    /// Inside a `try` whose blocks carry `return` out of the enclosing
    /// function [P16c]: the closures yield `Option<value>` and `return e`
    /// is `Ok(Some(e))` with *e* converted the way the enclosing function
    /// (or nested-`def` closure) converts its return value.
    pub try_ret: Option<TryRet>,
    /// Inside a `try` body / handler that is inside a loop and carries a
    /// `break` / `continue` of that loop [P16d]: the closures yield
    /// `Option<bool>` (`Some(true)`: break, `Some(false)`: continue).
    pub try_loopctl: bool,
    /// Classes whose instances are attribute-dynamic (`py_model!` classes
    /// declared in the block, plus `DYN_OBJ_CLASSES`).
    pub dyn_classes: HashSet<String>,
    /// Classes defined in the block (every inherent `impl X`): a method of
    /// theirs called on a fresh instance (`Cls(..).build()`) is the
    /// Python-defined method with positional arguments.
    pub local_classes: HashSet<String>,
    /// Method names of every inherent `impl` of the block.
    pub local_methods: HashSet<String>,
    /// Module constants of the block (`static NAME: LazyLock<Arg>`, rule
    /// P11): a bare read of the name is the value, `(*NAME).clone()`.
    pub module_consts: HashSet<String>,
    /// Locals bound to `Cls.__new__(Cls)`: an unbound `Cls.method(local, ..)`
    /// call is the bound `local.method(..)`.
    pub bare_instances: HashSet<String>,
    /// Functions / methods of the block declared `-> Arg`: a local bound
    /// to `f(..)` / `self.f(..)` is an `Arg` local.
    pub arg_fns: HashSet<String>,
}

impl Ctx {
    /// A per-expansion unique id for `with` guards (nested `with` blocks
    /// must not shadow each other's guard before dropping it).
    pub fn next_with_id(&self) -> usize {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        NEXT.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for Ctx {
    fn default() -> Self {
        Ctx {
            impl_methods: HashSet::new(),
            class_attrs: HashSet::new(),
            pysig_methods: HashSet::new(),
            local_fns: HashSet::new(),
            raw_params: HashMap::new(),
            dynamic_self: false,
            ret: RetKind::Unit,
            in_closure: false,
            in_async: false,
            bound: HashSet::new(),
            dynamic_vars: HashSet::new(),
            cell_vars: HashSet::new(),
            dyn_objs: HashSet::new(),
            alias_classes: HashSet::new(),
            alias_objs: HashSet::new(),
            arg_locals: HashSet::new(),
            arg_fns: HashSet::new(),
            closure_fns: HashSet::new(),
            in_try: false,
            try_loopctl: false,
            try_ret: None,
            dyn_classes: HashSet::new(),
            local_classes: HashSet::new(),
            local_methods: HashSet::new(),
            module_consts: HashSet::new(),
            bare_instances: HashSet::new(),
        }
    }
}

/// `fn name(..)` of a nested item's tokens (after any `#[..]` attributes).
pub fn item_fn_name(ts: &TokenStream) -> Option<Ident> {
    let mut it = ts.clone().into_iter();
    while let Some(t) = it.next() {
        if matches!(&t, TokenTree::Ident(i) if i == "fn") {
            if let Some(TokenTree::Ident(n)) = it.next() {
                return Some(n);
            }
            return None;
        }
    }
    None
}

/// A nested `fn` item split into its parts.
struct NestedFn {
    attrs: Vec<TokenTree>,
    name: Ident,
    params: proc_macro2::Group,
    body: proc_macro2::Group,
    /// `async fn` (a nested `async def`).
    is_async: bool,
}

/// Split a nested `fn` item into (attributes, name, parameter group, body).
fn split_nested_fn(ts: &TokenStream) -> Option<NestedFn> {
    let tts: Vec<TokenTree> = ts.clone().into_iter().collect();
    let mut i = 0;
    let mut attrs = Vec::new();
    let mut is_async = false;
    while i < tts.len() {
        match &tts[i] {
            TokenTree::Punct(p) if p.as_char() == '#' => {
                attrs.push(tts[i].clone());
                if let Some(g) = tts.get(i + 1) {
                    attrs.push(g.clone());
                }
                i += 2;
            }
            TokenTree::Ident(k) if k == "fn" => break,
            TokenTree::Ident(k) => {
                // `pub`, `async`
                if k == "async" {
                    is_async = true;
                }
                i += 1;
            }
            _ => return None,
        }
    }
    if !matches!(tts.get(i), Some(TokenTree::Ident(k)) if k == "fn") {
        return None;
    }
    let name = match tts.get(i + 1) {
        Some(TokenTree::Ident(n)) => n.clone(),
        _ => return None,
    };
    let params = match tts.get(i + 2) {
        Some(TokenTree::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Parenthesis => {
            g.clone()
        }
        _ => return None,
    };
    let body = match tts.last() {
        Some(TokenTree::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Brace => g.clone(),
        _ => return None,
    };
    Some(NestedFn {
        attrs,
        name,
        params,
        body,
        is_async,
    })
}

/// Parameter names of a `(a: T, b: U)` group, in order.
fn group_param_names(g: &proc_macro2::Group) -> Vec<Ident> {
    let mut out = Vec::new();
    let mut expect_name = true;
    let mut depth = 0usize;
    for t in g.stream() {
        match t {
            TokenTree::Punct(p) if p.as_char() == ',' && depth == 0 => expect_name = true,
            TokenTree::Punct(p) if p.as_char() == ':' && depth == 0 => expect_name = false,
            TokenTree::Punct(p) if p.as_char() == '<' => depth += 1,
            TokenTree::Punct(p) if p.as_char() == '>' => depth = depth.saturating_sub(1),
            TokenTree::Ident(i) if expect_name && depth == 0 && i != "mut" && i != "self" => {
                out.push(i);
                expect_name = false;
            }
            _ => {}
        }
    }
    out
}

/// Python stdlib modules shipped as value shims in `laila::python::stdlib`.
pub const STDLIB_MODULES: &[&str] = &[
    "np",
    "sqlite3",
    "h5py",
    "shutil",
    "urllib",
    "json",
    "uuid",
    "time",
    "os",
    "sys",
    "threading",
    "subprocess",
    "asyncio",
    "tempfile",
    "math",
    "random",
    "base64",
    "hashlib",
    "copy",
    "gc",
    "builtins",
    "pathlib",
    "shutil",
    "socket",
    "struct",
    "logging",
    "re",
    "datetime",
    "functools",
    "itertools",
    "collections",
    "np",
    "numpy",
    "pytest",
    "unittest",
    "zlib",
    "pickle",
    "io",
    "textwrap",
    "statistics",
    "msgpack",
    "binascii",
    "ast",
    "cryptography",
    "dotmap",
    "tomllib",
    "xml",
    "inspect",
    // `from datetime import UTC, datetime, timedelta, timezone` -- class objects
    "timedelta",
    "timezone",
    "UTC",
];

/// Identifiers bound by a `let` header (`let mut x`, `let (a, b): T`).
fn let_header_names(header: &TokenStream, out: &mut HashSet<String>) {
    let mut seen_let = false;
    let mut after_colon = false;
    for t in header.clone() {
        match t {
            TokenTree::Ident(i) if i == "let" => seen_let = true,
            TokenTree::Ident(i) if i == "mut" || i == "ref" => {}
            TokenTree::Punct(p) if p.as_char() == ':' => after_colon = true,
            TokenTree::Ident(i) if seen_let && !after_colon => {
                out.insert(i.to_string());
            }
            TokenTree::Group(g) if seen_let && !after_colon => pattern_names(&g.stream(), out),
            _ => {}
        }
    }
}

fn pattern_names(pat: &TokenStream, out: &mut HashSet<String>) {
    for t in pat.clone() {
        match t {
            TokenTree::Ident(i) if i != "mut" && i != "ref" && i != "_" => {
                out.insert(i.to_string());
            }
            TokenTree::Group(g) => pattern_names(&g.stream(), out),
            _ => {}
        }
    }
}

/// Locals that are certainly `Arg`s: the names of a tuple-unpacking `let`
/// (`c, comm = self._carrier(1)` -- `unpackN` yields `Arg`s) and of a
/// tuple `for` pattern (`for k, v in ..`), anywhere in the body.
pub fn scan_arg_locals(block: &Block, out: &mut HashSet<String>, arg_fns: &HashSet<String>) {
    scan_arg_locals_in(block, out, false, arg_fns)
}

/// [`scan_arg_locals`] where `self.<attr>` is known to be dynamic (a
/// `py_class!` method): values reached through `self` are `Arg`s too.
pub fn scan_arg_locals_dyn(block: &Block, out: &mut HashSet<String>, arg_fns: &HashSet<String>) {
    scan_arg_locals_in(block, out, true, arg_fns)
}

fn scan_arg_locals_in(
    block: &Block,
    out: &mut HashSet<String>,
    dyn_self: bool,
    arg_fns: &HashSet<String>,
) {
    let is_arg = |e: &Expr, out: &HashSet<String>| expr_is_arg_chain(e, out, dyn_self, arg_fns);
    for st in &block.stmts {
        match st {
            Stmt::Let {
                header,
                value: Some(_),
            } if let_tuple_arity(header).is_some() => {
                let mut names = HashSet::new();
                let_header_names(header, &mut names);
                out.extend(names);
            }
            // `got: Arg = []`: an explicitly `Arg`-typed local.
            Stmt::Let { header, .. } if let_is_arg_typed(header) => {
                let mut names = HashSet::new();
                let_header_names(header, &mut names);
                if names.len() == 1 {
                    out.extend(names);
                }
            }
            // `x = <arg local>` / `x = <arg local>.attr` / `x = <arg local>[k]`:
            // a value reached through an `Arg` is an `Arg`.
            Stmt::Let {
                header,
                value: Some(v),
            } if is_plain_let(header) && is_arg(v, out) => {
                let mut names = HashSet::new();
                let_header_names(header, &mut names);
                if names.len() == 1 {
                    out.extend(names);
                }
            }
            // `proxy = self.a.central.communication.peers[gid]` /
            // `ch = tp.ca.peers[bid]["video"]`: an item of anything
            // (`__getitem__` always yields an `Arg`).
            Stmt::Let {
                header,
                value: Some(Expr::Index { .. }),
            } if is_plain_let(header) => {
                let mut names = HashSet::new();
                let_header_names(header, &mut names);
                if names.len() == 1 {
                    out.extend(names);
                }
            }
            Stmt::For {
                pat, body, iter, ..
            } => {
                let tts: Vec<TokenTree> = pat.clone().into_iter().collect();
                if matches!(tts.as_slice(), [TokenTree::Group(g)] if g.delimiter() == proc_macro2::Delimiter::Parenthesis)
                {
                    pattern_names(pat, out);
                } else if is_arg(iter, out) {
                    // `for e in got` / `for au in self.aus`: the items of an
                    // `Arg` iterable are `Arg`s.
                    pattern_names(pat, out);
                }
                scan_arg_locals_in(body, out, dyn_self, arg_fns);
            }
            Stmt::With {
                cm, target, body, ..
            } => {
                // `with pair as tp`: `__enter__` of an `Arg` yields an `Arg`.
                if let Some(t) = target {
                    if is_arg(cm, out) {
                        let mut names = HashSet::new();
                        pattern_names(t, &mut names);
                        if names.len() == 1 {
                            out.extend(names);
                        }
                    }
                }
                scan_arg_locals_in(body, out, dyn_self, arg_fns)
            }
            Stmt::While { body, .. } | Stmt::Loop { body } => {
                scan_arg_locals_in(body, out, dyn_self, arg_fns)
            }
            Stmt::Try {
                body,
                handlers,
                orelse,
                finalbody,
                ..
            } => {
                scan_arg_locals_in(body, out, dyn_self, arg_fns);
                for h in handlers {
                    scan_arg_locals_in(&h.body, out, dyn_self, arg_fns);
                }
                if let Some(b) = orelse {
                    scan_arg_locals_in(b, out, dyn_self, arg_fns);
                }
                if let Some(b) = finalbody {
                    scan_arg_locals_in(b, out, dyn_self, arg_fns);
                }
            }
            Stmt::Expr {
                expr: Expr::If {
                    then, otherwise, ..
                },
                ..
            } => {
                scan_arg_locals_in(then, out, dyn_self, arg_fns);
                if let Some(Expr::Block(b)) = otherwise.as_deref() {
                    scan_arg_locals_in(b, out, dyn_self, arg_fns);
                }
            }
            _ => {}
        }
    }
}

/// `let x: Arg = ..`: a `let` header whose type annotation is exactly `Arg`.
fn let_is_arg_typed(header: &TokenStream) -> bool {
    let tts: Vec<TokenTree> = header.clone().into_iter().collect();
    let mut i = 0;
    while i < tts.len() {
        if let TokenTree::Punct(p) = &tts[i] {
            if p.as_char() == ':' {
                return matches!(tts.get(i + 1), Some(TokenTree::Ident(t)) if t == "Arg")
                    && tts.len() == i + 2;
            }
        }
        i += 1;
    }
    false
}

/// Is `e` a value that is certainly an `Arg`: a known `Arg` local, or an
/// attribute / item chain rooted at one or at `self.<attr>`?
fn expr_is_arg_chain(
    e: &Expr,
    arg_locals: &HashSet<String>,
    dyn_self: bool,
    arg_fns: &HashSet<String>,
) -> bool {
    match e {
        Expr::Ident(i) => arg_locals.contains(&i.to_string()),
        // `laila.args.environment`: every attribute of the process-wide
        // `laila.args` mapping is a dynamic value.
        Expr::Attr { base, .. } if is_laila_args(base) => true,
        Expr::Attr { base, .. } => {
            (dyn_self && is_self(base)) || expr_is_arg_chain(base, arg_locals, dyn_self, arg_fns)
        }
        // `__getitem__` always yields an `Arg`
        Expr::Index { .. } => true,
        // `tmp_path / "a.json"`, `a // b`, `a % b`, `a ** b`: these
        // operators always go through the builtins and yield an `Arg`.
        Expr::Binary {
            op: BinOp::Div | BinOp::FloorDiv | BinOp::Mod | BinOp::Pow,
            ..
        } => true,
        // `type(x)` / `next(it)` / `sorted(xs)` / `getattr(o, n)`: builtins
        // whose Rust twin returns an `Arg`; `_chan()`: a function of the
        // block declared `-> Arg`.
        Expr::Call { callee, .. } => matches!(&**callee, Expr::Ident(i)
            if ARG_RETURNING_BUILTINS.contains(&i.to_string().trim_start_matches("r#"))
                || arg_fns.contains(&i.to_string())),
        // `self._frames(..)`: a method of the block declared `-> Arg`.
        Expr::MethodCall { recv, name, .. } => is_self(recv) && arg_fns.contains(&name.to_string()),
        _ => false,
    }
}

/// Builtins whose Rust twin returns an `Arg` (a local bound to one is an
/// `Arg` local: attribute reads on it go through `__getattr__`).
const ARG_RETURNING_BUILTINS: &[&str] = &[
    "type",
    "next",
    "sorted",
    "reversed",
    "getattr",
    "list",
    "tuple",
    "dict",
    "set",
    "zip",
    "enumerate",
    "iter",
    "min",
    "max",
    "abs",
    "bytearray",
];

/// Collect every name a function body binds, plus the dynamic-scalar locals.
/// Python closure cells among a body's dynamic-scalar locals: the names
/// that are rebound (`x = ..` / `x += ..`) somewhere in the body *and*
/// read inside a nested `def` item (whose tokens mention them).
pub fn scan_cell_vars(block: &Block, dynamic: &HashSet<String>) -> HashSet<String> {
    let mut reassigned = HashSet::new();
    let mut captured = HashSet::new();
    scan_cells_block(block, &mut reassigned, &mut captured);
    dynamic
        .iter()
        .filter(|n| reassigned.contains(*n) && captured.contains(*n))
        .cloned()
        .collect()
}

fn scan_cells_block(
    block: &Block,
    reassigned: &mut HashSet<String>,
    captured: &mut HashSet<String>,
) {
    for st in &block.stmts {
        match st {
            Stmt::Assign { target, .. } | Stmt::AugAssign { target, .. } => {
                if let Expr::Ident(i) = target {
                    reassigned.insert(i.to_string());
                }
            }
            Stmt::Item(t) => {
                let mut used = HashSet::new();
                super::ast::idents_in_block(
                    &Block {
                        stmts: vec![Stmt::Item(t.clone())],
                        span: None,
                    },
                    &mut used,
                );
                captured.extend(used);
            }
            Stmt::With { body, .. }
            | Stmt::For { body, .. }
            | Stmt::While { body, .. }
            | Stmt::Loop { body } => scan_cells_block(body, reassigned, captured),
            Stmt::Try {
                body,
                handlers,
                orelse,
                finalbody,
                ..
            } => {
                scan_cells_block(body, reassigned, captured);
                for h in handlers {
                    scan_cells_block(&h.body, reassigned, captured);
                }
                if let Some(b) = orelse {
                    scan_cells_block(b, reassigned, captured);
                }
                if let Some(b) = finalbody {
                    scan_cells_block(b, reassigned, captured);
                }
            }
            Stmt::Expr {
                expr: Expr::If {
                    then, otherwise, ..
                },
                ..
            } => {
                scan_cells_block(then, reassigned, captured);
                if let Some(o) = otherwise {
                    if let Expr::Block(b) = &**o {
                        scan_cells_block(b, reassigned, captured);
                    } else if let Expr::If { .. } = &**o {
                        scan_cells_block(
                            &Block {
                                stmts: vec![Stmt::Expr {
                                    expr: (**o).clone(),
                                    semi: false,
                                }],
                                span: None,
                            },
                            reassigned,
                            captured,
                        );
                    }
                }
            }
            _ => {}
        }
    }
}

pub fn scan_bindings(block: &Block, bound: &mut HashSet<String>, dynamic: &mut HashSet<String>) {
    for st in &block.stmts {
        match st {
            Stmt::Let { header, value } => {
                let mut names = HashSet::new();
                let_header_names(header, &mut names);
                if let Some(v) = value {
                    if names.len() == 1 && is_dynamic_scalar_literal(v) && is_plain_let(header) {
                        dynamic.extend(names.iter().cloned());
                    }
                    scan_expr(v, bound, dynamic);
                }
                bound.extend(names);
            }
            Stmt::Assign { target, value } => {
                if let Expr::Ident(i) = target {
                    bound.insert(i.to_string());
                }
                scan_expr(value, bound, dynamic);
            }
            Stmt::AugAssign { value, .. } => scan_expr(value, bound, dynamic),
            Stmt::Expr { expr, .. } => scan_expr(expr, bound, dynamic),
            Stmt::Return(Some(e), _) | Stmt::Raise(Some(e), _) | Stmt::Del(e, _) => {
                scan_expr(e, bound, dynamic)
            }
            Stmt::With {
                cm, target, body, ..
            } => {
                scan_expr(cm, bound, dynamic);
                if let Some(t) = target {
                    pattern_names(t, bound);
                }
                scan_bindings(body, bound, dynamic);
            }
            Stmt::For {
                pat, iter, body, ..
            } => {
                pattern_names(pat, bound);
                scan_expr(iter, bound, dynamic);
                scan_bindings(body, bound, dynamic);
            }
            Stmt::While { cond, body } => {
                scan_expr(cond, bound, dynamic);
                scan_bindings(body, bound, dynamic);
            }
            Stmt::Loop { body } => scan_bindings(body, bound, dynamic),
            Stmt::Try {
                body,
                handlers,
                orelse,
                finalbody,
                ..
            } => {
                scan_bindings(body, bound, dynamic);
                for h in handlers {
                    if let Some(n) = &h.name {
                        bound.insert(n.to_string());
                    }
                    scan_bindings(&h.body, bound, dynamic);
                }
                if let Some(b) = orelse {
                    scan_bindings(b, bound, dynamic);
                }
                if let Some(b) = finalbody {
                    scan_bindings(b, bound, dynamic);
                }
            }
            // A nested `def` binds its name like a local.
            Stmt::Item(ts) => {
                if let Some(n) = item_fn_name(ts) {
                    bound.insert(n.to_string());
                }
            }
            _ => {}
        }
    }
}

/// Python classes whose instances are attribute-dynamic (`obj.anything`).
pub const DYN_OBJ_CLASSES: &[&str] = &["DotMap", "AtomicDotMap", "Mock", "MagicMock"];

/// CamelCase names that are *values*, not classes.
pub const CAMEL_VALUES: &[&str] = &["NotImplemented", "Ellipsis", "True", "False", "None"];

fn is_dyn_obj_value(e: &Expr, dyn_classes: &HashSet<String>) -> bool {
    match e {
        Expr::Call { callee, .. } => {
            matches!(&**callee, Expr::Ident(i) if DYN_OBJ_CLASSES.contains(&i.to_string().as_str()) || dyn_classes.contains(&i.to_string()))
        }
        // `laila.args`
        Expr::Attr { base, name } => {
            name == "args" && matches!(&**base, Expr::Ident(i) if i == "laila")
        }
        _ => false,
    }
}

/// `py_model!` classes declared as statements inside a body.
pub fn scan_model_items(block: &Block, out: &mut HashSet<String>) {
    for st in &block.stmts {
        match st {
            Stmt::Item(ts) => {
                let tts: Vec<TokenTree> = ts.clone().into_iter().collect();
                if super::model::is_py_model(&tts) {
                    if let Some(n) = super::model::py_model_name(&tts) {
                        out.insert(n);
                    }
                }
                // `py_class!(Obj);` in a body: instances carry dynamically
                // assigned attributes like a module-level class's.
                if let Some(n) = super::py_class_name(&tts) {
                    out.insert(n);
                }
            }
            Stmt::With { body, .. }
            | Stmt::For { body, .. }
            | Stmt::While { body, .. }
            | Stmt::Loop { body } => scan_model_items(body, out),
            Stmt::Try {
                body,
                handlers,
                orelse,
                finalbody,
                ..
            } => {
                scan_model_items(body, out);
                for h in handlers {
                    scan_model_items(&h.body, out);
                }
                if let Some(b) = orelse {
                    scan_model_items(b, out);
                }
                if let Some(b) = finalbody {
                    scan_model_items(b, out);
                }
            }
            _ => {}
        }
    }
}

/// Is `e` a `patch.object(..)` / `patch(..)` context manager expression?
fn is_patch_cm(e: &Expr) -> bool {
    match e {
        Expr::MethodCall { recv, name, .. } => {
            name == "object" && matches!(&**recv, Expr::Ident(i) if i == "patch")
        }
        Expr::Call { callee, .. } => matches!(&**callee, Expr::Ident(i) if i == "patch"),
        _ => false,
    }
}

/// Is `e` a `Cls.__new__(Cls)` call (a bare instance whose `__init__` did not run)?
fn is_bare_instance_value(e: &Expr) -> bool {
    matches!(e, Expr::MethodCall { name, .. } if name == "__new__")
}

/// Locals bound (anywhere in the body) to `Cls.__new__(Cls)`.
pub fn scan_bare_instances(block: &Block, out: &mut HashSet<String>) {
    for st in &block.stmts {
        match st {
            Stmt::Let {
                header,
                value: Some(v),
            } if is_bare_instance_value(v) && is_plain_let(header) => {
                let mut names = HashSet::new();
                let_header_names(header, &mut names);
                if names.len() == 1 {
                    out.extend(names);
                }
            }
            Stmt::Assign {
                target: Expr::Ident(i),
                value,
            } if is_bare_instance_value(value) => {
                out.insert(i.to_string());
            }
            Stmt::With { body, .. }
            | Stmt::For { body, .. }
            | Stmt::While { body, .. }
            | Stmt::Loop { body } => scan_bare_instances(body, out),
            Stmt::Expr {
                expr: Expr::If {
                    then, otherwise, ..
                },
                ..
            } => {
                scan_bare_instances(then, out);
                if let Some(Expr::Block(b)) = otherwise.as_deref() {
                    scan_bare_instances(b, out);
                }
            }
            _ => {}
        }
    }
}

/// Locals bound (anywhere in the body) to a dynamic object value.
pub fn scan_dyn_objs(block: &Block, out: &mut HashSet<String>, dyn_classes: &HashSet<String>) {
    for st in &block.stmts {
        match st {
            Stmt::Let {
                header,
                value: Some(v),
            } => {
                if is_dyn_obj_value(v, dyn_classes) && is_plain_let(header) {
                    let mut names = HashSet::new();
                    let_header_names(header, &mut names);
                    if names.len() == 1 {
                        out.extend(names);
                    }
                }
            }
            Stmt::Assign {
                target: Expr::Ident(i),
                value,
            } if is_dyn_obj_value(value, dyn_classes) => {
                out.insert(i.to_string());
            }
            Stmt::With { body, .. }
            | Stmt::For { body, .. }
            | Stmt::While { body, .. }
            | Stmt::Loop { body } => scan_dyn_objs(body, out, dyn_classes),
            Stmt::Try {
                body,
                handlers,
                orelse,
                finalbody,
                ..
            } => {
                scan_dyn_objs(body, out, dyn_classes);
                for h in handlers {
                    scan_dyn_objs(&h.body, out, dyn_classes);
                }
                if let Some(b) = orelse {
                    scan_dyn_objs(b, out, dyn_classes);
                }
                if let Some(b) = finalbody {
                    scan_dyn_objs(b, out, dyn_classes);
                }
            }
            Stmt::Expr {
                expr: Expr::If {
                    then, otherwise, ..
                },
                ..
            } => {
                scan_dyn_objs(then, out, dyn_classes);
                if let Some(Expr::Block(b)) = otherwise.as_deref() {
                    scan_dyn_objs(b, out, dyn_classes);
                }
            }
            _ => {}
        }
    }
}

fn scan_expr(e: &Expr, bound: &mut HashSet<String>, dynamic: &mut HashSet<String>) {
    match e {
        Expr::Block(b) => scan_bindings(b, bound, dynamic),
        Expr::If {
            cond,
            then,
            otherwise,
        } => {
            scan_expr(cond, bound, dynamic);
            scan_bindings(then, bound, dynamic);
            if let Some(o) = otherwise {
                scan_expr(o, bound, dynamic);
            }
        }
        Expr::Match { scrutinee, arms } => {
            scan_expr(scrutinee, bound, dynamic);
            for (_, body) in arms {
                scan_expr(body, bound, dynamic);
            }
        }
        Expr::Closure { body, .. } => scan_expr(body, bound, dynamic),
        _ => {}
    }
}

/// Python builtins and laila free functions callable by bare name.
pub const BUILTIN_FNS: &[&str] = &[
    "print",
    "len",
    "str",
    "repr",
    "int",
    "float",
    "bool",
    "list",
    "dict",
    "tuple",
    "set",
    "sorted",
    "all",
    "any",
    "range",
    "isinstance",
    "hasattr",
    "getattr",
    "setattr",
    "delattr",
    "callable",
    "slice",
    "truthy",
    "pow",
    "floordiv",
    "divmod",
    "abs",
    "min",
    "max",
    "sum",
    "enumerate",
    "zip",
    "reversed",
    "iter",
    "next",
    "id",
    "hash",
    "type",
    "bytes",
    "bytearray",
    "memoryview",
    "complex",
    "round",
    "ord",
    "chr",
    "format",
    "open",
    "input",
    "arg",
    "py_iter",
    "contains",
    "is_",
    "is_none",
    "unpack2",
    "unpack3",
    "exit",
    "dir",
];

include!(concat!(env!("OUT_DIR"), "/args_methods.rs"));

/// The arity of `Arg`'s method `name` (`-1`: an `Args` bag), if any.
pub fn arg_method_arity(name: &str) -> Option<i32> {
    ARG_METHOD_ARITY
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, k)| *k)
}

/// Does a laila API call named `name` take Python-style `Args`?
/// Union of the build-time table derived from laila-core's `#[pysig]` /
/// `impl Into<Args>` signatures and the hand list of builtins below.
pub fn takes_args_name(name: &str) -> bool {
    ARGS_METHODS_GENERATED.contains(&name) || ARGS_METHODS.contains(&name)
}

/// Builtins and facade verbs that take `Args` (names that are not, or not
/// yet, derivable from a `#[pysig]` in laila-core).
pub const ARGS_METHODS: &[&str] = &[
    "new",
    // pydantic `model_dump(..)` on a dynamic model instance.
    "model_dump",
    // `Cls.__new__(Cls)`: the class object is the argument.
    "__new__",
    // cryptography.fernet (Python-shaped on both the typed `Fernet` and `Arg`)
    "encrypt",
    "decrypt",
    "encrypt_at_time",
    "decrypt_at_time",
    "extract_timestamp",
    "rotate",
    // numpy
    "reshape",
    "memorize",
    "remember",
    "forget",
    "build",
    "wait",
    "constant",
    "variable",
    "contingent",
    "extend",
    "keys",
    "submit",
    "terminate",
    "request",
    "relay",
    "evolve",
    "enable_logging",
    "read_args",
    "print",
    "range",
    "dict",
    "Thread",
    "Barrier",
    "run",
    "memorize_async",
    "remember_async",
    "forget_async",
    "resolve_global_id",
    "duplicate",
    "guarantee",
    "Entry",
    "Manifest",
    "atomic",
    "empty",
    "close",
    "sync",
    "data",
    "setUp",
    "tearDown",
];

/// Builtin *classes* that Python code also uses as values
/// (`default_factory=dict`, `map(str, xs)`).
pub const BUILTIN_TYPE_NAMES: &[&str] = &[
    "dict",
    "list",
    "set",
    "frozenset",
    "tuple",
    "str",
    "int",
    "float",
    "bool",
    "bytes",
    "bytearray",
];

/// Attribute names that are also Python container methods (`index`,
/// `keys`, ...): `Arg` carries the *method*, so an attribute read of the
/// same name is lowered to `__getattr__` (rule P22).
/// Attributes of private helper objects reached through the dynamic layer
/// (`m._sql_state.db_path`): read through `__getattr__` on every receiver.
pub const DYN_ATTRS: &[&str] = &[
    // Manifest._sql_state (`_SqlState`)
    "db_path",
    "is_persistent",
    "table_name",
    "indexed",
    "row_keys",
    "stale",
    "conn",
];

pub const CONTAINER_METHOD_ATTRS: &[&str] = &[
    "index",
    "count",
    "copy",
    "keys",
    "values",
    "items",
    "get",
    "pop",
    "clear",
    "update",
    "remove",
    "sort",
    "reverse",
    "split",
    "join",
    "replace",
    "strip",
    "upper",
    "lower",
    "encode",
    "decode",
    "startswith",
    "endswith",
    "setdefault",
    "append",
    "extend",
    "insert",
    // Properties that are also `Args`-taking methods on other classes
    // (`record.as_dict` vs `pool.as_dict(..)`).
    "as_dict",
];

/// Builtins/methods whose argument is taken by reference (`&T`).
const REF_ARG: &[&str] = &[
    "len",
    "id",
    "dir",
    "truthy",
    "assertTrue",
    "assertFalse",
    "hasattr",
    "py_iter",
];

/// `name(x, T)` forms whose second argument is a type.
const TYPE_ARG: &[&str] = &["isinstance", "assertIsInstance", "assertNotIsInstance"];

/// `unittest` assertions and their argument count without the trailing `msg`.
const ASSERT_ARITY: &[(&str, usize)] = &[
    ("assertEqual", 2),
    ("assertNotEqual", 2),
    ("assertIs", 2),
    ("assertIsNot", 2),
    ("assertIn", 2),
    ("assertNotIn", 2),
    ("assertGreater", 2),
    ("assertGreaterEqual", 2),
    ("assertLess", 2),
    ("assertLessEqual", 2),
    ("assertAlmostEqual", 2),
    ("assertCountEqual", 2),
    ("assertTrue", 1),
    ("assertFalse", 1),
    ("assertIsNone", 1),
    ("assertIsNotNone", 1),
];

pub const TESTCASE_METHODS: &[&str] = &[
    "assertEqual",
    "assertNotEqual",
    "assertTrue",
    "assertFalse",
    "assertIs",
    "assertIsNot",
    "assertIsNone",
    "assertIsNotNone",
    "assertIn",
    "assertNotIn",
    "assertIsInstance",
    "assertNotIsInstance",
    "assertGreater",
    "assertGreaterEqual",
    "assertLess",
    "assertLessEqual",
    "assertAlmostEqual",
    "assertCountEqual",
    "assertRaises",
    "assertRaisesRegex",
    "assertRegex",
    "assertNotRegex",
    "fail",
    "skipTest",
    "subTest",
    "setUp",
    "tearDown",
    "__getattr__",
    "__setattr__",
    "assertEqual_msg",
    "assertNotEqual_msg",
    "assertIs_msg",
    "assertIsNot_msg",
    "assertIn_msg",
    "assertNotIn_msg",
    "assertGreater_msg",
    "assertGreaterEqual_msg",
    "assertLess_msg",
    "assertLessEqual_msg",
    "assertAlmostEqual_msg",
    "assertCountEqual_msg",
    "assertTrue_msg",
    "assertFalse_msg",
    "assertIsNone_msg",
    "assertIsNotNone_msg",
];

fn wrap_try(ts: TokenStream) -> TokenStream {
    quote!(::laila::py_try!(#ts)?)
}

/// Pull the `#[pysig("..")]` text and the `#[decorate(expr)]` expressions
/// (outermost-first, lowered) out of a nested `fn`'s attributes.
fn split_nested_attrs(attrs: &[TokenTree]) -> (Option<String>, Vec<TokenStream>) {
    let mut sig = None;
    let mut decorators = Vec::new();
    for tt in attrs {
        let TokenTree::Group(g) = tt else { continue };
        if g.delimiter() != proc_macro2::Delimiter::Bracket {
            continue;
        }
        let inner: Vec<TokenTree> = g.stream().into_iter().collect();
        // `name(...)` or `laila::name(...)`: the ident right before the
        // parenthesised group names the attribute.
        let Some(TokenTree::Group(payload)) = inner.last() else {
            continue;
        };
        if payload.delimiter() != proc_macro2::Delimiter::Parenthesis {
            continue;
        }
        let attr_name = inner
            .iter()
            .rev()
            .skip(1)
            .find_map(|t| match t {
                TokenTree::Ident(i) => Some(i.to_string()),
                _ => None,
            })
            .unwrap_or_default();
        match attr_name.as_str() {
            "pysig" => {
                if let Some(TokenTree::Literal(l)) = payload.stream().into_iter().next() {
                    if let Ok(s) = syn::parse2::<syn::LitStr>(TokenTree::Literal(l).into()) {
                        sig = Some(s.value());
                    }
                }
            }
            "decorate" => decorators.push(payload.stream()),
            _ => {}
        }
    }
    (sig, decorators)
}

/// Every `self` token of a lowered body becomes `__laila_self` (the clone a
/// `'static` callable owns); groups are walked recursively.
///
/// The clone itself is taken with method syntax (`self.clone()`), which
/// auto-refs: inside an enclosing nested `def` / lambda the renamed `self`
/// is already the handle *value*, where `Clone::clone(self)` would not
/// type-check.
fn rename_self(ts: TokenStream) -> TokenStream {
    ts.into_iter()
        .map(|t| match t {
            TokenTree::Ident(i) if i == "self" => {
                TokenTree::Ident(Ident::new("__laila_self", i.span()))
            }
            TokenTree::Group(g) => {
                let mut ng = proc_macro2::Group::new(g.delimiter(), rename_self(g.stream()));
                ng.set_span(g.span());
                TokenTree::Group(ng)
            }
            other => other,
        })
        .collect()
}

/// Every identifier token `from` of a stream (recursively) renamed to `to`.
fn rename_ident(ts: TokenStream, from: &str, to: &Ident) -> TokenStream {
    ts.into_iter()
        .map(|t| match t {
            TokenTree::Ident(i) if i == from => TokenTree::Ident(to.clone()),
            TokenTree::Group(g) => {
                let mut ng =
                    proc_macro2::Group::new(g.delimiter(), rename_ident(g.stream(), from, to));
                ng.set_span(g.span());
                TokenTree::Group(ng)
            }
            other => other,
        })
        .collect()
}

fn truthy(ts: TokenStream) -> TokenStream {
    quote!(::laila::python::builtins::truthy(&(#ts)))
}

fn is_self(e: &Expr) -> bool {
    matches!(e, Expr::Ident(i) if i == "self")
}

/// `laila.args`: the process-wide dynamic argument mapping.
fn is_laila_args(e: &Expr) -> bool {
    matches!(e, Expr::Attr { base, name } if name == "args" && matches!(&**base, Expr::Ident(i) if i == "laila"))
}

fn ident_name(e: &Expr) -> Option<String> {
    match e {
        Expr::Ident(i) => Some(i.to_string()),
        _ => None,
    }
}

/// `laila`, `laila.data`, ... (module-shaped attribute chains).
fn as_module_path(e: &Expr) -> Option<TokenStream> {
    match e {
        Expr::Ident(i) if i == "laila" => Some(quote!(#i)),
        Expr::Attr { base, name } if !ident_is_camel(name) => {
            // `laila.args.KEY` is an attribute of the args DotMap, not a path.
            if ident_name(base).as_deref() == Some("laila")
                && !LAILA_MODULES.contains(&name.to_string().as_str())
            {
                return None;
            }
            let b = as_module_path(base)?;
            Some(quote!(#b :: #name))
        }
        _ => None,
    }
}

/// Python submodules of `laila` whose members are items, not facade
/// attributes: `laila.entry.transformation_base64` is a path, `laila.memory`
/// is a property. (`laila.args` is the DotMap and stays dynamic.)
pub const LAILA_MODULES: &[&str] = &[
    "entry", "data", "policy", "basics", "atomic", "macros", "utils", "logger", "runtime", "hal",
    "python", "_compat",
];

/// Module constants of the laila package (`NAME = value` at module level,
/// rule P11): `fn NAME() -> value` in Rust, so a bare / attribute read of
/// the name is a call. The `laila.entry` transformation presets are the
/// ones Python code reaches for by name.
pub const MODULE_CONSTANTS: &[&str] =
    &["transformation_base64", "transformation_base64_compression"];

/// `laila.entry`, `laila.data.pools`, ... (a module path strictly below the facade).
fn as_submodule_path(e: &Expr) -> Option<TokenStream> {
    match e {
        Expr::Attr { base, name } if !ident_is_camel(name) => {
            // `from unittest import mock`: `mock.patch.object(..)`
            if ident_name(base).as_deref() == Some("mock") && name == "patch" {
                return Some(quote!(::laila::python::mock::patch));
            }
            if ident_name(base).as_deref() == Some("laila") {
                if LAILA_MODULES.contains(&name.to_string().as_str()) {
                    return Some(quote!(laila :: #name));
                }
                return None;
            }
            let b = as_submodule_path(base)?;
            Some(quote!(#b :: #name))
        }
        _ => None,
    }
}

/// A type/path expression: `Name`, `a::b`, `Type.CONST`, `laila.data.S3Pool`.
pub fn as_path(e: &Expr) -> Option<TokenStream> {
    match e {
        Expr::Ident(i) if ident_is_camel(i) => Some(quote!(#i)),
        Expr::Path(ts) => Some(ts.clone()),
        Expr::Attr { base, name } => {
            if let Some(b) = as_path(base) {
                return Some(quote!(#b :: #name));
            }
            if ident_is_camel(name) {
                if let Some(m) = as_module_path(base) {
                    return Some(quote!(#m :: #name));
                }
            }
            None
        }
        _ => None,
    }
}

/// Is the path's last segment `SCREAMING_CASE` (an enum variant / const)?
fn last_segment_is_screaming(path: &TokenStream) -> bool {
    path.clone()
        .into_iter()
        .filter_map(|t| match t {
            TokenTree::Ident(i) => Some(i.to_string()),
            _ => None,
        })
        .last()
        .map(|s| {
            let s = s.strip_prefix("r#").unwrap_or(&s).trim_start_matches('_');
            !s.is_empty()
                && s.chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        })
        .unwrap_or(false)
}

fn last_segment_is_camel(path: &TokenStream) -> bool {
    path.clone()
        .into_iter()
        .filter_map(|t| match t {
            TokenTree::Ident(i) => Some(i),
            _ => None,
        })
        .last()
        .map(|i| ident_is_camel(&i))
        .unwrap_or(false)
}

fn has_kw_or_splat(args: &[CallArg]) -> bool {
    args.iter().any(|a| !matches!(a, CallArg::Pos(_)))
}

pub struct Gen<'c> {
    pub ctx: &'c Ctx,
}

impl<'c> Gen<'c> {
    pub fn new(ctx: &'c Ctx) -> Self {
        Gen { ctx }
    }

    // ---- blocks and statements ----

    pub fn block(&self, b: &Block, tail: Option<TokenStream>) -> TokenStream {
        let n = b.stmts.len();
        let mut out = TokenStream::new();
        // A nested `def` rebinds the name for the statements that follow, so
        // the context is re-derived after each one.
        let mut owned_ctx: Option<Ctx> = None;
        for (i, s) in b.stmts.iter().enumerate() {
            let last = i + 1 == n;
            let g = match &owned_ctx {
                Some(c) => Gen::new(c),
                None => Gen::new(self.ctx),
            };
            if let Stmt::Item(ts) = s {
                // `py_class!(_Stub);` in a function body: the class with its
                // `impl` methods reachable from a dynamic call (`stub.echo(..)`).
                let tts: Vec<TokenTree> = ts.clone().into_iter().collect();
                if let Some(t) = super::nested_py_class(&tts, b) {
                    out.extend(t);
                    continue;
                }
                if let Some(parts) = split_nested_fn(ts) {
                    let (lowered, direct) = g.nested_fn(parts, b);
                    out.extend(lowered);
                    let mut next = g.ctx.clone();
                    next.bound.insert(direct.0.clone());
                    if direct.1 {
                        next.closure_fns.insert(direct.0);
                    } else {
                        next.closure_fns.remove(&direct.0);
                    }
                    owned_ctx = Some(next);
                    continue;
                }
            }
            out.extend(g.stmt(s, last && tail.is_none()));
        }
        if let Some(t) = tail {
            out.extend(t);
        }
        quote!({ #out })
    }

    /// A nested `def` (py2rs emits `fn name(params) { body }` inside a body).
    ///
    /// Python closures capture the enclosing scope by reference and outlive
    /// it when passed around; Rust closures cannot capture locals of a
    /// function that may return first. Two lowerings:
    ///
    /// * the name is used as a *value* in the enclosing block (passed to
    ///   `Thread(target=..)`, `run_atomic(f)`, stored, ...): a `'static`
    ///   Python callable -- every captured local is cloned first (laila
    ///   handles are `Arc`s, so the clone aliases the same object);
    /// * otherwise a plain Rust closure borrowing the enclosing locals (and
    ///   `self`), called directly as `name(..)`.
    ///
    /// Parameters are `Arg`s and the body returns `Result<Arg>` (`return x`
    /// converts), exactly like a Python function. Returns the lowered
    /// statement and `(name, is_direct_closure)`.
    fn nested_fn(&self, parts: NestedFn, enclosing: &Block) -> (TokenStream, (String, bool)) {
        let NestedFn {
            attrs,
            name,
            params,
            body,
            is_async,
        } = parts;
        let name_s = name.to_string();
        let (pysig_text, decorators) = split_nested_attrs(&attrs);
        if let Some(text) = pysig_text {
            return self.nested_pysig_fn(&name, &text, &decorators, &body, is_async, enclosing);
        }
        if !decorators.is_empty() {
            let msg = format!(
                "laila::python: decorated nested `def {name_s}` needs a `#[pysig]` signature"
            );
            return (quote!(compile_error!(#msg);), (name_s, false));
        }
        let block = match super::parse::parse_block(&body) {
            Ok(b) => b,
            Err(e) => return (e.to_compile_error(), (name_s, false)),
        };
        let param_names = group_param_names(&params);
        let mut inner_ctx = Ctx {
            ret: RetKind::Arg,
            in_closure: true,
            in_try: false,
            in_async: is_async,
            ..self.ctx.clone()
        };
        for p in &param_names {
            inner_ctx.bound.insert(p.to_string());
        }
        let mut dyn_vars = inner_ctx.dynamic_vars.clone();
        scan_bindings(&block, &mut inner_ctx.bound, &mut dyn_vars);
        inner_ctx.dynamic_vars = dyn_vars;
        inner_ctx
            .cell_vars
            .extend(scan_cell_vars(&block, &inner_ctx.dynamic_vars));
        // `py_model!` / `py_class!` classes declared in this body are
        // dynamic classes too (instances carry assigned attributes).
        scan_model_items(&block, &mut inner_ctx.dyn_classes);
        let dyn_classes = inner_ctx.dyn_classes.clone();
        scan_dyn_objs(&block, &mut inner_ctx.dyn_objs, &dyn_classes);
        let alias_classes = inner_ctx.alias_classes.clone();
        scan_dyn_objs(&block, &mut inner_ctx.alias_objs, &alias_classes);
        if inner_ctx.dynamic_self {
            scan_arg_locals_dyn(&block, &mut inner_ctx.arg_locals, &inner_ctx.arg_fns);
        } else {
            scan_arg_locals(&block, &mut inner_ctx.arg_locals, &inner_ctx.arg_fns);
        }
        scan_bare_instances(&block, &mut inner_ctx.bare_instances);
        let g = Gen::new(&inner_ctx);
        let body_ts = g.block(
            &block,
            Some(quote!(::core::result::Result::Ok(
                ::laila::python::Arg::None
            ))),
        );
        let ps = param_names.iter().map(|p| quote!(#p: ::laila::python::Arg));

        let mut value_uses = HashSet::new();
        super::ast::value_idents_in_block(enclosing, &mut value_uses);
        let escapes = value_uses.contains(&name_s);
        // A nested `async def` is a coroutine function *value*: calling it
        // returns a coroutine (`lambda: aiowork()` hands one to a
        // taskforce; `await aiowork()` runs it). Every captured local is
        // cloned into the `'static` callable and again into each coroutine.
        // Names the nested body binds itself shadow the enclosing ones and
        // are not captured.
        let mut inner_bound = HashSet::new();
        let mut inner_dyn = HashSet::new();
        scan_bindings(&block, &mut inner_bound, &mut inner_dyn);
        if is_async {
            let mut used = HashSet::new();
            super::ast::idents_in_block(&block, &mut used);
            let params_set: HashSet<String> = param_names.iter().map(|p| p.to_string()).collect();
            let mut captured: Vec<&String> = used
                .iter()
                .filter(|n| {
                    self.ctx.bound.contains(*n)
                        && !params_set.contains(*n)
                        && !inner_bound.contains(*n)
                        && *n != "self"
                        && *n != &name_s
                })
                .collect();
            captured.sort();
            let clones: Vec<TokenStream> = captured
                .iter()
                .map(|n| {
                    let i = ident_maybe_raw(n, name.span());
                    quote!(let #i = ::core::clone::Clone::clone(&#i);)
                })
                .collect();
            let uses_self = used.contains("self");
            let body_ts = if uses_self {
                rename_self(body_ts)
            } else {
                body_ts
            };
            let self_clone = if uses_self {
                quote!(let __laila_self = self.clone();)
            } else {
                quote!()
            };
            let self_reclone = if uses_self {
                quote!(let __laila_self = ::core::clone::Clone::clone(&__laila_self);)
            } else {
                quote!()
            };
            let n_params = param_names.len();
            let binds = param_names.iter().enumerate().map(|(k, p)| {
                quote!(let #p: ::laila::python::Arg = __laila_args.positional.get(#k).cloned().unwrap_or(::laila::python::Arg::None);)
            });
            // A recursive `async def` (`rec` submits `rec`): the callable
            // reads itself back from a slot filled right after construction.
            let recursive = used.contains(&name_s);
            let (slot_new, slot_fill, self_bind) = if recursive {
                (
                    quote!(let __laila_slot: ::laila::python::__rt::Arc<::laila::python::sync::Mutex<::core::option::Option<::laila::python::Arg>>> = ::laila::python::__rt::Arc::new(::laila::python::sync::Mutex::new(::core::option::Option::None)); let __laila_slot2 = ::core::clone::Clone::clone(&__laila_slot);),
                    quote!(*__laila_slot.lock() = ::core::option::Option::Some(::core::clone::Clone::clone(&__laila_f));),
                    quote!(let #name = __laila_slot2.lock().clone().unwrap_or(::laila::python::Arg::None);),
                )
            } else {
                (quote!(), quote!(), quote!())
            };
            let closure = quote_spanned!(name.span() =>
                let #name = { #(#clones)* #self_clone #slot_new let __laila_f = ::laila::python::stdlib::asyncio::coroutine_function(#name_s, move |__laila_args: ::laila::python::Args| {
                    #(#clones)* #self_reclone #self_bind
                    let _ = #n_params;
                    #(#binds)*
                    async move {
                        #[allow(unreachable_code, unused_variables, unused_mut, clippy::all)]
                        #body_ts
                    }
                }); #slot_fill __laila_f };
            );
            return (closure, (name_s, false));
        }
        if !escapes {
            let closure = quote_spanned!(name.span() =>
                let #name = |#(#ps),*| -> ::laila::python::Result<::laila::python::Arg> {
                    #[allow(unreachable_code, unused_variables, unused_mut, clippy::all)]
                    #body_ts
                };
            );
            return (closure, (name_s, true));
        }
        // Captured enclosing bindings: cloned before the `move` (Python
        // shares them by reference; laila values are handles).
        let mut used = HashSet::new();
        super::ast::idents_in_block(&block, &mut used);
        let params_set: HashSet<String> = param_names.iter().map(|p| p.to_string()).collect();
        let mut captured: Vec<&String> = used
            .iter()
            .filter(|n| {
                self.ctx.bound.contains(*n)
                    && !params_set.contains(*n)
                    && !inner_bound.contains(*n)
                    && *n != "self"
                    && *n != &name_s
            })
            .collect();
        captured.sort();
        let clones = captured.iter().map(|n| {
            let i = ident_maybe_raw(n, name.span());
            quote!(let #i = ::core::clone::Clone::clone(&#i);)
        });
        // `self` inside the def (`def writer(i): self.pool[str(i)] = i`): a
        // `py_class!` instance is a handle, so the callable owns a clone of
        // it -- the body's `self` tokens are renamed to that clone.
        let uses_self = used.contains("self");
        let body_ts = if uses_self {
            rename_self(body_ts)
        } else {
            body_ts
        };
        let self_clone = if uses_self {
            quote!(let __laila_self = self.clone();)
        } else {
            quote!()
        };
        // A recursive `def` (`walk` calls `walk`): the callable reads itself
        // back from a slot filled right after construction.
        let recursive = used.contains(&name_s);
        let (slot_new, slot_fill, self_bind) = if recursive {
            (
                quote!(let __laila_slot: ::laila::python::__rt::Arc<::laila::python::sync::Mutex<::core::option::Option<::laila::python::Arg>>> = ::laila::python::__rt::Arc::new(::laila::python::sync::Mutex::new(::core::option::Option::None)); let __laila_slot2 = ::core::clone::Clone::clone(&__laila_slot);),
                quote!(*__laila_slot.lock() = ::core::option::Option::Some(::laila::python::IntoArg::into_arg(::core::clone::Clone::clone(&__laila_f)));),
                quote!(let #name = __laila_slot2.lock().clone().unwrap_or(::laila::python::Arg::None);),
            )
        } else {
            (quote!(), quote!(), quote!())
        };
        let closure = quote_spanned!(name.span() =>
            let #name = { #(#clones)* #self_clone #slot_new let __laila_f = ::laila::python::callable(move |#(#ps),*| -> ::laila::python::Result<::laila::python::Arg> {
                #self_bind
                #[allow(unreachable_code, unused_variables, unused_mut, clippy::all)]
                #body_ts
            }); #slot_fill __laila_f };
        );
        (closure, (name_s, false))
    }

    /// A nested `#[pysig("f(a, b=1, *args)")] fn f` (optionally under
    /// `#[decorate(expr)]` attributes): a Python *function value* with a
    /// real signature (`inspect.signature` works on it), bound as `let f`.
    /// Decorators apply innermost-first, as in Python.
    fn nested_pysig_fn(
        &self,
        name: &Ident,
        sig_text: &str,
        decorators: &[TokenStream],
        body: &proc_macro2::Group,
        is_async: bool,
        enclosing: &Block,
    ) -> (TokenStream, (String, bool)) {
        let name_s = name.to_string();
        let _ = enclosing;
        let sig = match crate::sig::parse_sig(sig_text) {
            Ok(s) => s,
            Err(msg) => {
                return (
                    quote_spanned!(name.span() => compile_error!(#msg);),
                    (name_s, false),
                )
            }
        };
        let block = match super::parse::parse_block(body) {
            Ok(b) => b,
            Err(e) => return (e.to_compile_error(), (name_s, false)),
        };
        let param_names: Vec<String> = sig.params.iter().map(|p| p.name.clone()).collect();
        let mut inner_ctx = Ctx {
            ret: RetKind::Arg,
            in_closure: true,
            in_try: false,
            in_async: is_async,
            ..self.ctx.clone()
        };
        for p in &param_names {
            inner_ctx.bound.insert(p.clone());
        }
        let mut dyn_vars = inner_ctx.dynamic_vars.clone();
        scan_bindings(&block, &mut inner_ctx.bound, &mut dyn_vars);
        inner_ctx.dynamic_vars = dyn_vars;
        inner_ctx
            .cell_vars
            .extend(scan_cell_vars(&block, &inner_ctx.dynamic_vars));
        // `py_model!` / `py_class!` classes declared in this body are
        // dynamic classes too (instances carry assigned attributes).
        scan_model_items(&block, &mut inner_ctx.dyn_classes);
        let dyn_classes = inner_ctx.dyn_classes.clone();
        scan_dyn_objs(&block, &mut inner_ctx.dyn_objs, &dyn_classes);
        let alias_classes = inner_ctx.alias_classes.clone();
        scan_dyn_objs(&block, &mut inner_ctx.alias_objs, &alias_classes);
        if inner_ctx.dynamic_self {
            scan_arg_locals_dyn(&block, &mut inner_ctx.arg_locals, &inner_ctx.arg_fns);
        } else {
            scan_arg_locals(&block, &mut inner_ctx.arg_locals, &inner_ctx.arg_fns);
        }
        scan_bare_instances(&block, &mut inner_ctx.bare_instances);
        let g = Gen::new(&inner_ctx);
        let body_ts = g.block(
            &block,
            Some(quote!(::core::result::Result::Ok(
                ::laila::python::Arg::None
            ))),
        );

        let mut inner_bound = HashSet::new();
        let mut inner_dyn = HashSet::new();
        scan_bindings(&block, &mut inner_bound, &mut inner_dyn);
        let mut used = HashSet::new();
        super::ast::idents_in_block(&block, &mut used);
        let params_set: HashSet<&String> = param_names.iter().collect();
        let mut captured: Vec<&String> = used
            .iter()
            .filter(|n| {
                self.ctx.bound.contains(*n)
                    && !params_set.contains(*n)
                    && !inner_bound.contains(*n)
                    && *n != "self"
                    && *n != &name_s
            })
            .collect();
        captured.sort();
        let clones: Vec<TokenStream> = captured
            .iter()
            .map(|n| {
                let i = ident_maybe_raw(n, name.span());
                quote!(let #i = ::core::clone::Clone::clone(&#i);)
            })
            .collect();
        let uses_self = used.contains("self");
        let body_ts = if uses_self {
            rename_self(body_ts)
        } else {
            body_ts
        };
        let self_clone = if uses_self {
            quote!(let __laila_self = self.clone();)
        } else {
            quote!()
        };
        let sig_static = crate::sig::signature_static(&sig);
        let bindings = crate::sig::bindings(&sig);
        let body_ts = if is_async {
            let self_reclone = if uses_self {
                quote!(let __laila_self = ::core::clone::Clone::clone(&__laila_self);)
            } else {
                quote!()
            };
            quote!(
                #(#clones)* #self_reclone
                ::core::result::Result::Ok(::laila::python::stdlib::asyncio::coroutine(async move {
                    #[allow(unreachable_code, unused_variables, unused_mut, clippy::all)]
                    #body_ts
                }))
            )
        } else {
            body_ts
        };
        // `#[decorate(d)]` lines are listed outermost-first; Python applies
        // the one nearest the `def` first.
        let wraps = decorators.iter().rev().map(|d| {
            let d = match parse_expr(&mut Cur::from_tts(d.clone().into_iter().collect())) {
                Ok(e) => self.expr(&e),
                Err(e) => return e.to_compile_error(),
            };
            let d = wrap_try(quote!(::laila::python::builtins::call(
                &::laila::python::IntoArg::into_arg(#d),
                ::laila::python::args!(__laila_f)
            )));
            quote!(let __laila_f = #d;)
        });
        // A recursive `def` (`_check(d)` calls `_check(v)`): the callable
        // reads itself (decorated, as Python rebinds the name) back from a
        // slot filled right after construction.
        let recursive = used.contains(&name_s);
        let (slot_new, slot_fill, self_bind) = if recursive {
            (
                quote!(let __laila_slot: ::laila::python::__rt::Arc<::laila::python::sync::Mutex<::core::option::Option<::laila::python::Arg>>> = ::laila::python::__rt::Arc::new(::laila::python::sync::Mutex::new(::core::option::Option::None)); let __laila_slot2 = ::core::clone::Clone::clone(&__laila_slot);),
                quote!(*__laila_slot.lock() = ::core::option::Option::Some(::laila::python::IntoArg::into_arg(::core::clone::Clone::clone(&__laila_f)));),
                quote!(let #name = __laila_slot2.lock().clone().unwrap_or(::laila::python::Arg::None);),
            )
        } else {
            (quote!(), quote!(), quote!())
        };
        let closure = quote_spanned!(name.span() =>
            let #name = {
                #(#clones)* #self_clone #slot_new
                #sig_static
                let __laila_f = ::laila::python::object::Function::new(#name_s, &__LAILA_SIG, move |__laila_args: ::laila::python::Args| -> ::laila::python::Result<::laila::python::Arg> {
                    #self_bind
                    #[allow(unused_mut)]
                    let mut __laila_bound = __LAILA_SIG.bind(__laila_args)?;
                    #(#[allow(unused_variables, unused_mut)] #bindings)*
                    #[allow(unreachable_code, unused_variables, unused_mut, clippy::all)]
                    #body_ts
                });
                #(#wraps)*
                #slot_fill
                __laila_f
            };
        );
        (closure, (name_s, false))
    }

    /// Block whose value is its trailing expression (Rust semantics).
    pub fn value_block(&self, b: &Block) -> TokenStream {
        self.block(b, None)
    }

    fn stmt(&self, s: &Stmt, is_tail: bool) -> TokenStream {
        match s {
            Stmt::Let { header, value } => match value {
                // Python ints/floats/None/bools bound to a bare name are
                // dynamic (`x = 0; x += item`), so they start life as `Arg`.
                Some(v) if is_dynamic_scalar_literal(v) && is_plain_let(header) => {
                    let v = self.expr(v);
                    let mut names = HashSet::new();
                    let_header_names(header, &mut names);
                    if names.iter().any(|n| self.ctx.cell_vars.contains(n)) {
                        // A closure cell: shared with the nested defs.
                        let name = Ident::new(names.iter().next().unwrap(), Span::call_site());
                        return quote!(let #name = ::laila::python::Cell::new(::laila::python::IntoArg::into_arg(#v)););
                    }
                    let header = ensure_mut(header);
                    quote!(#header = ::laila::python::IntoArg::into_arg(#v);)
                }
                // `k, v = d.popitem()`: Python unpacking of whatever the right
                // side is (a Rust tuple or an `Arg` sequence), with Python's
                // `ValueError` texts.
                Some(v) if let_tuple_arity(header).is_some() => {
                    let n = let_tuple_arity(header).unwrap();
                    let v = self.expr(v);
                    let pat: TokenStream = header.clone().into_iter().skip(1).collect();
                    if has_nested_tuple(&pat) {
                        let mut c = 0usize;
                        return nested_unpack_let(&pat, v, &mut c);
                    }
                    let unpack = Ident::new(&format!("unpack{n}"), Span::call_site());
                    let w = wrap_try(quote!(::laila::python::builtins::#unpack(
                        ::laila::python::IntoArg::into_arg(#v)
                    )));
                    quote!(#header = #w;)
                }
                // `before = people`: a Python name bound to an `Arg` local is
                // an alias of the same object -- the handle is shared, the
                // original stays usable (`people += ..; people is before`).
                Some(Expr::Ident(i))
                    if is_plain_let(header)
                        && (self.ctx.arg_locals.contains(&i.to_string())
                            || self.ctx.dynamic_vars.contains(&i.to_string())) =>
                {
                    let x = self.ident(i);
                    quote!(#header = ::laila::python::IntoArg::into_arg(&#x);)
                }
                Some(v) => {
                    let v = self.expr(v);
                    quote!(#header = #v;)
                }
                None => quote!(#header;),
            },
            Stmt::Assign { target, value } => self.assign(target, value),
            // `obj.x += v` / `d[k] += v`: read through the attribute / item
            // protocol, operate, write back through `__setattr__` / `__setitem__`.
            Stmt::AugAssign { target, op, value }
                if matches!(target, Expr::Attr { .. } | Expr::Index { .. })
                    || matches!(target, Expr::Ident(i) if self.ctx.cell_vars.contains(&i.to_string())) =>
            {
                let op_str = op.to_string().replace(' ', "");
                let bin = match op_str.as_str() {
                    "+=" => BinOp::Add,
                    "-=" => BinOp::Sub,
                    "*=" => BinOp::Mul,
                    "/=" => BinOp::Div,
                    "%=" => BinOp::Mod,
                    "|=" => BinOp::BitOr,
                    "&=" => BinOp::BitAnd,
                    "^=" => BinOp::BitXor,
                    "<<=" => BinOp::Shl,
                    ">>=" => BinOp::Shr,
                    _ => BinOp::Add,
                };
                let span = op
                    .clone()
                    .into_iter()
                    .next()
                    .map(|t| t.span())
                    .unwrap_or_else(Span::call_site);
                let combined = Expr::Binary {
                    op: bin,
                    left: Box::new(target.clone()),
                    right: Box::new(value.clone()),
                    span,
                };
                self.assign(target, &combined)
            }
            Stmt::AugAssign { target, op, value } => {
                // `a += b` -> `a = py_try!((a + b))?` so `Arg` arithmetic can raise.
                let op_str = op.to_string().replace(' ', "");
                let bin: TokenStream = match op_str.as_str() {
                    "+=" => quote!(+),
                    "-=" => quote!(-),
                    "*=" => quote!(*),
                    "/=" => quote!(/),
                    "%=" => quote!(%),
                    "|=" => quote!(|),
                    "&=" => quote!(&),
                    "^=" => quote!(^),
                    "<<=" => quote!(<<),
                    ">>=" => quote!(>>),
                    _ => quote!(+),
                };
                let t = self.place(target);
                // `nxt += period` in a loop: a local operand is shared, not
                // consumed (an `Arg` handle is cloned).
                let v = self.arith_operand(value, false);
                if op_str == "+=" {
                    // `m1 += m2` on a `Manifest` is `__iadd__` (in place,
                    // same identity); plain operands fall back to `a = a + b`.
                    let w = wrap_try(quote!({
                        #[allow(unused_imports)]
                        use ::laila::python::ops::{InPlaceAdd as _, PlainIAdd as _};
                        (&#t).__laila_iadd(#v)
                    }));
                    return quote!(#t = #w;);
                }
                let w = wrap_try(quote!((#t #bin #v)));
                quote!(#t = #w;)
            }
            Stmt::Expr { expr, semi } => {
                let e = self.expr(expr);
                if is_tail && !*semi {
                    quote!(#e)
                } else if matches!(
                    expr,
                    Expr::Binary {
                        op: BinOp::Shl | BinOp::Shr,
                        ..
                    }
                ) {
                    // `laila.alpha_pool << hdf5_pool << s3_pool;` -- the chain's
                    // value (the last pool) is Python's discarded expression result.
                    quote!(let _ = #e;)
                } else if *semi
                    || !matches!(expr, Expr::If { .. } | Expr::Match { .. } | Expr::Block(_))
                {
                    quote!(#e;)
                } else {
                    quote!(#e)
                }
            }
            Stmt::Return(e, span) => self.ret(e.as_ref(), *span),
            Stmt::With {
                cm,
                target,
                body,
                is_async,
                span,
            } => self.with(cm, target.as_ref(), body, *is_async, *span),
            Stmt::Raise(e, span) => match e {
                Some(e) => {
                    let e = self.expr(e);
                    quote_spanned!(*span => return ::core::result::Result::Err(::core::convert::Into::into(#e));)
                }
                // Bare `raise` inside an `except` handler re-raises its error.
                None if self.ctx.in_try => {
                    quote_spanned!(*span => return ::core::result::Result::Err(::core::clone::Clone::clone(&__laila_exc));)
                }
                None => {
                    quote_spanned!(*span => return ::core::result::Result::Err(::laila::python::LailaError::runtime_error("No active exception to reraise"));)
                }
            },
            Stmt::Del(e, span) => match e {
                Expr::Index { base, index, .. } => {
                    let b = self.expr(base);
                    let k = self.index_key(index);
                    let call = quote_spanned!(*span => #b.__delitem__(#k));
                    let w = wrap_try(call);
                    quote!(#w;)
                }
                // `del obj.name` -> `obj.__delattr__("name")`
                Expr::Attr { base, name } => {
                    let n = name.to_string();
                    let b = self.expr(base);
                    let call = quote_spanned!(*span => #b.__delattr__(#n));
                    let w = wrap_try(call);
                    quote!(#w;)
                }
                other => {
                    let e = self.expr(other);
                    quote_spanned!(*span => ::core::mem::drop(#e);)
                }
            },
            Stmt::Try {
                body,
                handlers,
                orelse,
                finalbody,
                span,
            } => self.try_stmt(body, handlers, orelse.as_ref(), finalbody.as_ref(), *span),
            Stmt::For {
                pat,
                iter,
                body,
                label,
            } => {
                let it = self.iter_operand(iter);
                let b = self.loop_body(body);
                let iter_call = wrap_try(quote!(::laila::python::builtins::py_iter(#it)));
                // Items are pulled lazily (`PyIterator` yields `Result`s): a
                // raised `__next__` surfaces at the item like in Python.
                let item = wrap_try(quote!(__laila_item));
                if is_tuple_pattern(pat) && has_nested_tuple(pat) {
                    let mut c = 0usize;
                    let binds = nested_unpack_let(pat, quote!(__laila_item), &mut c);
                    quote!(#label for __laila_item in #iter_call { let __laila_item = #item; #binds #b })
                } else if is_tuple_pattern(pat) {
                    let n = tuple_arity(pat);
                    let unpack = Ident::new(&format!("unpack{n}"), Span::call_site());
                    let inner = wrap_try(quote!(::laila::python::builtins::#unpack(__laila_item)));
                    quote!(#label for __laila_item in #iter_call { let __laila_item = #item; let #pat = #inner; #b })
                } else {
                    quote!(#label for __laila_item in #iter_call { let #pat = #item; #b })
                }
            }
            Stmt::While { cond, body } => {
                let c = self.cond(cond);
                let b = self.loop_body(body);
                quote!(while #c #b)
            }
            Stmt::Loop { body } => {
                let b = self.loop_body(body);
                quote!(loop #b)
            }
            // [P16d] inside a `try` closure: hand the loop control outward.
            Stmt::Break(rest) if self.ctx.try_loopctl && rest.is_empty() => {
                quote!(return ::core::result::Result::Ok(::core::option::Option::Some(true));)
            }
            Stmt::Continue(rest) if self.ctx.try_loopctl && rest.is_empty() => {
                quote!(return ::core::result::Result::Ok(::core::option::Option::Some(false));)
            }
            Stmt::Break(rest) => quote!(break #rest;),
            Stmt::Continue(rest) => quote!(continue #rest;),
            Stmt::Item(ts) => {
                let tts: Vec<TokenTree> = ts.clone().into_iter().collect();
                if super::model::is_py_model(&tts) {
                    return super::model::expand_py_model(&tts, self);
                }

                // A class declared inside a function body (`py_class!(_Stub);
                // impl _Stub { .. }`): its methods are Python methods like a
                // module-level class's -- lowered through the same pass.
                if super::item_kind(&tts).as_deref() == Some("impl")
                    && super::trait_impl_parts(&tts).is_none()
                {
                    let opts = super::Opts {
                        unittest: false,
                        dynamic: self.ctx.dynamic_self,
                        functions: HashSet::new(),
                        modules: self.ctx.dyn_objs.clone(),
                        dyn_classes: self.ctx.alias_classes.clone(),
                    };
                    let empty: HashSet<String> = HashSet::new();
                    return match super::expand_impl(
                        tts,
                        &opts,
                        &self.ctx.local_fns,
                        &self.ctx.raw_params,
                        &self.ctx.dyn_classes,
                        &self.ctx.local_classes,
                        &self.ctx.local_methods,
                        &empty,
                        &self.ctx.pysig_methods,
                        &self.ctx.module_consts,
                        &empty,
                        &self.ctx.arg_fns,
                    ) {
                        Ok(t) => t,
                        Err(e) => e.to_compile_error(),
                    };
                }
                ts.clone()
            }
            Stmt::Raw(ts) => ts.clone(),
            Stmt::Pass => quote!(),
        }
    }

    fn ret(&self, e: Option<&Expr>, span: Span) -> TokenStream {
        if self.ctx.in_try {
            // [P16c] the block runs as a closure yielding `Option<value>`;
            // `return` leaves it with `Some(value)` and the statement after
            // the `try` returns that value from the enclosing function.
            return match (self.ctx.try_ret, e) {
                (Some(TryRet::Arg), Some(e)) => {
                    let e = self.expr(e);
                    quote_spanned!(span => return ::core::result::Result::Ok(::core::option::Option::Some(::laila::python::IntoArg::into_arg(#e)));)
                }
                (Some(TryRet::Arg), None) => {
                    quote_spanned!(span => return ::core::result::Result::Ok(::core::option::Option::Some(::laila::python::Arg::None));)
                }
                (Some(TryRet::Unit), None) => {
                    quote_spanned!(span => return ::core::result::Result::Ok(::core::option::Option::Some(()));)
                }
                (Some(TryRet::Unit), Some(_)) => {
                    quote_spanned!(span => compile_error!("laila::python: `return value` inside `try` / `except` of a function without a return type; declare `-> Arg`");)
                }
                (Some(TryRet::Typed), Some(e)) if is_int_operand(e) => {
                    let e = self.expr(e);
                    quote_spanned!(span => return ::core::result::Result::Ok(::core::option::Option::Some((#e) as _));)
                }
                (Some(TryRet::Typed), Some(e)) => {
                    let e = self.expr(e);
                    quote_spanned!(span => return ::core::result::Result::Ok(::core::option::Option::Some(::laila::python::RetInto::ret_into(#e)?));)
                }
                (Some(TryRet::Typed), None) => {
                    quote_spanned!(span => compile_error!("laila::python: bare `return` inside `try` / `except` of a function with a typed return (`-> T`)");)
                }
                (None, _) => {
                    quote_spanned!(span => compile_error!("laila::python: `return` inside `try` / `except` of a function with this return type is not supported; return after the statement");)
                }
            };
        }
        if self.ctx.in_closure {
            return match e {
                Some(e) => {
                    let e = self.expr(e);
                    quote_spanned!(span => return ::core::result::Result::Ok(::laila::python::IntoArg::into_arg(#e));)
                }
                None => {
                    quote_spanned!(span => return ::core::result::Result::Ok(::laila::python::Arg::None);)
                }
            };
        }
        match (e, &self.ctx.ret) {
            (Some(e), RetKind::Arg) => {
                let e = self.expr(e);
                quote_spanned!(span => return ::core::result::Result::Ok(::laila::python::IntoArg::into_arg(#e));)
            }
            // `-> T`: a dynamic `Arg` holding a `T` converts (`def f() -> int:
            // return self.n`); a `T` passes through (`RetInto<T> for T`).
            // A bare integer literal (`def f() -> int: return 1`) takes the
            // declared type directly: `RetInto<i64>` has several sources
            // (`i64`, `usize`, `Arg`), so an untyped literal would otherwise
            // fall back to `i32`.
            (Some(e), RetKind::Other) if is_int_operand(e) => {
                let e = self.expr(e);
                quote_spanned!(span => return ::core::result::Result::Ok((#e) as _);)
            }
            (Some(e), RetKind::Other) => {
                let e = self.expr(e);
                quote_spanned!(span => return ::core::result::Result::Ok(::laila::python::RetInto::ret_into(#e)?);)
            }
            (Some(e), _) => {
                let e = self.expr(e);
                quote_spanned!(span => return ::core::result::Result::Ok(#e);)
            }
            (None, RetKind::Arg) => {
                quote_spanned!(span => return ::core::result::Result::Ok(::laila::python::Arg::None);)
            }
            (None, _) => quote_spanned!(span => return ::core::result::Result::Ok(());),
        }
    }

    fn assign(&self, target: &Expr, value: &Expr) -> TokenStream {
        // `obj.x = other` / `d[k] = other` share the value (a handle clone),
        // so the local stays usable after the statement, as in Python.
        let v = match (target, value) {
            (Expr::Attr { .. } | Expr::Index { .. }, Expr::Ident(i))
                if i != "self" && self.ctx.bound.contains(&i.to_string()) =>
            {
                self.arg_value(value)
            }
            // `self.Cls = Cls`: a class stored as a value (the class object).
            (Expr::Attr { .. } | Expr::Index { .. }, Expr::Ident(i))
                if ident_is_camel(i)
                    && !self.ctx.bound.contains(&i.to_string())
                    && !self.ctx.module_consts.contains(&i.to_string())
                    && !CAMEL_VALUES.contains(&i.to_string().as_str()) =>
            {
                self.arg_value(value)
            }
            _ => self.expr(value),
        };
        match target {
            Expr::Attr { base, name } => {
                let n = name.to_string();
                if is_self(base) {
                    if self.ctx.dynamic_self && !self.ctx.impl_methods.contains(&n) {
                        let w = wrap_try(quote!(self.__setattr__(#n, #v)));
                        return quote!(#w;);
                    }
                    if self.ctx.impl_methods.contains(&n)
                        || self.ctx.impl_methods.contains("__setattr__") && self.ctx.dynamic_self
                    {
                        let w = wrap_try(quote!(self.__setattr__(#n, #v)));
                        return quote!(#w;);
                    }
                    return quote!(self.#name = #v;);
                }
                let b = self.expr(base);
                // `relay.timeout = 0.2`: a typed object's `__setattr__` (the
                // `PyObject` protocol) takes an `Arg`.
                let w =
                    wrap_try(quote!(#b.__setattr__(#n, ::laila::python::IntoArg::into_arg(#v))));
                quote!(#w;)
            }
            Expr::Index { base, index, span } => {
                let b = self.expr(base);
                let k = self.index_key(index);
                let w = wrap_try(quote_spanned!(*span => #b.__setitem__(#k, #v)));
                quote!(#w;)
            }
            // A local declared from a dynamic scalar (`last = None`) holds an
            // `Arg`; every later assignment converts, as Python rebinding would.
            // Likewise a local first bound to an `Arg` (`p = _fresh_policy()`
            // then `p = DefaultPolicy()`).
            Expr::Ident(i) if self.ctx.cell_vars.contains(&i.to_string()) => {
                quote!(#i.set(::laila::python::IntoArg::into_arg(#v));)
            }
            Expr::Ident(i)
                if self.ctx.dynamic_vars.contains(&i.to_string())
                    || self.ctx.arg_locals.contains(&i.to_string()) =>
            {
                quote!(#i = ::laila::python::IntoArg::into_arg(#v);)
            }
            // `(k, v) = pair`: Python unpacking into existing locals;
            // `(self.a, self.b) = pair`: each element assigned in turn.
            Expr::Tuple(g) => {
                let parts = crate::literal::split_commas(g.stream());
                let n = parts.len();
                let unpack = Ident::new(&format!("unpack{n}"), Span::call_site());
                let w = wrap_try(quote!(::laila::python::builtins::#unpack(
                    ::laila::python::IntoArg::into_arg(#v)
                )));
                let all_idents = parts
                    .iter()
                    .all(|p| matches!(p.as_slice(), [TokenTree::Ident(_)]));
                if all_idents {
                    let pat = g.stream();
                    return quote!((#pat) = #w;);
                }
                let temps: Vec<Ident> = (0..n)
                    .map(|i| Ident::new(&format!("__laila_u{i}"), Span::call_site()))
                    .collect();
                let assigns: Vec<TokenStream> = parts
                    .iter()
                    .zip(&temps)
                    .map(|(p, t)| {
                        let mut c = Cur::from_tts(p.clone());
                        match parse_expr(&mut c) {
                            Ok(e) => self.assign(&e, &Expr::Raw(quote!(#t))),
                            Err(er) => er.to_compile_error(),
                        }
                    })
                    .collect();
                quote!(let (#(#temps),*) = #w; #(#assigns)*)
            }
            other => {
                let t = self.place(other);
                quote!(#t = #v;)
            }
        }
    }

    /// A bare name: a stdlib module shim unless the function binds the name.
    fn ident(&self, i: &Ident) -> TokenStream {
        let n = i.to_string();
        // `r#struct`: the module whose name is a Rust keyword.
        let bare = n.strip_prefix("r#").unwrap_or(&n);
        if STDLIB_MODULES.contains(&bare) && !self.ctx.bound.contains(&n) {
            return quote_spanned!(i.span() => ::laila::python::stdlib::#i);
        }
        // `__file__`: this source file's path (`os.path.dirname(__file__)`).
        if n == "__file__" && !self.ctx.bound.contains(&n) {
            return quote_spanned!(i.span() => ::laila::python::IntoArg::into_arg(::core::file!()));
        }
        // `from laila.entry import transformation_base64`: an imported
        // module constant is a value-returning function in Rust (P11).
        if MODULE_CONSTANTS.contains(&n.as_str()) && !self.ctx.bound.contains(&n) {
            return quote_spanned!(i.span() => #i());
        }
        // A module constant of this block (`static NAME: LazyLock<Arg>`):
        // the Python name is the object; the Rust twin shares the handle.
        if self.ctx.module_consts.contains(&n) && !self.ctx.bound.contains(&n) {
            return quote_spanned!(i.span() => (*#i).clone());
        }
        // `dict` / `list` / `str` ... used as *values* (`default_factory=dict`)
        // are the builtin classes.
        if BUILTIN_TYPE_NAMES.contains(&n.as_str()) && !self.ctx.bound.contains(&n) {
            return quote_spanned!(i.span() => ::laila::python::builtins::builtin_class(#n));
        }
        // A module-level `fn` of this block used as a *value* (`[_pid]`,
        // `submit(_pid)`) is the callable object.
        if self.ctx.local_fns.contains(&n)
            && !self.ctx.bound.contains(&n)
            && !BUILTIN_FNS.contains(&n.as_str())
        {
            return quote_spanned!(i.span() => ::laila::python::callable(#i));
        }
        // `assertIn(x, _DIRTY_POLICY_GIDS)`: a `CONSTANT`-case name imported
        // from a laila module, read as a value (a function, `static` or
        // `const` on the Rust side).
        if self.is_module_value_const(i) {
            return quote_spanned!(i.span() => ::laila::python::builtins::module_value(&#i));
        }
        // A closure cell (`stop`): the current value.
        if self.ctx.cell_vars.contains(&n) {
            return quote_spanned!(i.span() => #i.get());
        }
        quote!(#i)
    }

    /// An assignable place (identifier, field, deref), emitted without `py_try`.
    fn place(&self, e: &Expr) -> TokenStream {
        match e {
            Expr::Attr { base, name } if is_self(base) => quote!(self.#name),
            Expr::Attr { base, name } => {
                let b = self.place(base);
                quote!(#b.#name)
            }
            Expr::TupleField { base, index } => {
                let b = self.place(base);
                quote!(#b.#index)
            }
            Expr::Unary {
                op: UnOp::Deref,
                expr,
                ..
            } => {
                let e = self.place(expr);
                quote!(*#e)
            }
            Expr::Index { base, index, .. } => {
                let b = self.place(base);
                let k = self.index_key(index);
                quote!(#b[#k])
            }
            other => self.expr(other),
        }
    }

    fn with(
        &self,
        cm: &Expr,
        target: Option<&TokenStream>,
        body: &Block,
        is_async: bool,
        span: Span,
    ) -> TokenStream {
        // assertRaises / pytest.raises: run the body as a Result closure.
        if let Expr::MethodCall {
            recv, name, args, ..
        } = cm
        {
            let n = name.to_string();
            let is_assert_raises =
                is_self(recv) && (n == "assertRaises" || n == "assertRaisesRegex");
            let is_pytest_raises = ident_name(recv).as_deref() == Some("pytest") && n == "raises";
            // `with contextlib.suppress(T, ..):` -- the body runs as a
            // Result closure and a matching error is swallowed.
            if ident_name(recv).as_deref() == Some("contextlib") && n == "suppress" {
                let ty = match args.first() {
                    Some(CallArg::Pos(t)) => self.type_arg(t),
                    _ => {
                        return quote_spanned!(span => compile_error!("laila::python: contextlib.suppress needs an exception type");)
                    }
                };
                let body_ts = self.block_as_result_closure(body);
                let w = wrap_try(
                    quote!(::laila::python::stdlib::contextlib::suppress::<#ty>(#body_ts)),
                );
                return quote!(#w;);
            }
            // `with self.assertRaises(_ERRORS)`: a class *value* (module
            // constant, local) rather than a type.
            if is_assert_raises && n == "assertRaises" {
                if let Some(CallArg::Pos(t)) = args.first() {
                    let is_value = match t {
                        Expr::Ident(i) => {
                            let s = i.to_string();
                            self.ctx.module_consts.contains(&s)
                                || (self.ctx.bound.contains(&s) && s != "self")
                        }
                        Expr::Index { .. } => true,
                        Expr::Attr { base, name } => is_self(base) || name == "__class__",
                        _ => false,
                    };
                    if is_value {
                        let v = self.arg_value(t);
                        let body_ts = self.block_as_result_closure(body);
                        let w = wrap_try(quote!(self.assertRaises_value(#v, #body_ts)));
                        return match target {
                            Some(t) => quote!(let #t = #w;),
                            None => quote!(#w;),
                        };
                    }
                }
            }
            if is_assert_raises || is_pytest_raises {
                let ty = match args.first() {
                    Some(CallArg::Pos(t)) => self.type_arg(t),
                    _ => {
                        return quote_spanned!(span => compile_error!("laila::python: assertRaises needs an exception type");)
                    }
                };
                let body_ts = self.block_as_result_closure(body);
                let call = if is_pytest_raises {
                    let m = args.iter().find_map(|a| match a {
                        CallArg::Kw(k, v) if k == "match" => Some(self.expr(v)),
                        _ => None,
                    });
                    match m {
                        Some(m) => {
                            quote!(::laila::python::pytest::raises_match::<#ty>(#m, #body_ts))
                        }
                        None => quote!(::laila::python::pytest::raises::<#ty>(#body_ts)),
                    }
                } else if n == "assertRaisesRegex" {
                    let pat = match args.get(1) {
                        Some(CallArg::Pos(p)) => self.expr(p),
                        _ => {
                            return quote_spanned!(span => compile_error!("assertRaisesRegex needs a pattern");)
                        }
                    };
                    quote!(self.assertRaisesRegex::<#ty>(&(#pat), #body_ts))
                } else {
                    quote!(self.assertRaises::<#ty>(#body_ts))
                };
                let w = wrap_try(call);
                return match target {
                    Some(t) => quote!(let #t = #w;),
                    None => quote!(#w;),
                };
            }
        }
        // Python's ``with`` does not open a scope: names bound in the body
        // stay visible after it. The body is therefore emitted *inline* in
        // the enclosing block, with the context manager and the guard it
        // returned bound to unique names and the guard dropped (``__exit__``)
        // right after the last body statement (early ``?`` returns drop it
        // too).
        let n = self.ctx.next_with_id();
        let cm_name = Ident::new(&format!("__laila_cm_{n}"), span);
        let guard_name = Ident::new(&format!("__laila_guard_{n}"), span);
        // `with lock:` on a local: borrowed, so the name stays usable after
        // the block (Python shares it; a second `with lock:` must work).
        let cm_ts = match cm {
            Expr::Ident(i) if i != "self" => {
                let i = self.ident(i);
                quote!(&#i)
            }
            _ => self.expr(cm),
        };
        let b = self.block(body, None);
        let enter = if is_async {
            quote!(#cm_name.__aenter__().await)
        } else {
            quote!(#cm_name.__enter__())
        };
        let enter = wrap_try(enter);
        let body_stmts: TokenStream = match b.into_iter().next() {
            Some(TokenTree::Group(g)) => g.stream(),
            _ => TokenStream::new(),
        };
        // Normal exit: a guard with a *raising* `__exit__` (`laila.guarantee`)
        // runs it and propagates the error; any other guard is a no-op here
        // and releases in `Drop` (autoref specialisation, see
        // `laila::python::context`).
        let exit_of = |g: &Ident| -> TokenStream {
            let call = if is_async {
                quote!({
                    #[allow(unused_imports)]
                    use ::laila::python::context::{NoopAexit as _, RaisingAexit as _};
                    (&#g).__laila_aexit().await
                })
            } else {
                quote!({
                    #[allow(unused_imports)]
                    use ::laila::python::context::{NoopExit as _, RaisingExit as _};
                    (&#g).__laila_exit()
                })
            };
            let w = wrap_try(call);
            quote!(#w;)
        };
        match target {
            // `with patch.object(..) as m:` / `with patch(..) as m:` binds the
            // *mock*, which outlives the patch (`m.assert_called_once()` after
            // the block is Python); the guard stops the patch at the end.
            Some(t) if is_patch_cm(cm) => {
                quote_spanned!(span => let #cm_name = #cm_ts; let #guard_name = #enter; let #t = ::laila::python::IntoArg::into_arg(#guard_name.mock()); #body_stmts ::core::mem::drop(#guard_name);)
            }
            Some(t) => {
                let is_ident = {
                    let v: Vec<TokenTree> = t.clone().into_iter().collect();
                    matches!(v.as_slice(), [TokenTree::Ident(_)])
                };
                let release = if is_ident {
                    let t_ident = match t.clone().into_iter().next() {
                        Some(TokenTree::Ident(i)) => i,
                        _ => unreachable!(),
                    };
                    let exit = exit_of(&t_ident);
                    // Python keeps the `as` name bound after the block:
                    // drop the guard, rebind the name to a copy when the
                    // value is `Clone` (else to `()`).
                    quote!(#exit let #t = {
                        #[allow(unused_imports)]
                        use ::laila::python::context::{KeepClone as _, KeepUnit as _};
                        let __laila_w = ::laila::python::context::Keep(#t);
                        let __laila_kept = (&__laila_w).__laila_keep();
                        ::core::mem::drop(__laila_w);
                        __laila_kept
                    };)
                } else {
                    quote!()
                };
                quote_spanned!(span => let #cm_name = #cm_ts; let #t = #enter; #body_stmts #release)
            }
            None => {
                let exit = exit_of(&guard_name);
                quote_spanned!(span => let #cm_name = #cm_ts; let #guard_name = #enter; #body_stmts #exit ::core::mem::drop(#guard_name);)
            }
        }
    }

    /// The body of a loop: its `break` / `continue` are its own, even
    /// inside a `try` closure that carries the enclosing loop's control.
    fn loop_body(&self, body: &Block) -> TokenStream {
        if !self.ctx.try_loopctl {
            return self.block(body, None);
        }
        let inner_ctx = Ctx {
            try_loopctl: false,
            ..self.ctx.clone()
        };
        Gen::new(&inner_ctx).block(body, None)
    }

    /// `try { } except T as e { } else { } finally { }` [P16]: the body and
    /// each handler run as `Result<()>` closures; the handler whose class
    /// matches (`LailaError::is::<T>()`, subclass-aware) runs with `e` bound
    /// to the error; `finally` runs unconditionally before the outcome is
    /// propagated with `?`. A bare `raise` in a handler re-raises its error.
    fn try_stmt(
        &self,
        body: &Block,
        handlers: &[Handler],
        orelse: Option<&Block>,
        finalbody: Option<&Block>,
        span: Span,
    ) -> TokenStream {
        // [P16c] a `return` in the body / a handler / `else` leaves the
        // enclosing function: the closures then yield `Option<value>`.
        let carries_return = block_has_return(body)
            || handlers.iter().any(|h| block_has_return(&h.body))
            || orelse.is_some_and(block_has_return);
        let try_ret = if !carries_return {
            None
        } else if self.ctx.in_try {
            self.ctx.try_ret
        } else if self.ctx.in_closure {
            Some(TryRet::Arg)
        } else {
            match self.ctx.ret {
                RetKind::Unit => Some(TryRet::Unit),
                RetKind::Arg => Some(TryRet::Arg),
                RetKind::Other => Some(TryRet::Typed),
                _ => None,
            }
        };
        // [P16d] a `break` / `continue` of the enclosing loop in the body /
        // a handler / `else`: the closures yield `Option<bool>`.
        let carries_loopctl = !carries_return
            && (block_has_loopctl(body)
                || handlers.iter().any(|h| block_has_loopctl(&h.body))
                || orelse.is_some_and(block_has_loopctl));
        let inner_ctx = Ctx {
            ret: RetKind::Unit,
            in_closure: false,
            in_try: true,
            try_ret,
            try_loopctl: carries_loopctl,
            ..self.ctx.clone()
        };
        let g = Gen::new(&inner_ctx);
        let (fallthrough, ret_ty) = if carries_return {
            (
                quote!(::core::result::Result::Ok(::core::option::Option::None)),
                quote!(::core::option::Option<_>),
            )
        } else if carries_loopctl {
            (
                quote!(::core::result::Result::Ok(::core::option::Option::None)),
                quote!(::core::option::Option<bool>),
            )
        } else {
            (quote!(::core::result::Result::Ok(())), quote!(()))
        };
        let in_async = self.ctx.in_async;
        let as_closure = |b: &Block| -> TokenStream {
            let blk = g.block(b, Some(fallthrough.clone()));
            if in_async {
                // Inside an `async def`: an awaited async block, so the body
                // may itself `await`.
                return quote!((async { let __laila_out: ::laila::python::Result<#ret_ty> = #blk; __laila_out }).await);
            }
            quote!((|| -> ::laila::python::Result<#ret_ty> { #[allow(unreachable_code, unused_variables)] #blk })())
        };
        let body_ts = as_closure(body);
        let ok_arm = match orelse {
            Some(b) => as_closure(b),
            None => fallthrough.clone(),
        };
        let mut chain = TokenStream::new();
        for (i, h) in handlers.iter().enumerate() {
            let cond = match &h.ty {
                None => quote!(true),
                Some(Expr::Tuple(grp)) => {
                    let checks: Vec<TokenStream> = crate::literal::split_commas(grp.stream())
                        .iter()
                        .map(|tts| {
                            let mut c = Cur::from_tts(tts.clone());
                            match parse_expr(&mut c) {
                                Ok(e) => {
                                    let t = self.type_arg(&e);
                                    quote!(__laila_exc.is::<#t>())
                                }
                                Err(er) => er.to_compile_error(),
                            }
                        })
                        .collect();
                    quote!((#(#checks)||*))
                }
                Some(t) => {
                    let t = self.type_arg(t);
                    quote!(__laila_exc.is::<#t>())
                }
            };
            let bind = match &h.name {
                Some(n) => quote!(let #n = ::core::clone::Clone::clone(&__laila_exc);),
                None => quote!(),
            };
            let hb = as_closure(&h.body);
            let kw = if i == 0 { quote!(if) } else { quote!(else if) };
            chain.extend(quote!(#kw #cond { #bind #hb }));
        }
        let err_arm = if handlers.is_empty() {
            quote!(::core::result::Result::Err(__laila_exc))
        } else {
            quote!(#chain else { ::core::result::Result::Err(__laila_exc) })
        };
        let fin = match finalbody {
            Some(b) => {
                let blk = self.block(b, None);
                quote!(#blk)
            }
            None => quote!(),
        };
        if carries_return {
            // The value a `return` carried out of the closures leaves the
            // enclosing function after `finally` ran.
            let leave = if self.ctx.in_try {
                // nested `try` inside a `try`: hand the value outward.
                quote!(return ::core::result::Result::Ok(::core::option::Option::Some(__laila_v));)
            } else {
                quote!(return ::core::result::Result::Ok(__laila_v);)
            };
            return quote_spanned!(span => {
                let __laila_try: ::laila::python::Result<::core::option::Option<_>> = #body_ts;
                #[allow(clippy::needless_question_mark, unused_variables, clippy::redundant_closure_call)]
                let __laila_r: ::laila::python::Result<::core::option::Option<_>> = match __laila_try {
                    ::core::result::Result::Ok(::core::option::Option::Some(__laila_v)) => ::core::result::Result::Ok(::core::option::Option::Some(__laila_v)),
                    ::core::result::Result::Ok(::core::option::Option::None) => #ok_arm,
                    ::core::result::Result::Err(__laila_exc) => #err_arm,
                };
                #fin
                if let ::core::option::Option::Some(__laila_v) = __laila_r? {
                    #leave
                }
            });
        }
        if carries_loopctl {
            // The loop control a `break` / `continue` carried out of the
            // closures applies to the enclosing loop after `finally` ran.
            let apply = if self.ctx.try_loopctl {
                // nested `try` inside a `try` closure: hand it outward.
                quote!(return ::core::result::Result::Ok(::core::option::Option::Some(__laila_b));)
            } else {
                quote!(if __laila_b {
                    break;
                } else {
                    continue;
                })
            };
            return quote_spanned!(span => {
                let __laila_try: ::laila::python::Result<::core::option::Option<bool>> = #body_ts;
                #[allow(clippy::needless_question_mark, unused_variables, clippy::redundant_closure_call)]
                let __laila_r: ::laila::python::Result<::core::option::Option<bool>> = match __laila_try {
                    ::core::result::Result::Ok(::core::option::Option::Some(__laila_b)) => ::core::result::Result::Ok(::core::option::Option::Some(__laila_b)),
                    ::core::result::Result::Ok(::core::option::Option::None) => #ok_arm,
                    ::core::result::Result::Err(__laila_exc) => #err_arm,
                };
                #fin
                if let ::core::option::Option::Some(__laila_b) = __laila_r? {
                    #apply
                }
            });
        }
        quote_spanned!(span => {
            let __laila_try: ::laila::python::Result<()> = #body_ts;
            #[allow(clippy::needless_question_mark, unused_variables, clippy::redundant_closure_call)]
            let __laila_r: ::laila::python::Result<()> = match __laila_try {
                ::core::result::Result::Ok(()) => #ok_arm,
                ::core::result::Result::Err(__laila_exc) => #err_arm,
            };
            #fin
            __laila_r?;
        })
    }

    fn block_as_result_closure(&self, body: &Block) -> TokenStream {
        let inner_ctx = Ctx {
            ret: RetKind::Unit,
            in_closure: false,
            ..self.ctx.clone()
        };
        let g = Gen::new(&inner_ctx);
        let b = g.block(body, Some(quote!(::core::result::Result::Ok(()))));
        quote!((|| -> ::laila::python::Result<()> { #[allow(unreachable_code)] #b })())
    }

    pub fn cond(&self, e: &Expr) -> TokenStream {
        truthy(self.expr(e))
    }

    // ---- expressions ----

    pub fn expr(&self, e: &Expr) -> TokenStream {
        match e {
            Expr::Raw(ts) => ts.clone(),
            Expr::Lit(l) => quote!(#l),
            Expr::Ident(i) => self.ident(i),
            Expr::Path(p) => p.clone(),
            Expr::PyNone(sp) => quote_spanned!(*sp => ::laila::python::Arg::None),
            Expr::PyBool(b, sp) => quote_spanned!(*sp => #b),
            Expr::Attr { base, name } => self.attr(base, name),
            Expr::TupleField { base, index } => {
                let b = self.expr(base);
                quote!(#b.#index)
            }
            Expr::Call { callee, args, span } => self.call(callee, args, *span),
            Expr::MethodCall {
                recv,
                name,
                turbofish,
                args,
                span,
            } => self.method_call(recv, name, turbofish.as_ref(), args, *span),
            Expr::Index { base, index, span } => {
                let b = self.container_base(base);
                let k = self.index_key(index);
                wrap_try(quote_spanned!(*span => #b.__getitem__(#k)))
            }
            Expr::Unary { op, expr, span } => {
                let x = self.expr(expr);
                match op {
                    UnOp::Not => {
                        let t = truthy(x);
                        quote_spanned!(*span => !#t)
                    }
                    // `-len(x)`: `len` is a `usize`; negate as `i64`.
                    UnOp::Neg if contains_len_call(expr) => quote!(-((#x) as i64)),
                    // `-x` on an `Arg` yields a `Result` (`__neg__` may raise).
                    UnOp::Neg if !is_int_operand(expr) && !is_float_literal(expr) => {
                        wrap_try(quote!(-(#x)))
                    }
                    UnOp::Neg => quote!(-(#x)),
                    UnOp::Pos => quote!((#x)),
                    UnOp::Bang | UnOp::Invert => quote!(!(#x)),
                    UnOp::Deref => quote!(*(#x)),
                }
            }
            Expr::Binary {
                op,
                left,
                right,
                span,
            } => self.binary(*op, left, right, *span),
            // `a if c else b`: a Python value either way, so both branches
            // are `Arg`s (`1 if v is None else v + 1` mixes an int and an `Arg`).
            Expr::Ternary {
                cond,
                then,
                otherwise,
            } => {
                let c = self.cond(cond);
                // `d1 if c else d2`: a bound name is *shared* by the
                // expression (a handle clone), not moved out of the scope
                // (a closure would otherwise become `FnOnce`).
                let branch = |e: &Expr| match e {
                    Expr::Ident(i)
                        if i != "self"
                            && self.ctx.bound.contains(&i.to_string())
                            && !self.ctx.module_consts.contains(&i.to_string()) =>
                    {
                        self.arg_value(e)
                    }
                    _ => {
                        let x = self.expr(e);
                        quote!(::laila::python::IntoArg::into_arg(#x))
                    }
                };
                let a = branch(then);
                let b = branch(otherwise);
                quote!(if #c { #a } else { #b })
            }
            Expr::Lambda { params, body, span } => {
                let param_names: HashSet<String> = params.iter().map(|p| p.to_string()).collect();
                let mut bound = self.ctx.bound.clone();
                bound.extend(param_names.iter().cloned());
                let inner_ctx = Ctx {
                    in_closure: true,
                    in_async: false,
                    bound,
                    ..self.ctx.clone()
                };
                let g = Gen::new(&inner_ctx);
                // `lambda: e` hands out the captured handle: the closure keeps
                // its clone (a `move` of the capture would make it `FnOnce`).
                let b = match body.as_ref() {
                    Expr::Ident(i)
                        if i != "self"
                            && self.ctx.bound.contains(&i.to_string())
                            && !param_names.contains(&i.to_string()) =>
                    {
                        quote!(::core::clone::Clone::clone(&#i))
                    }
                    _ => g.expr(body),
                };
                let ps = params.iter().map(|p| quote!(#p: ::laila::python::Arg));
                // Python closures share enclosing bindings by reference: clone
                // every captured binding (handles are `Arc`s, so the closure
                // aliases the same object) before the `move`.
                let mut used = HashSet::new();
                super::ast::idents_in_expr(body, &mut used);
                let mut captured: Vec<&String> = used
                    .iter()
                    .filter(|n| {
                        self.ctx.bound.contains(*n) && !param_names.contains(*n) && *n != "self"
                    })
                    .collect();
                captured.sort();
                let clones = captured.iter().map(|n| {
                    let i = ident_maybe_raw(n, *span);
                    quote!(let #i = ::core::clone::Clone::clone(&#i);)
                });
                // `lambda: self.x`: the closure owns a clone of `self` (the
                // Python-shaped classes share their state through an `Arc`,
                // so the clone aliases the same object).
                let (self_clone, b) = if used.contains("self") {
                    let alias = Ident::new("__laila_self", *span);
                    (
                        quote!(let #alias = self.clone();),
                        rename_ident(b, "self", &alias),
                    )
                } else {
                    (quote!(), b)
                };
                quote_spanned!(*span => { #(#clones)* #self_clone ::laila::python::callable(move |#(#ps),*| -> ::laila::python::Result<::laila::python::Arg> {
                    ::core::result::Result::Ok(::laila::python::IntoArg::into_arg(#b))
                }) })
            }
            Expr::Closure { header, body } => {
                let inner_ctx = Ctx {
                    in_closure: true,
                    in_async: false,
                    ..self.ctx.clone()
                };
                let g = Gen::new(&inner_ctx);
                let has_result_ret = header.to_string().contains("Result");
                let b = match &**body {
                    Expr::Block(blk) if has_result_ret => {
                        let tail = if header.to_string().contains("Arg") {
                            quote!(::core::result::Result::Ok(::laila::python::Arg::None))
                        } else {
                            quote!(::core::result::Result::Ok(()))
                        };
                        let blk = g.block(blk, Some(tail));
                        quote!(#[allow(unreachable_code)] #blk)
                    }
                    other => g.expr(other),
                };
                quote!(#header #b)
            }
            // `await f` then `f.release()`: awaiting a bound name shares the
            // handle (`IntoFuture` consumes its receiver).
            Expr::Await(e, span) => {
                let x = self.arith_operand(e, false);
                wrap_try(
                    quote_spanned!(*span => ::laila::python::stdlib::asyncio::PyAwait::__laila_await(#x).await),
                )
            }
            Expr::Paren(e) => {
                let x = self.expr(e);
                quote!((#x))
            }
            Expr::Dict(g) => crate::literal::lower_brace(g, &|ts| self.rewrite_tokens(ts)),
            Expr::List(g) => crate::literal::lower_list(g, &|ts| self.rewrite_tokens(ts)),
            Expr::Tuple(g) => crate::literal::lower_tuple(g, &|ts| self.rewrite_tokens(ts)),
            Expr::Comprehension {
                kind,
                elem,
                value,
                pat,
                iter,
                cond,
                span,
            } => self.comprehension(
                kind,
                elem,
                value.as_deref(),
                pat,
                iter,
                cond.as_deref(),
                *span,
            ),
            Expr::FString(l) => self.fstring(l),
            Expr::Block(b) => self.value_block(b),
            Expr::If {
                cond,
                then,
                otherwise,
            } => {
                let c = self.cond(cond);
                let t = self.value_block(then);
                match otherwise {
                    Some(o) => {
                        let o = self.expr(o);
                        quote!(if #c #t else #o)
                    }
                    None => quote!(if #c #t),
                }
            }
            Expr::Match { scrutinee, arms } => {
                let s = self.expr(scrutinee);
                let arms = arms.iter().map(|(p, b)| {
                    let b = self.expr(b);
                    quote!(#p => #b,)
                });
                quote!(match #s { #(#arms)* })
            }
            Expr::Cast { expr, ty } => {
                let x = self.expr(expr);
                quote!((#x as #ty))
            }
            Expr::Try(e) => {
                let x = self.expr_no_try(e);
                quote!(#x?)
            }
            Expr::RustAwait(e) => {
                let x = self.expr(e);
                quote!((#x).await)
            }
            Expr::Ref { mutable, expr } => {
                let x = self.expr(expr);
                if *mutable {
                    quote!(&mut #x)
                } else {
                    quote!(&#x)
                }
            }
            Expr::Range {
                start,
                end,
                inclusive,
            } => {
                let s = start.as_ref().map(|s| self.expr(s));
                let e = end.as_ref().map(|e| self.expr(e));
                if *inclusive {
                    quote!(#s..=#e)
                } else {
                    quote!(#s..#e)
                }
            }
        }
    }

    /// Expression where the user wrote an explicit `?`: don't add `py_try`.
    fn expr_no_try(&self, e: &Expr) -> TokenStream {
        match e {
            Expr::Call { callee, args, .. } => {
                let c = self.callee(callee);
                let a = self.args_passthrough(args);
                quote!(#c(#a))
            }
            Expr::MethodCall {
                recv,
                name,
                turbofish,
                args,
                ..
            } => {
                let r = self.expr(recv);
                let a = self.args_passthrough(args);
                let tf = turbofish.as_ref().map(|t| quote!(::<#t>));
                quote!(#r.#name #tf(#a))
            }
            other => self.expr(other),
        }
    }

    /// Re-parse and lower a token stream (used for literal elements). A bare
    /// local inside a literal (`(small, large)`, `[x]`) is shared, not moved:
    /// Python puts a reference in the container.
    pub fn rewrite_tokens(&self, ts: TokenStream) -> TokenStream {
        let mut c = Cur::new(ts.clone());
        match parse_expr(&mut c) {
            Ok(Expr::Ident(i))
                if c.at_end() && i != "self" && self.ctx.bound.contains(&i.to_string()) =>
            {
                quote!(::core::clone::Clone::clone(&#i))
            }
            Ok(e) if c.at_end() => self.expr(&e),
            Ok(_) => ts,
            Err(err) => err.to_compile_error(),
        }
    }

    /// `for x in it`: iterating a named value borrows it (Python iteration
    /// never consumes the iterable); temporaries are passed by value.
    fn iter_operand(&self, e: &Expr) -> TokenStream {
        match e {
            // `for s in EntryState`: iterating an enum *class* walks its
            // members (`Enum::MEMBERS`, the port's `__members__` order).
            Expr::Ident(i) if ident_is_camel(i) && !self.ctx.bound.contains(&i.to_string()) => {
                quote!(&#i::MEMBERS[..])
            }
            Expr::Ident(_) | Expr::Attr { .. } | Expr::Index { .. } => {
                let x = self.expr(e);
                quote!(&(#x))
            }
            _ => self.expr(e),
        }
    }

    /// Operand of `+ - *`: string literals become `Arg` so `"ab" * 2` and
    /// `1 + "x"` compile (and raise) like Python.
    fn arith_operand(&self, e: &Expr, lift_literals: bool) -> TokenStream {
        match e {
            Expr::Lit(l)
                if l.to_string().starts_with('"')
                    || l.to_string().starts_with("b\"")
                    || (lift_literals && is_dynamic_scalar_literal(e)) =>
            {
                quote_spanned!(l.span() => ::laila::python::IntoArg::into_arg(#l))
            }
            Expr::FString(_) => {
                let s = self.expr(e);
                quote!(::laila::python::IntoArg::into_arg(#s))
            }
            // A local operand is used, not consumed (`small * 8` then
            // `(small, large)`): arithmetic on a clone (handles are `Arc`s).
            Expr::Ident(i) if i != "self" && self.ctx.bound.contains(&i.to_string()) => {
                quote!(::core::clone::Clone::clone(&#i))
            }
            _ => self.expr(e),
        }
    }

    fn attr(&self, base: &Expr, name: &Ident) -> TokenStream {
        // `laila.logger`: the package property returning the `Logger`
        // singleton (the `laila.logger` *module* is only ever a path prefix).
        if name == "logger"
            && matches!(base, Expr::Ident(i) if i == "laila" && !self.ctx.bound.contains("laila"))
        {
            return wrap_try(quote!(::laila::logger::get_logger(
                ::laila::python::Args::new()
            )));
        }
        if let Some(p) = as_path(&Expr::Attr {
            base: Box::new(base.clone()),
            name: name.clone(),
        }) {
            // `Cls.model_fields` / `Cls.__name__`: a lowercase attribute on a
            // class path is an associated function in Rust, so read it by
            // calling it; `Enum.VARIANT` / `Cls.CONST` stay paths.
            if is_lower_attr(name) {
                if let Some(bp) = as_path(base) {
                    // `Enum.VARIANT.value` / `.name`: a method of the member.
                    if last_segment_is_screaming(&bp) {
                        return quote!(#bp.#name());
                    }
                    return wrap_try(quote!(#p()));
                }
            }
            return p;
        }
        // `laila.entry.transformation_base64`: a module constant (P11) --
        // a value-returning function in Rust, read by calling it.
        // Any other module-level function used as a value -> a Python callable.
        if let Some(m) = as_submodule_path(base) {
            if MODULE_CONSTANTS.contains(&name.to_string().as_str()) {
                return quote!(#m::#name());
            }
            return quote!(::laila::python::callable(#m::#name));
        }
        let n = name.to_string();
        if is_self(base) {
            if self.ctx.impl_methods.contains(&n) {
                if self.ctx.dynamic_self && !TESTCASE_METHODS.contains(&n.as_str()) {
                    // A class attribute (`n: int = 0` -> `fn n(&self) -> Arg`)
                    // shadowed by an instance attribute of the same name:
                    // Python reads the instance first, then the class.
                    let w = wrap_try(quote!(self.#name()));
                    return quote!(match self.__getattr__(#n) {
                        ::core::result::Result::Ok(__laila_v) => __laila_v,
                        ::core::result::Result::Err(_) => ::laila::python::IntoArg::into_arg(#w),
                    });
                }
                return wrap_try(quote!(self.#name()));
            }
            if self.ctx.dynamic_self {
                return wrap_try(quote!(self.__getattr__(#n)));
            }
            return quote!(self.#name);
        }
        if self.is_dyn_obj(base) {
            let b = self.expr(base);
            return wrap_try(quote!(#b.__getattr__(#n)));
        }
        // `self.proxy.central.memory.route`: an attribute of a value that
        // is certainly an `Arg` whose name `Arg` has no zero-argument
        // forwarder for (none at all, or a method taking arguments) is a
        // dynamic attribute read.
        // A name that is only a `__call_method__` forwarder (`arg_methods0!`:
        // `proxy.neg()`) read without parentheses (`target.neg`) is a data
        // attribute of the dynamic object, not a call.
        if self.is_arg_receiver(base)
            && (!matches!(arg_method_arity(&n), Some(0))
                || (ZERO_ARG_METHODS.contains(&n.as_str())
                    && !ARG_ATTR_NAMES.contains(&n.as_str())))
        {
            let b = self.expr(base);
            return wrap_try(quote!(#b.__getattr__(#n)));
        }
        let b = self.expr(base);
        // An attribute whose name is also a container method (`pool.index`
        // vs `list.index(x)`) cannot be a zero-argument method on `Arg`: it
        // is read through `__getattr__` on every receiver.
        if CONTAINER_METHOD_ATTRS.contains(&n.as_str()) || DYN_ATTRS.contains(&n.as_str()) {
            return wrap_try(quote!(#b.__getattr__(#n)));
        }
        // A `CONSTANT`-case attribute on a value (`secrets.AWS_REGION` on
        // `laila.args`): no Rust method is spelled that way, so it is a
        // dynamic attribute read. (A stdlib shim's constant -- `uuid.NAMESPACE_DNS`
        // -- is a typed method of the module value.)
        let stdlib_base = matches!(base, Expr::Ident(i)
            if STDLIB_MODULES.contains(&i.to_string().as_str()) && !self.ctx.bound.contains(&i.to_string()));
        if is_const_case(&n) && !stdlib_base {
            return wrap_try(quote!(#b.__getattr__(#n)));
        }
        // `real = mem.remember`: a method taking `Args` read as a value is
        // the bound method (a module's function of that name is a path).
        let laila_base =
            matches!(base, Expr::Ident(i) if i == "laila" && !self.ctx.bound.contains("laila"));
        if arg_method_arity(&n) == Some(-1)
            && !stdlib_base
            && !laila_base
            && as_path(base).is_none()
        {
            return wrap_try(quote!(#b.__getattr__(#n)));
        }
        wrap_try(quote!(#b.#name()))
    }

    /// The base of a subscript. A bare `CONSTANT`-case name imported from a
    /// laila module (`LAILA_DEFAULT_DIRECTORIES["pools"]`) is a module
    /// constant the Rust side exposes as a function, a `static` or a
    /// `const`: `module_value(&NAME)` reads it as a value either way.
    /// An operand that is (almost certainly) an `Arg` at runtime: a known
    /// `Arg` local, or an attribute / item / method result reached through
    /// a value (`up.peer.messages_with_drop`, `d["n"]`, `q.qsize()`).
    fn is_dynamic_operand(&self, e: &Expr) -> bool {
        self.is_arg_receiver(e)
            || matches!(
                e,
                Expr::Attr { .. } | Expr::Index { .. } | Expr::MethodCall { .. }
            )
    }

    fn container_base(&self, base: &Expr) -> TokenStream {
        if let Expr::Ident(i) = base {
            if self.is_module_value_const(i) {
                return quote_spanned!(i.span() => ::laila::python::builtins::module_value(&#i));
            }
        }
        self.expr(base)
    }

    /// A `CONSTANT`-case name imported from a laila module and used as a
    /// value (`LAILA_DEFAULT_DIRECTORIES`): not a local, not a module
    /// constant of this block, and not one of laila's `_LAILA_*` base
    /// classes (class objects, not values).
    fn is_module_value_const(&self, i: &Ident) -> bool {
        let n = i.to_string();
        is_const_case(&n)
            && !n.starts_with("_LAILA_")
            && !STDLIB_MODULES.contains(&n.as_str())
            && !self.ctx.bound.contains(&n)
            && !self.ctx.module_consts.contains(&n)
    }

    /// Is `e` certainly an `Arg`: a dynamic attribute of `self` in a
    /// `py_class!` method (`self.proxy`), or an item reached through one
    /// (`self.peers[gid]`)?
    fn is_arg_receiver(&self, e: &Expr) -> bool {
        match e {
            // a dynamic-scalar local (`p = None; p = f()`) starts life as an `Arg`
            Expr::Ident(i) => {
                self.ctx.arg_locals.contains(&i.to_string())
                    || self.ctx.dynamic_vars.contains(&i.to_string())
            }
            Expr::Attr { base, name } if is_self(base) => {
                let n = name.to_string();
                self.ctx.dynamic_self
                    && !self.ctx.impl_methods.contains(&n)
                    && !TESTCASE_METHODS.contains(&n.as_str())
                    && !self.is_python_defined_method(&n)
            }
            // an attribute of an `Arg` is an `Arg`
            Expr::Attr { base, .. } => self.is_arg_receiver(base),
            Expr::Index { base, .. } => self.is_arg_receiver(base) || self.is_dyn_obj(base),
            _ => false,
        }
    }

    /// Is `e` an attribute-dynamic object (a `DotMap` local or any attribute /
    /// item reached through one)?
    fn is_dyn_obj(&self, e: &Expr) -> bool {
        match e {
            Expr::Ident(i) => self.ctx.dyn_objs.contains(&i.to_string()),
            Expr::Attr { base, .. } => {
                is_dyn_obj_value(e, &self.ctx.dyn_classes) || self.is_dyn_obj(base)
            }
            Expr::Index { base, .. } => self.is_dyn_obj(base),
            other => is_dyn_obj_value(other, &self.ctx.dyn_classes),
        }
    }

    fn index_key(&self, k: &IndexKind) -> TokenStream {
        match k {
            IndexKind::Expr(e) => self.arg_pass(e),
            IndexKind::Slice { start, stop, step } => {
                let opt = |x: &Option<Box<Expr>>| match x {
                    Some(e) => {
                        let e = self.expr(e);
                        quote!(::laila::python::object::SliceBound::__laila_slice_bound(&(#e)))
                    }
                    None => quote!(::core::option::Option::None),
                };
                let (s, e, st) = (opt(start), opt(stop), opt(step));
                quote!(::laila::python::Slice { start: #s, stop: #e, step: #st })
            }
        }
    }

    /// `isinstance(x, T)` where `T` is a class *value* computed at runtime
    /// (`y.__class__`, `type(y)`, or a tuple of those) rather than a type
    /// name: it cannot be a generic argument and goes through
    /// `isinstance_value` instead.
    fn is_class_value(&self, e: &Expr) -> bool {
        match e {
            Expr::Attr { name, .. } => name == "__class__",
            Expr::Call { callee, .. } => matches!(&**callee, Expr::Ident(i) if i == "type"),
            // a local holding a class (`cls` of `for cls in classes`, a
            // tuple of classes) or an item of a container of classes
            Expr::Ident(i) => i != "self" && self.ctx.bound.contains(&i.to_string()),
            Expr::Index { .. } => true,
            Expr::Tuple(g) => crate::literal::split_commas(g.stream()).iter().any(|tts| {
                let mut c = Cur::from_tts(tts.clone());
                parse_expr(&mut c).is_ok_and(|e| self.is_class_value(&e))
            }),
            _ => false,
        }
    }

    fn type_arg(&self, e: &Expr) -> TokenStream {
        match e {
            Expr::Tuple(g) => {
                let elems: Vec<TokenStream> = crate::literal::split_commas(g.stream())
                    .iter()
                    .map(|tts| {
                        let mut c = Cur::from_tts(tts.clone());
                        match parse_expr(&mut c) {
                            Ok(e) => {
                                let t = self.type_arg(&e);
                                quote!(&#t)
                            }
                            Err(er) => er.to_compile_error(),
                        }
                    })
                    .collect();
                quote!((#(#elems,)*))
            }
            Expr::Ident(i) => quote!(#i),
            // `uuid.UUID`, `datetime.datetime`: a class of a stdlib shim module
            // is a type in `laila::python::stdlib`.
            Expr::Attr { base, name } if matches!(&**base, Expr::Ident(m) if STDLIB_MODULES.contains(&m.to_string().as_str()) && !self.ctx.bound.contains(&m.to_string())) =>
            {
                quote!(::laila::python::stdlib::#name)
            }
            other => as_path(other).unwrap_or_else(|| self.expr(other)),
        }
    }

    // ---- calls ----

    fn callee(&self, callee: &Expr) -> TokenStream {
        match as_path(callee) {
            Some(p) => p,
            None => self.expr(callee),
        }
    }

    fn arg_pass(&self, e: &Expr) -> TokenStream {
        match e {
            // `self.assertIs(type(pool), SQLitePool)`: a bare class name is
            // the class object (as in `arg_value`).
            Expr::Ident(i)
                if ident_is_camel(i)
                    && !self.ctx.bound.contains(&i.to_string())
                    && !self.ctx.module_consts.contains(&i.to_string())
                    && !CAMEL_VALUES.contains(&i.to_string().as_str()) =>
            {
                quote_spanned!(i.span() => &::laila::python::builtins::class_value::<#i>())
            }
            Expr::Ident(i) if i != "self" => {
                let x = self.ident(i);
                quote!(&#x)
            }
            // `helper.check(self)` from a dynamic class: the instance as a
            // value (a handle alias) -- a Python-defined parameter is `Arg`.
            Expr::Ident(i) if self.ctx.dynamic_self => {
                quote!(::laila::python::IntoArg::into_arg(#i))
            }
            Expr::Ident(i) => quote!(#i),
            other => self.expr(other),
        }
    }

    fn args_passthrough(&self, args: &[CallArg]) -> TokenStream {
        let items: Vec<TokenStream> = args
            .iter()
            .map(|a| match a {
                CallArg::Pos(e) => self.arg_pass(e),
                CallArg::Kw(_, e) | CallArg::Star(e) | CallArg::DoubleStar(e) => self.expr(e),
            })
            .collect();
        quote!(#(#items),*)
    }

    fn args_bag(&self, args: &[CallArg]) -> TokenStream {
        let mut stmts = Vec::new();
        for a in args {
            match a {
                CallArg::Pos(e) => {
                    let v = self.arg_value(e);
                    stmts.push(quote!(__laila_a.push_positional(#v);));
                }
                CallArg::Kw(k, e) => {
                    let key = k.to_string();
                    let key = key.strip_prefix("r#").unwrap_or(&key).to_string();
                    let v = self.arg_value(e);
                    stmts.push(quote!(__laila_a.push_keyword(#key, #v);));
                }
                CallArg::Star(e) => {
                    let v = self.expr(e);
                    let w = wrap_try(quote!(__laila_a.extend_positional(#v)));
                    stmts.push(quote!(#w;));
                }
                CallArg::DoubleStar(e) => {
                    let v = self.expr(e);
                    let w = wrap_try(quote!(__laila_a.extend_keywords(#v)));
                    stmts.push(quote!(#w;));
                }
            }
        }
        quote!({ let mut __laila_a = ::laila::python::Args::new(); #(#stmts)* __laila_a })
    }

    /// An argument as an `Arg` value: identifiers shared by reference, literals by value.
    fn arg_value(&self, e: &Expr) -> TokenStream {
        match e {
            // A bare class name (`patch.object(FilesystemPool, ..)`): the
            // class object. `NotImplemented` / `Ellipsis` are values.
            Expr::Ident(i)
                if ident_is_camel(i)
                    && !self.ctx.bound.contains(&i.to_string())
                    && !self.ctx.module_consts.contains(&i.to_string())
                    && !CAMEL_VALUES.contains(&i.to_string().as_str()) =>
            {
                quote_spanned!(i.span() => ::laila::python::builtins::class_value::<#i>())
            }
            // `dict(LAILA_DEFAULT_DIRECTORIES)`: an imported module constant
            // passed as a value.
            Expr::Ident(i) if self.is_module_value_const(i) => {
                quote_spanned!(i.span() => ::laila::python::builtins::module_value(&#i))
            }
            Expr::Ident(i) if i != "self" => {
                let x = self.ident(i);
                quote!(::laila::python::IntoArg::into_arg(&#x))
            }
            Expr::Dict(_)
            | Expr::List(_)
            | Expr::Tuple(_)
            | Expr::PyNone(_)
            | Expr::Comprehension { .. } => self.expr(e),
            other => {
                let x = self.expr(other);
                quote!(::laila::python::IntoArg::into_arg(#x))
            }
        }
    }

    fn takes_args(&self, name: &str, args: &[CallArg]) -> bool {
        has_kw_or_splat(args) || takes_args_name(name) || self.ctx.pysig_methods.contains(name)
    }

    /// [`args_values`](Self::args_values) for a named local function: a
    /// parameter declared with a plain Rust type (`n: i64`) takes the
    /// lowered expression itself.
    fn args_values_for(&self, name: &str, args: &[CallArg]) -> TokenStream {
        let Some(raw) = self.ctx.raw_params.get(name) else {
            return self.args_values(args);
        };
        let items: Vec<TokenStream> = args
            .iter()
            .enumerate()
            .map(|(k, a)| match a {
                CallArg::Pos(e) => match raw.get(k).copied().unwrap_or(RawParam::Dyn) {
                    // `_blob(n, i)` with `n: i64`: a dynamic argument (an
                    // `Arg` from a tuple unpack) converts to the declared type.
                    RawParam::Plain => {
                        // a local argument is shared, not consumed (`_blob(n, n)`)
                        let x = match e {
                            Expr::Ident(i)
                                if i != "self" && self.ctx.bound.contains(&i.to_string()) =>
                            {
                                let x = self.ident(i);
                                quote!(::core::clone::Clone::clone(&#x))
                            }
                            _ => self.expr(e),
                        };
                        wrap_try(quote!(::laila::python::object::RetInto::ret_into(#x)))
                    }
                    RawParam::Str => {
                        let x = self.expr(e);
                        quote!(::laila::python::__rt::ToString::to_string(&(#x)))
                    }
                    RawParam::Dyn => self.arg_value(e),
                },
                CallArg::Kw(k, _) => {
                    let msg = format!(
                        "laila::python: keyword argument `{k}` to a positional-only function"
                    );
                    quote!(compile_error!(#msg))
                }
                CallArg::Star(_) | CallArg::DoubleStar(_) => {
                    quote!(compile_error!(
                        "laila::python: `*args` / `**kwargs` to a positional-only function"
                    ))
                }
            })
            .collect();
        quote!(#(#items),*)
    }

    /// Arguments to a Python-defined function (`def f(self, e)` -> `fn f(&self, e: Arg)`):
    /// every positional is an owned `Arg` (identifiers shared, Python-style).
    fn args_values(&self, args: &[CallArg]) -> TokenStream {
        let items: Vec<TokenStream> = args
            .iter()
            .map(|a| match a {
                CallArg::Pos(e) => self.arg_value(e),
                CallArg::Kw(k, _) => {
                    let msg =
                        format!("laila::python: keyword argument `{k}` needs a `#[pysig]` callee");
                    quote!(compile_error!(#msg))
                }
                CallArg::Star(_) | CallArg::DoubleStar(_) => quote!(compile_error!(
                    "laila::python: `*args`/`**kwargs` need a `#[pysig]` callee"
                )),
            })
            .collect();
        quote!(#(#items),*)
    }

    /// A function/method written in Python (so its parameters are `Arg`).
    fn is_python_defined_fn(&self, name: &str) -> bool {
        self.ctx.local_fns.contains(name) && !BUILTIN_FNS.contains(&name) && !takes_args_name(name)
    }
    fn is_python_defined_method(&self, name: &str) -> bool {
        self.ctx.impl_methods.contains(name)
            && !TESTCASE_METHODS.contains(&name)
            && !name.starts_with("__")
    }

    fn lower_args_for(&self, name: &str, args: &[CallArg]) -> TokenStream {
        if REF_ARG.contains(&name) && args.len() == 1 {
            if let CallArg::Pos(e) = &args[0] {
                let x = self.expr(e);
                return quote!(&(#x));
            }
        }
        if self.takes_args(name, args) {
            self.args_bag(args)
        } else {
            self.args_passthrough(args)
        }
    }

    fn call(&self, callee: &Expr, args: &[CallArg], span: Span) -> TokenStream {
        if let Expr::Ident(i) = callee {
            // `r#type(x)`: the raw spelling of a builtin that is a Rust keyword.
            let n = i.to_string();
            let n = n.strip_prefix("r#").unwrap_or(&n).to_string();
            match n.as_str() {
                "isinstance" => {
                    if let [CallArg::Pos(x), CallArg::Pos(t)] = args {
                        // `x` may be a typed object (`DotMap`, `Entry`) or an `Arg`:
                        // both share through `IntoArg` (an `Arc` clone).
                        let x = self.arg_value(x);
                        if self.is_class_value(t) {
                            let t = self.arg_value(t);
                            return quote_spanned!(span => ::laila::py_try!(::laila::python::pyclass::isinstance_value(&(#x), &(#t))));
                        }
                        let t = self.type_arg(t);
                        return quote_spanned!(span => ::laila::python::builtins::isinstance::<#t>(&(#x)));
                    }
                    return quote_spanned!(span => compile_error!("isinstance(x, T) takes two arguments"));
                }
                // `assert cond, msg` raises `AssertionError` (catchable, and
                // carried to a remote caller by the RPC layer) -- not a panic.
                "assert" => {
                    return match args {
                        [CallArg::Pos(c)] => {
                            let c = self.cond(c);
                            // A bare `assert` reports its source location (the
                            // generated file's line is what a reader has).
                            quote_spanned!(span => if !(#c) {
                                return ::core::result::Result::Err(::laila::python::LailaError::new(
                                    ::laila::python::ExcKind::AssertionError,
                                    ::core::concat!("assert failed at ", ::core::file!(), ":", ::core::line!())));
                            })
                        }
                        [CallArg::Pos(c), CallArg::Pos(m)] => {
                            let c = self.cond(c);
                            let m = self.expr(m);
                            quote_spanned!(span => if !(#c) {
                                return ::core::result::Result::Err(::laila::python::LailaError::new(
                                    ::laila::python::ExcKind::AssertionError,
                                    ::laila::python::builtins::str(#m)));
                            })
                        }
                        _ => quote_spanned!(span => compile_error!("assert(cond[, msg])")),
                    };
                }
                "hasattr" => {
                    if let [CallArg::Pos(x), CallArg::Pos(name)] = args {
                        let x = self.expr(x);
                        let name = self.expr(name);
                        return quote_spanned!(span => ::laila::python::builtins::hasattr(&(#x), #name));
                    }
                }
                // `issubclass(A, B)`: both arguments are classes (types in Rust).
                "issubclass" => {
                    if let [CallArg::Pos(a), CallArg::Pos(b)] = args {
                        let a = self.type_arg(a);
                        let b = self.type_arg(b);
                        return quote_spanned!(span => ::laila::python::builtins::issubclass::<#a, #b>());
                    }
                    return quote_spanned!(span => compile_error!("issubclass(A, B) takes two arguments"));
                }
                // Python's `callable(x)` is a predicate; the Tier 1 wrapper
                // of a Rust closure is reached as `::laila::python::callable`.
                // `object()`: a fresh sentinel (identity-only).
                "object" if args.is_empty() && !self.ctx.bound.contains("object") => {
                    return quote_spanned!(span => ::laila::python::builtins::object_sentinel());
                }
                "callable" => {
                    if let [CallArg::Pos(x)] = args {
                        let x = self.expr(x);
                        return quote_spanned!(span => ::laila::python::builtins::is_callable(&(#x)));
                    }
                }
                "super" => {
                    return quote_spanned!(span => compile_error!("laila::python: `super().m(..)` is lowered by py2rs to the class-qualified base method `self.<Base>__m(..)` (rule P12d)"));
                }
                // `bytes(x)`: the builtin function (`bytes` the name is the
                // `isinstance` marker type; the function is `bytes_`).
                "bytes" if !self.ctx.bound.contains("bytes") => {
                    let a = self.args_bag(args);
                    return wrap_try(quote_spanned!(span => ::laila::python::builtins::bytes_(#a)));
                }
                _ => {}
            }
            if ident_is_camel(i) {
                let a = self.args_bag(args);
                return wrap_try(quote_spanned!(span => #i::new(#a)));
            }
            // A nested `def` lowered to a plain closure: owned `Arg` parameters.
            if self.ctx.closure_fns.contains(&n) {
                let a = self.args_values(args);
                return wrap_try(quote_spanned!(span => #i(#a)));
            }
            if self.is_python_defined_fn(&n) && !self.takes_args(&n, args) {
                let a = self.args_values_for(&n, args);
                return wrap_try(quote_spanned!(span => #i(#a)));
            }
            // A local binding (`def log(tag)` lowered to a Python callable
            // because it escapes) shadows a laila `Args`-taking function of
            // the same name (`logger.log`): it is called as a value below.
            let shadowed_by_local = self.ctx.bound.contains(&n) && !self.ctx.local_fns.contains(&n);
            if BUILTIN_FNS.contains(&n.as_str())
                || self.ctx.local_fns.contains(&n)
                || (takes_args_name(&n) && !shadowed_by_local)
            {
                let a = self.lower_args_for(&n, args);
                return wrap_try(quote_spanned!(span => #i(#a)));
            }
            // A callable value (lambda, bound method, `Arg::Callable`, or a
            // lower-case stdlib class object such as `datetime(..)`).
            let a = self.args_bag(args);
            let callee = self.ident(i);
            return wrap_try(quote_spanned!(span => ::laila::python::builtins::call(&#callee, #a)));
        }
        // `Cls[T](..)`: a subscripted generic class (`AtomicList[int]()`) --
        // Rust has no twin for the type parameters, the constructor is `Cls::new`.
        if let Expr::Index { base, .. } = callee {
            if let Some(p) = as_path(base) {
                if last_segment_is_camel(&p) {
                    let a = self.args_bag(args);
                    return wrap_try(quote_spanned!(span => #p::new(#a)));
                }
            }
            // `callbacks[status](x)`: the subscript yields a callable value.
            let c = self.expr(callee);
            let a = self.args_bag(args);
            return wrap_try(quote_spanned!(span => ::laila::python::builtins::call(&(#c), #a)));
        }
        // `ensure_list("items")(original)`: a decorator factory's result is a
        // callable value applied right away.
        if let Expr::Call { .. } = callee {
            let c = self.expr(callee);
            let a = self.args_bag(args);
            return wrap_try(
                quote_spanned!(span => ::laila::python::builtins::call(&::laila::python::IntoArg::into_arg(#c), #a)),
            );
        }
        if let Some(p) = as_path(callee) {
            if last_segment_is_camel(&p) {
                let a = self.args_bag(args);
                return wrap_try(quote_spanned!(span => #p::new(#a)));
            }
            let last = p
                .clone()
                .into_iter()
                .filter_map(|t| match t {
                    TokenTree::Ident(i) => Some(i.to_string()),
                    _ => None,
                })
                .last()
                .unwrap_or_default();
            let a = self.lower_args_for(&last, args);
            return wrap_try(quote_spanned!(span => #p(#a)));
        }
        let c = self.expr(callee);
        let a = self.args_passthrough(args);
        wrap_try(quote_spanned!(span => (#c)(#a)))
    }

    fn method_call(
        &self,
        recv: &Expr,
        name: &Ident,
        turbofish: Option<&TokenStream>,
        args: &[CallArg],
        span: Span,
    ) -> TokenStream {
        let n = name.to_string();
        let tf = turbofish.map(|t| quote!(::<#t>));
        // `patch.object(Cls, "attr", ..)`: `unittest.mock.patch` is both the
        // function and the namespace of `object`.
        if n == "object"
            && matches!(recv, Expr::Ident(i) if i == "patch" && !self.ctx.bound.contains("patch"))
        {
            let a = self.args_bag(args);
            return wrap_try(quote_spanned!(span => ::laila::python::unittest::patch::object(#a)));
        }
        // `object.__setattr__(x, "name", v)` / `object.__getattribute__(x, "name")`:
        // the base-class slots that bypass an overridden `__setattr__`.
        if matches!(recv, Expr::Ident(i) if i == "object" && !self.ctx.bound.contains("object"))
            && n.starts_with("__")
        {
            let f = Ident::new(&format!("object{n}"), name.span());
            let a = self.args_bag(args);
            return wrap_try(quote_spanned!(span => ::laila::python::builtins::#f(#a)));
        }
        // `self.assertX(.., msg=m)`: unittest's `longMessage` -- the message
        // is appended to the failure text (`assert_with_msg`).
        if is_self(recv) && TESTCASE_METHODS.contains(&n.as_str()) {
            if let Some(pos) = args
                .iter()
                .position(|a| matches!(a, CallArg::Kw(k, _) if k == "msg"))
            {
                let mut rest: Vec<CallArg> = args.to_vec();
                let msg_expr = match rest.remove(pos) {
                    CallArg::Kw(_, e) => e,
                    _ => unreachable!(),
                };
                let m = self.arg_value(&msg_expr);
                let raw = if TYPE_ARG.contains(&n.as_str()) {
                    match rest.as_slice() {
                        [CallArg::Pos(x), CallArg::Pos(t)] if self.is_class_value(t) => {
                            let x = self.arg_pass(x);
                            let t = self.arg_value(t);
                            let v = Ident::new(&format!("{n}_value"), name.span());
                            quote_spanned!(span => self.#v(#x, #t))
                        }
                        [CallArg::Pos(x), CallArg::Pos(t)] => {
                            let x = self.arg_pass(x);
                            let t = self.type_arg(t);
                            quote_spanned!(span => self.#name::<#t>(#x))
                        }
                        _ => {
                            let a = self.lower_args_for(&n, &rest);
                            quote_spanned!(span => self.#name(#a))
                        }
                    }
                } else {
                    let a = self.lower_args_for(&n, &rest);
                    quote_spanned!(span => self.#name(#a))
                };
                return quote_spanned!(span => ::laila::python::unittest::assert_with_msg(::laila::py_try!(#raw), #m)?);
            }
        }
        // `asyncio.run(coro)`: the coroutine may be a typed Rust future
        // (`Entry._build_async(..)` -> `Result<Entry>`) as well as a
        // coroutine object -- `run_value` drives either and yields an `Arg`.
        if n == "run"
            && matches!(recv, Expr::Ident(i) if i == "asyncio" && !self.ctx.bound.contains("asyncio"))
        {
            if let [CallArg::Pos(e)] = args {
                let x = self.expr(e);
                return wrap_try(
                    quote_spanned!(span => ::laila::python::stdlib::asyncio::run_value(#x)),
                );
            }
        }
        // `datetime.datetime(2026, 1, 1)` / `datetime.timedelta(days=1)`:
        // the module's class called with arguments constructs (the
        // argument-less `datetime.datetime` is the class accessor).
        if matches!(n.as_str(), "datetime" | "timedelta" | "timezone")
            && !args.is_empty()
            && matches!(recv, Expr::Ident(i) if i == "datetime" && !self.ctx.bound.contains("datetime"))
        {
            let a = self.args_bag(args);
            return wrap_try(quote_spanned!(span => ::laila::python::stdlib::#name::__new__(#a)));
        }
        // `LAILA_DEFAULT_DIRECTORIES.update(..)`: a method on an imported
        // module constant (a module-level dict) -- read it as a value.
        if let Expr::Ident(i) = recv {
            if self.is_module_value_const(i) {
                if ZERO_ARG_METHODS.contains(&n.as_str()) && !args.is_empty() {
                    let a = self.args_bag(args);
                    return wrap_try(
                        quote_spanned!(span => ::laila::python::builtins::module_value(&#i).__call_method__(#n, #a)),
                    );
                }
                let a = self.lower_args_for(&n, args);
                return wrap_try(
                    quote_spanned!(span => ::laila::python::builtins::module_value(&#i).#name #tf(#a)),
                );
            }
        }
        // Type::method(...) -- classmethods, enum constructors, CamelCase paths.
        if let Some(p) = as_path(recv) {
            // `Cls.method(obj, ..)` on a bare instance (`obj = Cls.__new__(Cls)`):
            // Python's unbound call binds `obj` as `self` -- `obj.method(..)`.
            if let Some(CallArg::Pos(Expr::Ident(first))) = args.first() {
                if self.ctx.bare_instances.contains(&first.to_string()) {
                    let rest = &args[1..];
                    let a = self.lower_args_for(&n, rest);
                    return wrap_try(quote_spanned!(span => #first.#name #tf(#a)));
                }
            }
            if ident_is_camel(name) {
                let a = self.args_bag(args);
                // `Outer._Inner(..)`: a private nested helper class, exposed by
                // the Rust type as an associated constructor function.
                if n.starts_with('_') {
                    return wrap_try(quote_spanned!(span => #p::#name(#a)));
                }
                return wrap_try(quote_spanned!(span => #p::#name::new(#a)));
            }
            let a = self.lower_args_for(&n, args);
            return wrap_try(quote_spanned!(span => #p::#name #tf(#a)));
        }
        // `laila.Entry(..)` / `laila.data.S3Pool(..)`: a class on a module path.
        if ident_is_camel(name) {
            if let Some(m) = as_module_path(recv) {
                let a = self.args_bag(args);
                return wrap_try(quote_spanned!(span => #m::#name::new(#a)));
            }
        }
        // `laila.entry.transformation_base64(..)`: a function on a submodule path.
        if let Some(m) = as_submodule_path(recv) {
            let a = self.lower_args_for(&n, args);
            return wrap_try(quote_spanned!(span => #m::#name #tf(#a)));
        }
        if TYPE_ARG.contains(&n.as_str()) {
            // `self.assertIsInstance(x, cls, msg)` with a class value
            if let [CallArg::Pos(x), CallArg::Pos(t), CallArg::Pos(m)] = args {
                if self.is_class_value(t) && n != "isinstance" {
                    let r = self.expr(recv);
                    let x = self.arg_pass(x);
                    let t = self.arg_value(t);
                    let m = self.arg_value(m);
                    let v = Ident::new(&format!("{n}_value_msg"), name.span());
                    return wrap_try(quote_spanned!(span => #r.#v(#x, #t, #m)));
                }
            }
            if let [CallArg::Pos(x), CallArg::Pos(t)] = args {
                let r = self.expr(recv);
                let x = self.arg_pass(x);
                if self.is_class_value(t) {
                    let t = self.arg_value(t);
                    let v = Ident::new(&format!("{n}_value"), name.span());
                    return wrap_try(quote_spanned!(span => #r.#v(#x, #t)));
                }
                let t = self.type_arg(t);
                return wrap_try(quote_spanned!(span => #r.#name::<#t>(#x)));
            }
        }
        if is_self(recv) {
            // `self.addCleanup(f, *args, **kwargs)`: the bound call is
            // deferred as a zero-argument callable (`self` cloned into it).
            if n == "addCleanup" && !self.ctx.impl_methods.contains("addCleanup") {
                if let Some((CallArg::Pos(f), rest)) = args.split_first() {
                    let call = match f {
                        // a bound method (`laila.activate_policy`)
                        Expr::Attr { base, name } => Expr::MethodCall {
                            recv: base.clone(),
                            name: name.clone(),
                            turbofish: None,
                            args: rest.to_vec(),
                            span,
                        },
                        other => Expr::Call {
                            callee: Box::new(other.clone()),
                            args: rest.to_vec(),
                            span,
                        },
                    };
                    let body = rename_self(self.expr(&call));
                    return wrap_try(
                        quote_spanned!(span => self.addCleanup(::laila::python::args!(
                            {
                                let __laila_self = self.clone();
                                ::laila::python::callable(move || -> ::laila::python::Result<::laila::python::Arg> {
                                    ::laila::python::IntoArg::into_arg(#body);
                                    ::core::result::Result::Ok(::laila::python::Arg::None)
                                })
                            }
                        ))),
                    );
                }
            }
            // `self.assertX(a, b, msg)`: the optional trailing `msg` selects
            // the `assertX_msg` twin (`"<standard> : <msg>"` on failure).
            if let Some(arity) = ASSERT_ARITY
                .iter()
                .find(|(m, _)| *m == n.as_str())
                .map(|(_, k)| *k)
            {
                if args.len() == arity + 1 && args.iter().all(|a| matches!(a, CallArg::Pos(_))) {
                    let with_msg = Ident::new(&format!("{n}_msg"), name.span());
                    let a = self.args_passthrough(args);
                    return wrap_try(quote_spanned!(span => self.#with_msg(#a)));
                }
            }
            // `self.factory(**kw)`: a class attribute holding a class /
            // callable is called, not the getter.
            if self.ctx.class_attrs.contains(&n) {
                let a = self.args_bag(args);
                let getter = wrap_try(quote!(self.#name()));
                return wrap_try(quote_spanned!(span => #getter.__call__(#a)));
            }
            if self.is_python_defined_method(&n) && !self.takes_args(&n, args) {
                let a = self.args_values_for(&n, args);
                return wrap_try(quote_spanned!(span => self.#name #tf(#a)));
            }
            if self.ctx.impl_methods.contains(&n) || !self.ctx.dynamic_self {
                let a = self.lower_args_for(&n, args);
                return wrap_try(quote_spanned!(span => self.#name #tf(#a)));
            }
            // dynamic attribute holding a callable
            let a = self.args_bag(args);
            let getter = wrap_try(quote!(self.__getattr__(#n)));
            return wrap_try(quote_spanned!(span => #getter.__call__(#a)));
        }
        let r = self.expr(recv);
        // `Cls(..).method(..)`: a Python-defined method of a class of this
        // block on a fresh instance takes its declared (positional) parameters.
        let local_instance = matches!(recv, Expr::Call { callee, .. }
            if matches!(&**callee, Expr::Ident(c) if self.ctx.local_classes.contains(&c.to_string())));
        if local_instance
            && self.ctx.local_methods.contains(&n)
            && !TESTCASE_METHODS.contains(&n.as_str())
            && !n.starts_with("__")
            && !has_kw_or_splat(args)
            && !self.ctx.pysig_methods.contains(&n)
        {
            let a = self.args_values_for(&n, args);
            return wrap_try(quote_spanned!(span => #r.#name #tf(#a)));
        }
        // `obj.__class__(..)`: construct another instance of `obj`'s class
        // (the class object is read, then called).
        if n == "__class__" {
            let a = self.args_bag(args);
            let cls = wrap_try(quote_spanned!(span => #r.__class__()));
            return wrap_try(
                quote_spanned!(span => ::laila::python::builtins::call(&::laila::python::IntoArg::into_arg(#cls), #a)),
            );
        }
        // `s.encode("utf-8")` / `b.decode("ascii")`: the codec-naming form
        // of the zero-argument `encode()` / `decode()`.
        let names_codec = |a: &CallArg| match a {
            CallArg::Pos(e) => is_str_operand(e),
            CallArg::Kw(k, _) => k == "encoding" || k == "errors",
            _ => false,
        };
        if (n == "encode" || n == "decode") && !args.is_empty() && args.iter().all(names_codec) {
            let f = Ident::new(&format!("{n}_with"), name.span());
            let a = self.args_bag(args);
            return wrap_try(
                quote_spanned!(span => ::laila::python::methods::#f(&::laila::python::IntoArg::into_arg(#r), #a)),
            );
        }
        // `case.assertX(a, b, msg)` on a `TestCase` passed as a value.
        if let Some(arity) = ASSERT_ARITY
            .iter()
            .find(|(m, _)| *m == n.as_str())
            .map(|(_, k)| *k)
        {
            if args.len() == arity + 1 && args.iter().all(|a| matches!(a, CallArg::Pos(_))) {
                let with_msg = Ident::new(&format!("{n}_msg"), name.span());
                let a = self.args_passthrough(args);
                return wrap_try(quote_spanned!(span => #r.#with_msg(#a)));
            }
        }
        // `c._try_admit()` on an `#![dyn_classes(..)]` instance: every
        // method goes through the object protocol.
        if matches!(recv, Expr::Ident(i) if self.ctx.alias_objs.contains(&i.to_string())) {
            let a = self.args_bag(args);
            return wrap_try(quote_spanned!(span => {
                #[allow(unused_imports)]
                use ::laila::python::builtins::CallMethod as _;
                (#r).__laila_call_method__(#n, #a)
            }));
        }
        // `self.proxy.add(3, 7)`: the receiver is a dynamic attribute of a
        // `py_class!` instance (always an `Arg`). A name `Arg` has no
        // method for, or whose forwarder's arity differs from the call's,
        // is another object's method reached dynamically.
        if self.is_arg_receiver(recv) {
            let positional = args.iter().filter(|a| matches!(a, CallArg::Pos(_))).count();
            let plain = positional == args.len();
            let dynamic = match arg_method_arity(&n) {
                None => true,
                Some(-1) => false,
                Some(k) => !plain || positional as i32 != k,
            };
            if dynamic {
                let a = self.args_bag(args);
                return wrap_try(quote_spanned!(span => {
                    #[allow(unused_imports)]
                    use ::laila::python::builtins::CallMethod as _;
                    (#r).__laila_call_method__(#n, #a)
                }));
            }
            if arg_method_arity(&n) == Some(-1) {
                let a = self.args_bag(args);
                return wrap_try(quote_spanned!(span => #r.#name #tf(#a)));
            }
        }
        // `x.reset(token)`: a method whose `Arg` forwarder takes no
        // arguments, called with some -- another object's method of the
        // same name, reached dynamically. A module receiver (`laila`,
        // `aliases` of `#![modules(..)]`) is a typed Rust value whose method
        // of that name is the one meant (`laila.constant(..)`).
        let module_recv = matches!(recv, Expr::Ident(i)
            if !self.ctx.bound.contains(&i.to_string())
                && (i == "laila"
                    || self.ctx.dyn_objs.contains(&i.to_string())
                    || STDLIB_MODULES.contains(&i.to_string().trim_start_matches("r#"))));
        // `f.result(timeout=10)` / `f.exception(timeout)`: an attribute
        // forwarder on `Arg` (`fut.result`) that is a method with arguments
        // on a `concurrent.futures` future.
        // `peers.channel(comm, gid, name)`: likewise the `channel` attribute
        // of a `StreamEntry` vs. the registry's resolver.
        const ATTR_AS_METHOD: &[&str] = &["result", "exception", "channel"];
        if (ZERO_ARG_METHODS.contains(&n.as_str()) || ATTR_AS_METHOD.contains(&n.as_str()))
            && !args.is_empty()
            && !module_recv
        {
            let a = self.args_bag(args);
            return wrap_try(quote_spanned!(span => {
                #[allow(unused_imports)]
                use ::laila::python::builtins::CallMethod as _;
                (#r).__laila_call_method__(#n, #a)
            }));
        }
        // `fut.error()`: a zero-argument `Arg` forwarder whose name another
        // class also uses with arguments (`logger.error(msg)`) -- a call
        // without arguments is the zero-argument method.
        if ZERO_ARG_METHODS.contains(&n.as_str()) && args.is_empty() {
            return wrap_try(quote_spanned!(span => #r.#name #tf()));
        }
        // `proxy.value()`: a *call* of an attribute-read forwarder's name on
        // a dynamic receiver is a method of the object (the remote `value`),
        // not the getter.
        if args.is_empty()
            && ARG_ATTR_NAMES.contains(&n.as_str())
            && !ZERO_ARG_METHODS.contains(&n.as_str())
            && !module_recv
            && self.is_arg_receiver(recv)
        {
            return wrap_try(quote_spanned!(span => {
                #[allow(unused_imports)]
                use ::laila::python::builtins::CallMethod as _;
                (#r).__laila_call_method__(#n, ::laila::python::Args::new())
            }));
        }
        let a = self.lower_args_for(&n, args);
        wrap_try(quote_spanned!(span => #r.#name #tf(#a)))
    }

    fn binary(&self, op: BinOp, l: &Expr, r: &Expr, span: Span) -> TokenStream {
        match op {
            // Python `and` / `or` short-circuit and return an *operand*
            // (`d or {}`), not a bool; conditions apply truthiness to the result.
            // Operands that are names are shared (`fp and os.path.exists(fp)`
            // reads `fp` twice), as every Python read of a name is.
            BinOp::And => {
                let a = self.arg_value(l);
                let b = self.arg_value(r);
                quote_spanned!(span => {
                    let __laila_l = #a;
                    if !::laila::python::builtins::truthy(&__laila_l) { __laila_l }
                    else { #b }
                })
            }
            BinOp::Or => {
                let a = self.arg_value(l);
                let b = self.arg_value(r);
                quote_spanned!(span => {
                    let __laila_l = #a;
                    if ::laila::python::builtins::truthy(&__laila_l) { __laila_l }
                    else { #b }
                })
            }
            BinOp::Is | BinOp::IsNot => {
                let inner = if matches!(r, Expr::PyNone(_)) {
                    let a = self.expr(l);
                    quote_spanned!(span => ::laila::python::builtins::is_none(&(#a)))
                } else {
                    let a = self.expr(l);
                    let b = self.expr(r);
                    quote_spanned!(span => ::laila::python::builtins::is_(&(#a), &(#b)))
                };
                if op == BinOp::IsNot {
                    quote!(!#inner)
                } else {
                    inner
                }
            }
            BinOp::In | BinOp::NotIn => {
                let container = self.expr(r);
                let item = self.arg_pass(l);
                let inner = wrap_try(
                    quote_spanned!(span => ::laila::python::builtins::contains(&(#container), #item)),
                );
                if op == BinOp::NotIn {
                    quote!(!#inner)
                } else {
                    inner
                }
            }
            // Operands are shared, not consumed (`v % 2` then `v * 10`).
            BinOp::Pow => {
                let a = self.arith_operand(l, false);
                let b = self.arith_operand(r, false);
                wrap_try(quote_spanned!(span => ::laila::python::builtins::pow(#a, #b)))
            }
            BinOp::FloorDiv => {
                let a = self.arith_operand(l, false);
                let b = self.arith_operand(r, false);
                wrap_try(quote_spanned!(span => ::laila::python::builtins::floordiv(#a, #b)))
            }
            // Python `/` is true division and `%` takes the divisor's sign:
            // neither matches Rust on integers, so both go through builtins.
            BinOp::Div => {
                let a = self.arith_operand(l, false);
                let b = self.arith_operand(r, false);
                wrap_try(quote_spanned!(span => ::laila::python::builtins::truediv(#a, #b)))
            }
            BinOp::Mod => {
                let a = self.arith_operand(l, false);
                let b = self.arith_operand(r, false);
                wrap_try(quote_spanned!(span => ::laila::python::builtins::pymod(#a, #b)))
            }
            // `+ - *` may raise on `Arg` (`TypeError`), so they go through
            // `py_try!`: plain Rust scalars pass straight through. A string
            // literal operand (`"ab" * 2`, `1 + "x"`) is lifted to `Arg`.
            BinOp::Add | BinOp::Sub | BinOp::Mul => {
                // `0.99 * (len(lat) - 1)`: a float literal against a `usize`
                // length is Python float arithmetic -- the literal is lifted
                // to `Arg` so the mixed-type operation goes through `__mul__`.
                // `len(got) + peer.messages_with_drop`: a length against a
                // dynamic value is lifted too (`usize` has no `Arg` operand
                // impl: the integer literal must stay `i64`).
                let lift = is_str_operand(l)
                    || is_str_operand(r)
                    || (is_float_literal(l) && contains_len_call(r))
                    || (is_float_literal(r) && contains_len_call(l));
                let lift_len_l = contains_len_call(l) && self.is_dynamic_operand(r);
                let lift_len_r = contains_len_call(r) && self.is_dynamic_operand(l);
                let a = self.arith_operand(l, lift);
                let b = self.arith_operand(r, lift);
                let a = if lift_len_l {
                    quote!(::laila::python::IntoArg::into_arg(#a))
                } else {
                    a
                };
                let b = if lift_len_r {
                    quote!(::laila::python::IntoArg::into_arg(#b))
                } else {
                    b
                };
                let o: TokenStream = match op {
                    BinOp::Add => quote!(+),
                    BinOp::Sub => quote!(-),
                    _ => quote!(*),
                };
                wrap_try(quote_spanned!(span => (#a #o #b)))
            }
            _ => {
                let a = self.expr(l);
                let b = self.expr(r);
                let o: TokenStream = match op {
                    BinOp::Eq => quote!(==),
                    BinOp::Ne => quote!(!=),
                    BinOp::Lt => quote!(<),
                    BinOp::Le => quote!(<=),
                    BinOp::Gt => quote!(>),
                    BinOp::Ge => quote!(>=),
                    BinOp::BitOr => quote!(|),
                    BinOp::BitXor => quote!(^),
                    BinOp::BitAnd => quote!(&),
                    BinOp::Shl => quote!(<<),
                    BinOp::Shr => quote!(>>),
                    BinOp::Add => quote!(+),
                    BinOp::Sub => quote!(-),
                    BinOp::Mul => quote!(*),
                    BinOp::Div => quote!(/),
                    BinOp::Mod => quote!(%),
                    BinOp::AndAnd => quote!(&&),
                    BinOp::OrOr => quote!(||),
                    _ => unreachable!(),
                };
                // `cache << origin` / `origin >> cache` on pools: the fallible
                // `__lshift__` / `__rshift__` (a proxy cycle is a `ValueError`),
                // each returning the right operand so chains nest
                // (`(mem << hdf5) << s3`). Operands are shared, not moved.
                // Shifting a number (`8 << 20`, `n >> 1`): an integer
                // operand marks the arithmetic shift, which is plain Rust.
                let numeric = is_int_operand(l) || is_int_operand(r);
                // `cache <= origin` on pools is the duplication operator
                // (`__le__`, a `GroupFuture`); a `<=` between two values
                // that are not number literals dispatches by autoref:
                // pools (typed or `Arg`) to `__le__`, the rest to `PartialOrd`.
                // `delay > 0` / `0 < delay`: an ordering against an integer
                // literal -- a float operand compares like Python (autoref
                // specialisation: `FloatIntCmp` over the `PartialOrd` fallback).
                if matches!(op, BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge)
                    && (is_int_operand(l) != is_int_operand(r))
                    && !is_str_operand(l)
                    && !is_str_operand(r)
                {
                    let m = match op {
                        BinOp::Lt => quote!(__laila_lt),
                        BinOp::Gt => quote!(__laila_gt),
                        BinOp::Le => quote!(__laila_le),
                        _ => quote!(__laila_ge),
                    };
                    // `pool <= 5`: a pool operand reaches `PoolLe` (a
                    // `TypeError` result) through the same autoref chain;
                    // `LeOut` unifies it with the plain `bool`.
                    if op == BinOp::Le {
                        return wrap_try(quote_spanned!(span => {
                            #[allow(unused_imports)]
                            use ::laila::python::ops::{FloatIntCmp as _, LeOut as _, PlainCmp as _, PoolLe as _};
                            ::laila::python::ops::LeOut::le_out((&(#a)).__laila_le(&(#b)))
                        }));
                    }
                    return quote_spanned!(span => {
                        #[allow(unused_imports)]
                        use ::laila::python::ops::{FloatIntCmp as _, PlainCmp as _};
                        (&(#a)).#m(&(#b))
                    });
                }
                if op == BinOp::Le && !is_dynamic_scalar_literal(l) && !is_dynamic_scalar_literal(r)
                {
                    return wrap_try(quote_spanned!(span => {
                        #[allow(unused_imports)]
                        use ::laila::python::ops::{PlainLe as _, PoolLe as _};
                        (&(#a)).__laila_le(&(#b))
                    }));
                }
                if (op == BinOp::Shl || op == BinOp::Shr) && !numeric {
                    let dunder = if op == BinOp::Shl {
                        quote!(__lshift__)
                    } else {
                        quote!(__rshift__)
                    };
                    let b = match r {
                        Expr::Ident(i) if i != "self" => {
                            let x = self.ident(i);
                            quote!(&#x)
                        }
                        _ => b,
                    };
                    return wrap_try(quote_spanned!(span => (#a).#dunder(#b)));
                }
                // `flags | wire.FLAG_END` on `Arg`s yields a `Result` (the
                // operator may raise `TypeError`): `py_try!` passes plain
                // Rust scalars straight through.
                if matches!(op, BinOp::BitOr | BinOp::BitAnd | BinOp::BitXor) {
                    return wrap_try(quote_spanned!(span => (#a #o #b)));
                }
                quote_spanned!(span => (#a #o #b))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn comprehension(
        &self,
        kind: &CompKind,
        elem: &Expr,
        value: Option<&Expr>,
        pat: &TokenStream,
        iter: &Expr,
        cond: Option<&Expr>,
        span: Span,
    ) -> TokenStream {
        let it = self.iter_operand(iter);
        let iter_call = wrap_try(quote!(::laila::python::builtins::py_iter(#it)));
        let item = wrap_try(quote!(__laila_item));
        // The pattern binds names for the element / condition (`lambda: v`
        // inside the comprehension captures `v` like any local).
        let mut inner_ctx = self.ctx.clone();
        pattern_names(pat, &mut inner_ctx.bound);
        let inner = Gen::new(&inner_ctx);
        let bind = if is_tuple_pattern(pat) && has_nested_tuple(pat) {
            let mut c = 0usize;
            let binds = nested_unpack_let(pat, quote!(__laila_item), &mut c);
            quote!(let __laila_item = #item; #binds)
        } else if is_tuple_pattern(pat) {
            let n = tuple_arity(pat);
            let unpack = Ident::new(&format!("unpack{n}"), Span::call_site());
            let inner = wrap_try(quote!(::laila::python::builtins::#unpack(__laila_item)));
            quote!(let __laila_item = #item; let #pat = #inner;)
        } else {
            quote!(let #pat = #item;)
        };
        let guard = cond.map(|c| inner.cond(c));
        let body = match kind {
            CompKind::Dict => {
                let k = inner.arg_value(elem);
                let v = inner.arg_value(value.expect("dict comprehension value"));
                let w = wrap_try(quote!(__laila_out.__setitem__(#k, #v)));
                quote!(#w;)
            }
            _ => {
                let v = inner.arg_value(elem);
                quote!(__laila_out.push(#v);)
            }
        };
        let body = match guard {
            Some(g) => quote!(if #g { #body }),
            None => body,
        };
        match kind {
            CompKind::Dict => quote_spanned!(span => {
                let __laila_out = ::laila::python::builtins::dict(::laila::python::Args::new())?;
                for __laila_item in #iter_call { #bind #body }
                __laila_out
            }),
            CompKind::Set => {
                let mk = wrap_try(quote!(::laila::python::builtins::set(
                    ::laila::python::Args::from(::laila::python::IntoArg::into_arg(__laila_out))
                )));
                quote_spanned!(span => {
                    let mut __laila_out: ::laila::python::__rt::Vec<::laila::python::Arg> = ::laila::python::__rt::Vec::new();
                    for __laila_item in #iter_call { #bind #body }
                    #mk
                })
            }
            _ => quote_spanned!(span => {
                let mut __laila_out: ::laila::python::__rt::Vec<::laila::python::Arg> = ::laila::python::__rt::Vec::new();
                for __laila_item in #iter_call { #bind #body }
                ::laila::python::IntoArg::into_arg(__laila_out)
            }),
        }
    }

    fn fstring(&self, l: &Literal) -> TokenStream {
        let raw = l.to_string();
        let inner = raw
            .trim_start_matches('"')
            .trim_end_matches('"')
            .to_string();
        let mut fmt = String::new();
        let mut args: Vec<TokenStream> = Vec::new();
        let chars: Vec<char> = inner.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            if c == '{' {
                if chars.get(i + 1) == Some(&'{') {
                    fmt.push_str("{{");
                    i += 2;
                    continue;
                }
                // expression until matching '}' (respect nesting and strings)
                let mut depth = 1;
                let mut j = i + 1;
                let mut quote_ch: Option<char> = None;
                while j < chars.len() {
                    let d = chars[j];
                    match quote_ch {
                        Some(q) => {
                            if d == q {
                                quote_ch = None;
                            }
                        }
                        None => match d {
                            '"' | '\'' => quote_ch = Some(d),
                            '{' | '[' | '(' => depth += 1,
                            '}' | ']' | ')' => {
                                depth -= 1;
                                if depth == 0 {
                                    break;
                                }
                            }
                            _ => {}
                        },
                    }
                    j += 1;
                }
                let field: String = chars[i + 1..j].iter().collect();
                i = j + 1;
                // split conversion `!r` and spec `:...` at top level
                let (expr_text, conv, spec) = split_field(&field);
                // Nested string literals arrive escaped (`{d[\"k\"]}`): unescape
                // them so the placeholder parses as ordinary Rust tokens.
                let expr_text = expr_text.replace("\\\"", "\"");
                let expr_ts = match syn::parse_str::<TokenStream>(&expr_text) {
                    Ok(ts) => ts,
                    Err(_) => {
                        let msg = format!("f-string: cannot parse `{expr_text}`");
                        return quote_spanned!(l.span() => compile_error!(#msg));
                    }
                };
                let e = self.rewrite_tokens(expr_ts);
                let e = match conv.as_deref() {
                    Some("r") => quote!(::laila::python::builtins::repr(&(#e))),
                    // `{exc}` is `str(exc)` (the message, not `Kind: message`);
                    // autoref specialization picks the exception impl.
                    Some("s") | None if spec.is_none() => {
                        quote!({ use ::laila::python::fstr::{FStrDisplay as _, FStrExc as _}; (&&::laila::python::fstr::FStr(&(#e))).__laila_fstr() })
                    }
                    _ => quote!((#e)),
                };
                // `{x:spec}`: Python's format-spec mini-language at run time
                // (`Arg`'s `Display` ignores Rust width / precision flags).
                let e = match spec {
                    Some(s) => {
                        let spec_lit = Literal::string(&s);
                        quote!(::laila::python::strfmt::fstr_spec(#e, #spec_lit))
                    }
                    None => e,
                };
                args.push(e);
                fmt.push_str("{}");
            } else if c == '}' {
                if chars.get(i + 1) == Some(&'}') {
                    fmt.push_str("}}");
                    i += 2;
                    continue;
                }
                fmt.push('}');
                i += 1;
            } else {
                fmt.push(c);
                i += 1;
            }
        }
        let fmt_lit = Literal::string(&unescape_keep(&fmt));
        quote_spanned!(l.span() => ::laila::python::__rt::format!(#fmt_lit #(, #args)*))
    }
}

/// `expr!r:spec` -> (expr, conv, spec)
fn split_field(field: &str) -> (String, Option<String>, Option<String>) {
    let mut depth = 0;
    let mut quote_ch: Option<char> = None;
    let mut conv = None;
    let mut spec = None;
    let mut expr_end = field.len();
    let chars: Vec<char> = field.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match quote_ch {
            Some(q) => {
                if c == q {
                    quote_ch = None;
                }
            }
            None => match c {
                '"' | '\'' => quote_ch = Some(c),
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth -= 1,
                '!' if depth == 0
                    && chars
                        .get(i + 1)
                        .map(|n| n.is_ascii_alphabetic())
                        .unwrap_or(false)
                    && chars.get(i + 1) != Some(&'=') =>
                {
                    expr_end = expr_end.min(i);
                    conv = Some(chars[i + 1].to_string());
                    i += 2;
                    continue;
                }
                ':' if depth == 0 => {
                    expr_end = expr_end.min(i);
                    spec = Some(chars[i + 1..].iter().collect());
                    break;
                }
                _ => {}
            },
        }
        i += 1;
    }
    let expr: String = chars[..expr_end].iter().collect();
    (expr.trim().to_string(), conv, spec)
}

fn unescape_keep(s: &str) -> String {
    // The literal text came from a Rust string literal body; `Literal::string`
    // re-escapes, so undo the source escapes first.
    let mut out = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('\\') => out.push('\\'),
                Some('"') => out.push('"'),
                Some('\'') => out.push('\''),
                Some('0') => out.push('\0'),
                Some(o) => {
                    out.push('\\');
                    out.push(o);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// `model_fields`, `__name__`, `x`: a snake_case / dunder attribute name
/// (anything with a lowercase letter and no uppercase one).
fn is_lower_attr(i: &Ident) -> bool {
    let s = i.to_string();
    let s = s.strip_prefix("r#").unwrap_or(&s);
    s.chars().any(|c| c.is_ascii_lowercase()) && !s.chars().any(|c| c.is_ascii_uppercase())
}

/// `AWS_REGION` / `HF_TOKEN`: upper-case letters, digits and `_` only, at
/// least one letter.
fn is_const_case(s: &str) -> bool {
    // `.T` (numpy transpose) is a one-letter method, not a constant.
    s.len() >= 2
        && s.chars().any(|c| c.is_ascii_uppercase())
        && s.chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// A float literal operand (`0.99`).
fn is_float_literal(e: &Expr) -> bool {
    match e {
        Expr::Lit(l) => {
            let s = l.to_string();
            s.chars().next().is_some_and(|c| c.is_ascii_digit()) && s.contains('.')
        }
        Expr::Unary {
            op: UnOp::Neg,
            expr,
            ..
        } => is_float_literal(expr),
        _ => false,
    }
}

/// Does `e` contain a `len(..)` call (a `usize` in Rust)?
fn contains_len_call(e: &Expr) -> bool {
    match e {
        Expr::Call { callee, .. } => matches!(&**callee, Expr::Ident(i) if i == "len"),
        Expr::Paren(inner) => contains_len_call(inner),
        Expr::Binary { left, right, .. } => contains_len_call(left) || contains_len_call(right),
        Expr::Unary { expr, .. } => contains_len_call(expr),
        _ => false,
    }
}

fn is_str_operand(e: &Expr) -> bool {
    matches!(e, Expr::FString(_)) || matches!(e, Expr::Lit(l) if l.to_string().starts_with('"'))
}

/// An integer literal operand (`16 << 20`): the shift is arithmetic, not
/// the pool proxy operator.
fn is_int_operand(e: &Expr) -> bool {
    match e {
        Expr::Lit(l) => {
            let s = l.to_string();
            s.chars().next().is_some_and(|c| c.is_ascii_digit())
                && !s.contains('.')
                && !s.starts_with('"')
        }
        Expr::Unary {
            op: UnOp::Neg,
            expr,
            ..
        } => is_int_operand(expr),
        _ => false,
    }
}

fn is_dynamic_scalar_literal(e: &Expr) -> bool {
    match e {
        Expr::PyNone(_) | Expr::PyBool(..) => true,
        Expr::Lit(l) => {
            let s = l.to_string();
            s.chars()
                .next()
                .map(|c| c.is_ascii_digit())
                .unwrap_or(false)
        }
        Expr::Unary {
            op: UnOp::Neg,
            expr,
            ..
        } => {
            matches!(&**expr, Expr::Lit(l) if l.to_string().chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false))
        }
        _ => false,
    }
}

/// `let (a, b)` / `let (mut a, b)`: the arity of a tuple-pattern `let` header
/// (no type annotation), `None` for any other header.
/// `let (a, (b, c)) = v` for a dynamic `v`: Python unpacks each level
/// (`unpack2(v)` then `unpack2(inner)`), so nested tuple patterns become a
/// chain of `let`s with temporaries. Returns the (flattened) statements.
fn nested_unpack_let(pat: &TokenStream, value: TokenStream, counter: &mut usize) -> TokenStream {
    let tts: Vec<TokenTree> = pat.clone().into_iter().collect();
    let inner: TokenStream = match tts.as_slice() {
        [TokenTree::Group(g)] if g.delimiter() == proc_macro2::Delimiter::Parenthesis => g.stream(),
        _ => pat.clone(),
    };
    let parts = crate::literal::split_commas(inner);
    let n = parts.len();
    let unpack = Ident::new(&format!("unpack{n}"), Span::call_site());
    let mut flat: Vec<TokenStream> = Vec::new();
    let mut nested: Vec<(Ident, TokenStream)> = Vec::new();
    for p in &parts {
        let sub: TokenStream = p.iter().cloned().collect();
        if is_tuple_pattern(&sub) {
            *counter += 1;
            let t = Ident::new(&format!("__laila_nested_{counter}"), Span::call_site());
            flat.push(quote!(#t));
            nested.push((t, sub));
        } else {
            flat.push(sub);
        }
    }
    let w = wrap_try(quote!(::laila::python::builtins::#unpack(
        ::laila::python::IntoArg::into_arg(#value)
    )));
    let mut out = quote!(let (#(#flat,)*) = #w;);
    for (t, sub) in nested {
        let rest = nested_unpack_let(&sub, quote!(#t), counter);
        out.extend(rest);
    }
    out
}

/// Does a tuple pattern contain a nested tuple?
fn has_nested_tuple(pat: &TokenStream) -> bool {
    let tts: Vec<TokenTree> = pat.clone().into_iter().collect();
    let inner: TokenStream = match tts.as_slice() {
        [TokenTree::Group(g)] if g.delimiter() == proc_macro2::Delimiter::Parenthesis => g.stream(),
        _ => pat.clone(),
    };
    crate::literal::split_commas(inner)
        .iter()
        .any(|p| matches!(p.as_slice(), [TokenTree::Group(g)] if g.delimiter() == proc_macro2::Delimiter::Parenthesis))
}

fn let_tuple_arity(header: &TokenStream) -> Option<usize> {
    let tts: Vec<TokenTree> = header.clone().into_iter().collect();
    match tts.as_slice() {
        [TokenTree::Ident(l), TokenTree::Group(g)]
            if l == "let" && g.delimiter() == proc_macro2::Delimiter::Parenthesis =>
        {
            Some(crate::literal::split_commas(g.stream()).len())
        }
        _ => None,
    }
}

/// `let name` / `let mut name` without a type annotation or pattern.
fn is_plain_let(header: &TokenStream) -> bool {
    let tts: Vec<TokenTree> = header.clone().into_iter().collect();
    match tts.as_slice() {
        [TokenTree::Ident(l), TokenTree::Ident(_)] if l == "let" => true,
        [TokenTree::Ident(l), TokenTree::Ident(m), TokenTree::Ident(_)]
            if l == "let" && m == "mut" =>
        {
            true
        }
        _ => false,
    }
}

fn ensure_mut(header: &TokenStream) -> TokenStream {
    let tts: Vec<TokenTree> = header.clone().into_iter().collect();
    if tts.len() == 2 {
        let name = &tts[1];
        quote!(let mut #name)
    } else {
        header.clone()
    }
}

fn is_tuple_pattern(pat: &TokenStream) -> bool {
    let tts: Vec<TokenTree> = pat.clone().into_iter().collect();
    match tts.as_slice() {
        [TokenTree::Group(g)] => g.delimiter() == proc_macro2::Delimiter::Parenthesis,
        [_, TokenTree::Punct(p), ..] => p.as_char() == ',',
        _ => false,
    }
}

fn tuple_arity(pat: &TokenStream) -> usize {
    let tts: Vec<TokenTree> = pat.clone().into_iter().collect();
    let inner: Vec<TokenTree> = match tts.as_slice() {
        [TokenTree::Group(g)] => g.stream().into_iter().collect(),
        _ => tts,
    };
    crate::literal::split_commas(inner.into_iter().collect()).len()
}
