//! AST for Python-shaped Rust bodies.

use proc_macro2::{Group, Ident, Literal, Span, TokenStream};

#[derive(Debug, Clone)]
pub enum Expr {
    /// Untouched tokens (macro invocations, turbofish paths, closures' headers...).
    Raw(TokenStream),
    Lit(Literal),
    Ident(Ident),
    /// `a::b::C` written with `::`.
    Path(TokenStream),
    PyNone(Span),
    PyBool(bool, Span),
    /// `base.name` (not a call).
    Attr {
        base: Box<Expr>,
        name: Ident,
    },
    /// `base.0`
    TupleField {
        base: Box<Expr>,
        index: Literal,
    },
    /// `callee(args)`.
    Call {
        callee: Box<Expr>,
        args: Vec<CallArg>,
        span: Span,
    },
    /// `recv.name::<T>(args)`.
    MethodCall {
        recv: Box<Expr>,
        name: Ident,
        turbofish: Option<TokenStream>,
        args: Vec<CallArg>,
        span: Span,
    },
    /// `base[index]`.
    Index {
        base: Box<Expr>,
        index: IndexKind,
        span: Span,
    },
    Unary {
        op: UnOp,
        expr: Box<Expr>,
        span: Span,
    },
    Binary {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
        span: Span,
    },
    /// `a if c else b`
    Ternary {
        cond: Box<Expr>,
        then: Box<Expr>,
        otherwise: Box<Expr>,
    },
    /// `lambda x, y: body`
    Lambda {
        params: Vec<Ident>,
        body: Box<Expr>,
        span: Span,
    },
    /// Rust closure `move |a, b| body`.
    Closure {
        header: TokenStream,
        body: Box<Expr>,
    },
    Await(Box<Expr>, Span),
    /// `(e)`
    Paren(Box<Expr>),
    /// Python literals.
    Dict(Group),
    List(Group),
    Tuple(Group),
    /// `[expr for pat in iter if cond]` / generator / dict comprehension
    Comprehension {
        kind: CompKind,
        elem: Box<Expr>,
        value: Option<Box<Expr>>,
        pat: TokenStream,
        iter: Box<Expr>,
        cond: Option<Box<Expr>>,
        span: Span,
    },
    /// f-string literal
    FString(Literal),
    /// `{ stmts }` as an expression.
    Block(Block),
    If {
        cond: Box<Expr>,
        then: Block,
        otherwise: Option<Box<Expr>>,
    },
    /// `match x { arms }` (arms passed through with bodies rewritten).
    Match {
        scrutinee: Box<Expr>,
        arms: Vec<(TokenStream, Expr)>,
    },
    /// `e as T`
    Cast {
        expr: Box<Expr>,
        ty: TokenStream,
    },
    /// `e?` written explicitly by the user.
    Try(Box<Expr>),
    /// `e.await` written explicitly.
    RustAwait(Box<Expr>),
    /// `&e`, `&mut e`
    Ref {
        mutable: bool,
        expr: Box<Expr>,
    },
    /// `..` ranges and other passthrough binary forms
    Range {
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
        inclusive: bool,
    },
}

#[derive(Debug, Clone)]
pub enum CompKind {
    List,
    Set,
    Dict,
    Generator,
}

#[derive(Debug, Clone)]
pub enum IndexKind {
    Expr(Box<Expr>),
    Slice {
        start: Option<Box<Expr>>,
        stop: Option<Box<Expr>>,
        step: Option<Box<Expr>>,
    },
}

#[derive(Debug, Clone)]
pub enum CallArg {
    Pos(Expr),
    Kw(Ident, Expr),
    Star(Expr),
    DoubleStar(Expr),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnOp {
    Not,
    Neg,
    Bang,
    Deref,
    Pos,
    Invert,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Or,
    And,
    Is,
    IsNot,
    In,
    NotIn,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    BitOr,
    BitXor,
    BitAnd,
    Shl,
    Shr,
    Add,
    Sub,
    Mul,
    Div,
    /// Python `//`. Rust's lexer takes `//` as a comment, so sources write
    /// `floordiv(a, b)` (py2rs emits that); the variant exists for
    /// completeness of the operator table.
    #[allow(dead_code)]
    FloorDiv,
    Mod,
    Pow,
    /// Rust `&&`, `||` written explicitly.
    AndAnd,
    OrOr,
}

#[derive(Debug, Clone, Default)]
pub struct Block {
    pub stmts: Vec<Stmt>,
    pub span: Option<Span>,
}

#[derive(Debug, Clone)]
pub enum Stmt {
    /// `let [mut] pat [: ty] = value;` (header kept raw)
    Let {
        header: TokenStream,
        value: Option<Expr>,
    },
    /// `target = value;` without `let`
    Assign {
        target: Expr,
        value: Expr,
    },
    /// `target op= value;`
    AugAssign {
        target: Expr,
        op: TokenStream,
        value: Expr,
    },
    Expr {
        expr: Expr,
        semi: bool,
    },
    Return(Option<Expr>, Span),
    With {
        cm: Expr,
        target: Option<TokenStream>,
        body: Block,
        is_async: bool,
        span: Span,
    },
    Raise(Option<Expr>, Span),
    Del(Expr, Span),
    For {
        pat: TokenStream,
        iter: Expr,
        body: Block,
        label: Option<TokenStream>,
    },
    While {
        cond: Expr,
        body: Block,
    },
    Loop {
        body: Block,
    },
    Break(TokenStream),
    Continue(TokenStream),
    /// `try { } except T as e { } ... else { } finally { }` [P16].
    Try {
        body: Block,
        handlers: Vec<Handler>,
        orelse: Option<Block>,
        finalbody: Option<Block>,
        span: Span,
    },
    /// Anything passed through verbatim by a future lowering (unused today;
    /// items go through `Item`).
    #[allow(dead_code)]
    Raw(TokenStream),
    /// A nested item inside a body. A nested `fn` is a Python nested `def`
    /// (py2rs emits it as `fn`): the generator lowers it to a closure over
    /// the enclosing locals; other items pass through verbatim.
    Item(TokenStream),
    Pass,
}

/// One `except [T [as name]] { body }` clause.
#[derive(Debug, Clone)]
pub struct Handler {
    /// The exception class (or tuple of classes); `None` for a bare `except`.
    pub ty: Option<Expr>,
    pub name: Option<Ident>,
    pub body: Block,
}

/// Every identifier mentioned inside an expression (free or not); used to
/// find the enclosing-scope bindings a `lambda` captures.
pub fn idents_in_expr(e: &Expr, out: &mut std::collections::HashSet<String>) {
    collect_idents(e, out, false)
}

/// Every identifier mentioned in a block's statements (see [`idents_in_expr`]).
pub fn idents_in_block(b: &Block, out: &mut std::collections::HashSet<String>) {
    collect_block(b, out, false)
}

/// Identifiers used as *values* in a block: like [`idents_in_block`] but the
/// callee of a direct call `name(..)` does not count. Decides whether a
/// nested `def` escapes (is passed around) or is only ever called.
pub fn value_idents_in_block(b: &Block, out: &mut std::collections::HashSet<String>) {
    collect_block(b, out, true)
}

fn collect_ts(t: &TokenStream, out: &mut std::collections::HashSet<String>) {
    for tt in t.clone() {
        match tt {
            proc_macro2::TokenTree::Ident(i) => {
                out.insert(i.to_string());
            }
            proc_macro2::TokenTree::Group(g) => collect_ts(&g.stream(), out),
            _ => {}
        }
    }
}

fn collect_block(b: &Block, out: &mut std::collections::HashSet<String>, skip_callees: bool) {
    for s in &b.stmts {
        match s {
            Stmt::Let { header, value } => {
                collect_ts(header, out);
                if let Some(v) = value {
                    collect_idents(v, out, skip_callees);
                }
            }
            Stmt::Assign { target, value } | Stmt::AugAssign { target, value, .. } => {
                collect_idents(target, out, skip_callees);
                collect_idents(value, out, skip_callees);
            }
            Stmt::Expr { expr, .. } => collect_idents(expr, out, skip_callees),
            Stmt::Return(e, _) | Stmt::Raise(e, _) => {
                if let Some(e) = e {
                    collect_idents(e, out, skip_callees);
                }
            }
            Stmt::With { cm, body, .. } => {
                collect_idents(cm, out, skip_callees);
                collect_block(body, out, skip_callees);
            }
            Stmt::Del(e, _) => collect_idents(e, out, skip_callees),
            Stmt::For { iter, body, .. } => {
                collect_idents(iter, out, skip_callees);
                collect_block(body, out, skip_callees);
            }
            Stmt::While { cond, body } => {
                collect_idents(cond, out, skip_callees);
                collect_block(body, out, skip_callees);
            }
            Stmt::Loop { body } => collect_block(body, out, skip_callees),
            Stmt::Try {
                body,
                handlers,
                orelse,
                finalbody,
                ..
            } => {
                collect_block(body, out, skip_callees);
                for h in handlers {
                    if let Some(t) = &h.ty {
                        collect_idents(t, out, skip_callees);
                    }
                    collect_block(&h.body, out, skip_callees);
                }
                if let Some(b) = orelse {
                    collect_block(b, out, skip_callees);
                }
                if let Some(b) = finalbody {
                    collect_block(b, out, skip_callees);
                }
            }
            // A nested `def`'s own header is not a use of its name: only its
            // body counts (recursion is a genuine value use).
            Stmt::Item(t) if skip_callees && nested_fn_body(t).is_some() => {
                collect_ts(&nested_fn_body(t).unwrap(), out)
            }
            Stmt::Raw(t) | Stmt::Item(t) => collect_ts(t, out),
            Stmt::Break(_) | Stmt::Continue(_) | Stmt::Pass => {}
        }
    }
}

/// The `{ body }` of a nested `fn` item, if the tokens are one.
fn nested_fn_body(t: &TokenStream) -> Option<TokenStream> {
    let tts: Vec<proc_macro2::TokenTree> = t.clone().into_iter().collect();
    let is_fn = tts
        .iter()
        .any(|x| matches!(x, proc_macro2::TokenTree::Ident(i) if i == "fn"));
    match tts.last() {
        Some(proc_macro2::TokenTree::Group(g))
            if is_fn && g.delimiter() == proc_macro2::Delimiter::Brace =>
        {
            Some(g.stream())
        }
        _ => None,
    }
}

fn collect_idents(e: &Expr, out: &mut std::collections::HashSet<String>, skip_callees: bool) {
    let ts = |t: &TokenStream, out: &mut std::collections::HashSet<String>| collect_ts(t, out);
    let idents_in_expr = |e: &Expr, out: &mut std::collections::HashSet<String>| {
        collect_idents(e, out, skip_callees)
    };
    let args = |a: &[CallArg], out: &mut std::collections::HashSet<String>| {
        for x in a {
            match x {
                CallArg::Pos(e) | CallArg::Star(e) | CallArg::DoubleStar(e) | CallArg::Kw(_, e) => {
                    collect_idents(e, out, skip_callees)
                }
            }
        }
    };
    let block = |b: &Block, out: &mut std::collections::HashSet<String>| {
        collect_block(b, out, skip_callees)
    };
    match e {
        Expr::Call {
            callee, args: a, ..
        } if skip_callees && matches!(&**callee, Expr::Ident(_)) => {
            args(a, out);
        }
        Expr::Raw(t) | Expr::Path(t) => ts(t, out),
        Expr::Lit(_) | Expr::PyNone(_) | Expr::PyBool(..) | Expr::FString(_) => {
            if let Expr::FString(l) = e {
                // `{name}` placeholders inside the literal
                let s = l.to_string();
                let mut chars = s.chars().peekable();
                while let Some(c) = chars.next() {
                    if c == '{' {
                        let mut name = String::new();
                        while let Some(&n) = chars.peek() {
                            if n.is_alphanumeric() || n == '_' {
                                name.push(n);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        if !name.is_empty() {
                            out.insert(name);
                        }
                    }
                }
            }
        }
        Expr::Ident(i) => {
            out.insert(i.to_string());
        }
        Expr::Attr { base, .. } | Expr::TupleField { base, .. } => idents_in_expr(base, out),
        Expr::Call {
            callee, args: a, ..
        } => {
            idents_in_expr(callee, out);
            args(a, out);
        }
        Expr::MethodCall { recv, args: a, .. } => {
            idents_in_expr(recv, out);
            args(a, out);
        }
        Expr::Index { base, index, .. } => {
            idents_in_expr(base, out);
            match index {
                IndexKind::Expr(e) => idents_in_expr(e, out),
                IndexKind::Slice { start, stop, step } => {
                    for x in [start, stop, step].into_iter().flatten() {
                        idents_in_expr(x, out);
                    }
                }
            }
        }
        Expr::Unary { expr, .. }
        | Expr::Await(expr, _)
        | Expr::Paren(expr)
        | Expr::Try(expr)
        | Expr::RustAwait(expr)
        | Expr::Ref { expr, .. }
        | Expr::Cast { expr, .. } => idents_in_expr(expr, out),
        Expr::Binary { left, right, .. } => {
            idents_in_expr(left, out);
            idents_in_expr(right, out);
        }
        Expr::Ternary {
            cond,
            then,
            otherwise,
        } => {
            idents_in_expr(cond, out);
            idents_in_expr(then, out);
            idents_in_expr(otherwise, out);
        }
        // A lambda is a value that may escape (`ex.map(lambda _: worker(), ..)`):
        // a nested `def` it calls must be an owning callable too, so callees
        // inside a lambda body count as value uses.
        // The lambda's own parameters are not uses of enclosing names.
        Expr::Lambda { params, body, .. } => {
            let mut inner = std::collections::HashSet::new();
            collect_idents(body, &mut inner, false);
            for p in params {
                inner.remove(&p.to_string());
            }
            out.extend(inner);
        }
        Expr::Closure { body, .. } => collect_idents(body, out, false),
        Expr::Dict(g) | Expr::List(g) | Expr::Tuple(g) => ts(&g.stream(), out),
        // A comprehension binds its loop variables for `elem` / `cond`
        // (not the outermost `iter`, which Python evaluates outside); a
        // same-named enclosing local is shadowed, not used.
        Expr::Comprehension {
            elem,
            value,
            pat,
            iter,
            cond,
            ..
        } => {
            let mut bound = std::collections::HashSet::new();
            ts(pat, &mut bound);
            bound.retain(|n| n != "mut" && n != "ref" && n != "_");
            let mut inner = std::collections::HashSet::new();
            idents_in_expr(elem, &mut inner);
            if let Some(v) = value {
                idents_in_expr(v, &mut inner);
            }
            if let Some(c) = cond {
                idents_in_expr(c, &mut inner);
            }
            out.extend(inner.into_iter().filter(|n| !bound.contains(n)));
            idents_in_expr(iter, out);
        }
        Expr::Block(b) => block(b, out),
        Expr::If {
            cond,
            then,
            otherwise,
        } => {
            idents_in_expr(cond, out);
            block(then, out);
            if let Some(o) = otherwise {
                idents_in_expr(o, out);
            }
        }
        Expr::Match { scrutinee, arms } => {
            idents_in_expr(scrutinee, out);
            for (p, e) in arms {
                ts(p, out);
                idents_in_expr(e, out);
            }
        }
        Expr::Range { start, end, .. } => {
            for x in [start, end].into_iter().flatten() {
                idents_in_expr(x, out);
            }
        }
    }
}

/// Does a block contain a `return` that leaves the enclosing function
/// (nested `fn` items and lambdas excluded)? Decides whether a `try`
/// statement needs the return-carrying lowering [P16c].
pub fn block_has_return(b: &Block) -> bool {
    b.stmts.iter().any(stmt_has_return)
}

fn stmt_has_return(s: &Stmt) -> bool {
    match s {
        Stmt::Return(..) => true,
        Stmt::Let { value: Some(v), .. } => expr_has_return(v),
        Stmt::Let { value: None, .. } => false,
        Stmt::Assign { value, .. } | Stmt::AugAssign { value, .. } => expr_has_return(value),
        Stmt::Expr { expr, .. } => expr_has_return(expr),
        Stmt::With { body, .. }
        | Stmt::For { body, .. }
        | Stmt::While { body, .. }
        | Stmt::Loop { body } => block_has_return(body),
        Stmt::Try {
            body,
            handlers,
            orelse,
            finalbody,
            ..
        } => {
            block_has_return(body)
                || handlers.iter().any(|h| block_has_return(&h.body))
                || orelse.as_ref().is_some_and(block_has_return)
                || finalbody.as_ref().is_some_and(block_has_return)
        }
        _ => false,
    }
}

/// Does a block contain a `break` / `continue` of the loop *enclosing*
/// the block (nested loops excluded)? Decides whether a `try` statement
/// inside a loop needs the loop-control-carrying lowering [P16d].
pub fn block_has_loopctl(b: &Block) -> bool {
    b.stmts.iter().any(stmt_has_loopctl)
}

fn stmt_has_loopctl(s: &Stmt) -> bool {
    match s {
        Stmt::Break(_) | Stmt::Continue(_) => true,
        Stmt::Let { value: Some(v), .. } => expr_has_loopctl(v),
        Stmt::Assign { value, .. } | Stmt::AugAssign { value, .. } => expr_has_loopctl(value),
        Stmt::Expr { expr, .. } => expr_has_loopctl(expr),
        Stmt::With { body, .. } => block_has_loopctl(body),
        Stmt::Try {
            body,
            handlers,
            orelse,
            finalbody,
            ..
        } => {
            block_has_loopctl(body)
                || handlers.iter().any(|h| block_has_loopctl(&h.body))
                || orelse.as_ref().is_some_and(block_has_loopctl)
                || finalbody.as_ref().is_some_and(block_has_loopctl)
        }
        _ => false,
    }
}

fn expr_has_loopctl(e: &Expr) -> bool {
    match e {
        Expr::Block(b) => block_has_loopctl(b),
        Expr::If {
            then, otherwise, ..
        } => block_has_loopctl(then) || otherwise.as_deref().is_some_and(expr_has_loopctl),
        Expr::Match { arms, .. } => arms.iter().any(|(_, e)| expr_has_loopctl(e)),
        _ => false,
    }
}

fn expr_has_return(e: &Expr) -> bool {
    match e {
        Expr::Block(b) => block_has_return(b),
        Expr::If {
            then, otherwise, ..
        } => block_has_return(then) || otherwise.as_deref().is_some_and(expr_has_return),
        Expr::Match { arms, .. } => arms.iter().any(|(_, e)| expr_has_return(e)),
        _ => false,
    }
}
