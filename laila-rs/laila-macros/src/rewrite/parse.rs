//! Pratt parser from token trees to the Python-shaped AST.

use proc_macro2::{Delimiter, Group, Ident, Span, TokenStream, TokenTree};

use super::ast::*;

pub type PResult<T> = Result<T, syn::Error>;

pub struct Cur {
    tts: Vec<TokenTree>,
    pos: usize,
    /// Brace groups end the expression (statement headers: `if c {`, `for p in it {`).
    pub no_struct: bool,
    /// `as` ends the expression (`with x as y`).
    pub stop_at_as: bool,
    /// `for` / `if` end the expression (inside comprehensions).
    pub stop_at_for_if: bool,
    /// `else` ends the expression (ternary parsing).
    pub stop_at_else: bool,
}

fn err<T>(span: Span, msg: impl std::fmt::Display) -> PResult<T> {
    Err(syn::Error::new(span, format!("laila::python: {msg}")))
}

impl Cur {
    pub fn new(ts: TokenStream) -> Cur {
        Cur {
            tts: ts.into_iter().collect(),
            pos: 0,
            no_struct: false,
            stop_at_as: false,
            stop_at_for_if: false,
            stop_at_else: false,
        }
    }
    pub fn from_tts(tts: Vec<TokenTree>) -> Cur {
        Cur {
            tts,
            pos: 0,
            no_struct: false,
            stop_at_as: false,
            stop_at_for_if: false,
            stop_at_else: false,
        }
    }
    pub fn at_end(&self) -> bool {
        self.pos >= self.tts.len()
    }
    pub fn peek(&self) -> Option<&TokenTree> {
        self.tts.get(self.pos)
    }
    pub fn peek_n(&self, n: usize) -> Option<&TokenTree> {
        self.tts.get(self.pos + n)
    }
    pub fn next(&mut self) -> Option<TokenTree> {
        let t = self.tts.get(self.pos).cloned();
        self.pos += 1;
        t
    }
    pub fn span(&self) -> Span {
        self.peek()
            .map(|t| t.span())
            .or_else(|| self.tts.last().map(|t| t.span()))
            .unwrap_or_else(Span::call_site)
    }
    pub fn peek_ident(&self, s: &str) -> bool {
        matches!(self.peek(), Some(TokenTree::Ident(i)) if i == s)
    }
    pub fn peek_ident_n(&self, n: usize, s: &str) -> bool {
        matches!(self.peek_n(n), Some(TokenTree::Ident(i)) if i == s)
    }
    pub fn peek_punct_n(&self, n: usize, c: char) -> bool {
        matches!(self.tts.get(self.pos + n), Some(TokenTree::Punct(p)) if p.as_char() == c)
    }
    pub fn peek_punct(&self, c: char) -> bool {
        matches!(self.peek(), Some(TokenTree::Punct(p)) if p.as_char() == c)
    }
    /// Operator string formed by joint puncts starting at the cursor, with its token count.
    pub fn peek_op(&self) -> Option<(String, usize)> {
        let mut s = String::new();
        let mut n = 0;
        let mut i = self.pos;
        while let Some(TokenTree::Punct(p)) = self.tts.get(i) {
            s.push(p.as_char());
            n += 1;
            i += 1;
            if p.spacing() == proc_macro2::Spacing::Alone {
                break;
            }
        }
        if n == 0 {
            None
        } else {
            Some((s, n))
        }
    }
    pub fn peek_op_is(&self, op: &str) -> bool {
        matches!(self.peek_op(), Some((s, _)) if s == op)
    }
    pub fn eat_op(&mut self, op: &str) -> bool {
        if let Some((s, n)) = self.peek_op() {
            if s == op {
                self.pos += n;
                return true;
            }
        }
        false
    }
    pub fn eat_ident(&mut self, s: &str) -> Option<Ident> {
        if let Some(TokenTree::Ident(i)) = self.peek() {
            if i == s {
                let i = i.clone();
                self.pos += 1;
                return Some(i);
            }
        }
        None
    }
    pub fn expect_ident(&mut self, s: &str) -> PResult<Ident> {
        let span = self.span();
        self.eat_ident(s)
            .ok_or_else(|| syn::Error::new(span, format!("laila::python: expected `{s}`")))
    }
    pub fn eat_punct(&mut self, c: char) -> bool {
        if self.peek_punct(c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    pub fn eat_group(&mut self, d: Delimiter) -> Option<Group> {
        if let Some(TokenTree::Group(g)) = self.peek() {
            if g.delimiter() == d {
                let g = g.clone();
                self.pos += 1;
                return Some(g);
            }
        }
        None
    }
    pub fn expect_brace(&mut self) -> PResult<Group> {
        let span = self.span();
        self.eat_group(Delimiter::Brace)
            .ok_or_else(|| syn::Error::new(span, "laila::python: expected `{ ... }` block"))
    }
    /// Collect raw tokens until a top-level predicate matches (not consumed).
    pub fn take_until(&mut self, stop: impl Fn(&Cur) -> bool) -> TokenStream {
        let mut out = TokenStream::new();
        while !self.at_end() && !stop(self) {
            out.extend(std::iter::once(self.next().unwrap()));
        }
        out
    }
    fn with_flags<T>(&mut self, no_struct: bool, f: impl FnOnce(&mut Cur) -> T) -> T {
        let saved = (
            self.no_struct,
            self.stop_at_as,
            self.stop_at_for_if,
            self.stop_at_else,
        );
        self.no_struct = no_struct;
        self.stop_at_as = false;
        self.stop_at_for_if = false;
        self.stop_at_else = false;
        let r = f(self);
        (
            self.no_struct,
            self.stop_at_as,
            self.stop_at_for_if,
            self.stop_at_else,
        ) = saved;
        r
    }
}

/// `Name` / `_Name` (a private class): an uppercase first letter after any
/// leading underscores, and some lowercase letter.
fn is_camel(i: &Ident) -> bool {
    let s = i.to_string();
    let s = s.strip_prefix("r#").unwrap_or(&s);
    let s = s.trim_start_matches('_');
    s.chars()
        .next()
        .map(|c| c.is_ascii_uppercase())
        .unwrap_or(false)
        && s.chars().any(|c| c.is_ascii_lowercase())
}

// ---------------------------------------------------------------------------
// Blocks and statements

pub fn parse_block(g: &Group) -> PResult<Block> {
    let mut cur = Cur::new(g.stream());
    let stmts = parse_stmts(&mut cur)?;
    Ok(Block {
        stmts,
        span: Some(g.span()),
    })
}

pub fn parse_stmts(cur: &mut Cur) -> PResult<Vec<Stmt>> {
    let mut out = Vec::new();
    while !cur.at_end() {
        if cur.eat_punct(';') {
            continue;
        }
        out.push(parse_stmt(cur)?);
    }
    Ok(out)
}

fn ends_stmt(cur: &Cur) -> bool {
    cur.peek_punct(';')
}

const ITEM_KEYWORDS: &[&str] = &[
    "use",
    "fn",
    "struct",
    "enum",
    "impl",
    "trait",
    "mod",
    "const",
    "static",
    "type",
    "extern",
    "pub",
    "unsafe",
    "macro_rules",
];

fn parse_stmt(cur: &mut Cur) -> PResult<Stmt> {
    let span = cur.span();
    // Outer attributes `#[...]` pass through with the statement they annotate.
    if cur.peek_punct('#') {
        let mut raw = TokenStream::new();
        while cur.peek_punct('#') {
            raw.extend(std::iter::once(cur.next().unwrap()));
            if let Some(g) = cur.eat_group(Delimiter::Bracket) {
                raw.extend(std::iter::once(TokenTree::Group(g)));
            }
        }
        let inner = parse_stmt(cur)?;
        return Ok(match inner {
            Stmt::Raw(ts) => Stmt::Raw(quote::quote!(#raw #ts)),
            Stmt::Item(ts) => Stmt::Item(quote::quote!(#raw #ts)),
            other => {
                // attributes on expression statements: keep as raw prefix
                let _ = other;
                return err(
                    span,
                    "attributes are only supported on items inside #[laila::python] bodies",
                );
            }
        });
    }
    if let Some(TokenTree::Ident(i)) = cur.peek() {
        let kw = i.to_string();
        match kw.as_str() {
            "let" => {
                let header = cur
                    .take_until(|c| c.peek_punct('=') && !c.peek_op_is("==") || c.peek_punct(';'));
                if cur.eat_punct('=') {
                    let value = parse_expr(cur)?;
                    cur.eat_punct(';');
                    return Ok(Stmt::Let {
                        header,
                        value: Some(value),
                    });
                }
                cur.eat_punct(';');
                return Ok(Stmt::Let {
                    header,
                    value: None,
                });
            }
            "return" => {
                let sp = cur.next().unwrap().span();
                if cur.at_end() || cur.peek_punct(';') {
                    cur.eat_punct(';');
                    return Ok(Stmt::Return(None, sp));
                }
                let e = parse_expr(cur)?;
                cur.eat_punct(';');
                return Ok(Stmt::Return(Some(e), sp));
            }
            "raise" => {
                let sp = cur.next().unwrap().span();
                if cur.at_end() || cur.peek_punct(';') {
                    cur.eat_punct(';');
                    return Ok(Stmt::Raise(None, sp));
                }
                let e = parse_expr(cur)?;
                cur.eat_punct(';');
                return Ok(Stmt::Raise(Some(e), sp));
            }
            "del" => {
                let sp = cur.next().unwrap().span();
                let e = parse_expr(cur)?;
                cur.eat_punct(';');
                return Ok(Stmt::Del(e, sp));
            }
            "pass" => {
                cur.next();
                cur.eat_punct(';');
                return Ok(Stmt::Pass);
            }
            "with" | "async" if kw == "with" || cur.peek_ident_n(1, "with") => {
                let is_async = kw == "async";
                if is_async {
                    cur.next();
                }
                let sp = cur.next().unwrap().span();
                let cm = cur.with_flags(true, |c| {
                    c.stop_at_as = true;
                    parse_expr(c)
                })?;
                let target = if cur.eat_ident("as").is_some() {
                    Some(cur.take_until(|c| matches!(c.peek(), Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace)))
                } else {
                    None
                };
                let body = parse_block(&cur.expect_brace()?)?;
                return Ok(Stmt::With {
                    cm,
                    target,
                    body,
                    is_async,
                    span: sp,
                });
            }
            "for" => {
                cur.next();
                let pat = cur.take_until(|c| c.peek_ident("in"));
                cur.expect_ident("in")?;
                let iter = cur.with_flags(true, parse_expr)?;
                let body = parse_block(&cur.expect_brace()?)?;
                return Ok(Stmt::For {
                    pat,
                    iter,
                    body,
                    label: None,
                });
            }
            "while" => {
                cur.next();
                let cond = cur.with_flags(true, parse_expr)?;
                let body = parse_block(&cur.expect_brace()?)?;
                return Ok(Stmt::While { cond, body });
            }
            "loop" => {
                cur.next();
                let body = parse_block(&cur.expect_brace()?)?;
                return Ok(Stmt::Loop { body });
            }
            // `try { } except T as e { } ... else { } finally { }` [P16]
            "try" if matches!(cur.peek_n(1), Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace) =>
            {
                let sp = cur.next().unwrap().span();
                let body = parse_block(&cur.expect_brace()?)?;
                let mut handlers = Vec::new();
                while cur.eat_ident("except").is_some() {
                    let ty = if matches!(cur.peek(), Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace)
                    {
                        None
                    } else {
                        Some(cur.with_flags(true, |c| {
                            c.stop_at_as = true;
                            parse_expr(c)
                        })?)
                    };
                    let name = if cur.eat_ident("as").is_some() {
                        match cur.next() {
                            Some(TokenTree::Ident(i)) => Some(i),
                            _ => return err(cur.span(), "expected a name after `as`"),
                        }
                    } else {
                        None
                    };
                    let hbody = parse_block(&cur.expect_brace()?)?;
                    handlers.push(Handler {
                        ty,
                        name,
                        body: hbody,
                    });
                }
                let orelse = if cur.eat_ident("else").is_some() {
                    Some(parse_block(&cur.expect_brace()?)?)
                } else {
                    None
                };
                let finalbody = if cur.eat_ident("finally").is_some() {
                    Some(parse_block(&cur.expect_brace()?)?)
                } else {
                    None
                };
                if handlers.is_empty() && finalbody.is_none() {
                    return err(sp, "`try` needs at least one `except` or a `finally`");
                }
                return Ok(Stmt::Try {
                    body,
                    handlers,
                    orelse,
                    finalbody,
                    span: sp,
                });
            }
            "break" => {
                cur.next();
                let rest = cur.take_until(ends_stmt);
                cur.eat_punct(';');
                return Ok(Stmt::Break(rest));
            }
            "continue" => {
                cur.next();
                let rest = cur.take_until(ends_stmt);
                cur.eat_punct(';');
                return Ok(Stmt::Continue(rest));
            }
            "if" | "match" | "unsafe" => {
                let e = parse_expr(cur)?;
                let semi = cur.eat_punct(';');
                return Ok(Stmt::Expr { expr: e, semi });
            }
            k if ITEM_KEYWORDS.contains(&k) => {
                return Ok(Stmt::Item(take_item(cur)));
            }
            // A nested `async def` (py2rs emits `async fn name(..) { .. }`).
            "async" if cur.peek_ident_n(1, "fn") => {
                return Ok(Stmt::Item(take_item(cur)));
            }
            // `py_model! { .. }` / `py_class!(..);` declared inside a body are items.
            "py_model" | "py_class" if cur.peek_punct_n(1, '!') => {
                return Ok(Stmt::Item(take_item(cur)));
            }
            _ => {}
        }
    }
    // Expression statement, assignment or augmented assignment.
    let e = parse_expr(cur)?;
    if let Some((op, n)) = cur.peek_op() {
        if op == "=" {
            cur.pos += n;
            let value = parse_expr(cur)?;
            cur.eat_punct(';');
            return Ok(Stmt::Assign { target: e, value });
        }
        if matches!(
            op.as_str(),
            "+=" | "-=" | "*=" | "/=" | "%=" | "|=" | "&=" | "^=" | "<<=" | ">>="
        ) {
            let op_tokens: TokenStream = cur.tts[cur.pos..cur.pos + n].iter().cloned().collect();
            cur.pos += n;
            let value = parse_expr(cur)?;
            cur.eat_punct(';');
            return Ok(Stmt::AugAssign {
                target: e,
                op: op_tokens,
                value,
            });
        }
    }
    let semi = cur.eat_punct(';');
    if !semi && !cur.at_end() && !ends_with_block(&e) {
        return err(
            cur.span(),
            format!(
                "unexpected token `{}` after expression",
                cur.peek().unwrap()
            ),
        );
    }
    Ok(Stmt::Expr { expr: e, semi })
}

fn ends_with_block(e: &Expr) -> bool {
    matches!(e, Expr::Block(_) | Expr::If { .. } | Expr::Match { .. })
}

/// Consume a Rust item (fn/struct/use/...) verbatim: up to a `;` or the item's body group.
fn take_item(cur: &mut Cur) -> TokenStream {
    let mut out = TokenStream::new();
    while let Some(tt) = cur.next() {
        match &tt {
            TokenTree::Punct(p) if p.as_char() == ';' => {
                out.extend(std::iter::once(tt));
                break;
            }
            TokenTree::Group(g) if g.delimiter() == Delimiter::Brace => {
                out.extend(std::iter::once(tt));
                break;
            }
            _ => out.extend(std::iter::once(tt)),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Expressions

pub fn parse_expr(cur: &mut Cur) -> PResult<Expr> {
    if cur.at_end() {
        return err(cur.span(), "expected expression");
    }
    // lambda
    if cur.peek_ident("lambda") {
        let sp = cur.next().unwrap().span();
        let mut params = Vec::new();
        loop {
            match cur.next() {
                Some(TokenTree::Ident(i)) => params.push(i),
                Some(TokenTree::Punct(p)) if p.as_char() == ',' => {}
                Some(TokenTree::Punct(p)) if p.as_char() == ':' => break,
                other => {
                    return err(
                        sp,
                        format!(
                            "bad lambda parameter list near `{}`",
                            other.map(|t| t.to_string()).unwrap_or_default()
                        ),
                    )
                }
            }
        }
        let body = parse_expr(cur)?;
        return Ok(Expr::Lambda {
            params,
            body: Box::new(body),
            span: sp,
        });
    }
    let e = parse_ternary(cur)?;
    Ok(e)
}

fn parse_ternary(cur: &mut Cur) -> PResult<Expr> {
    let e = parse_binary(cur, 0)?;
    // A block-like expression (`if a { .. }` / `match` / `{ .. }`) in
    // statement position ends there, as in Rust: the `if` that follows is
    // the next statement, not a `x if c else y` ternary.
    if cur.peek_ident("if") && !cur.stop_at_for_if && !ends_with_block(&e) {
        cur.next();
        let cond = cur.with_flags(cur.no_struct, |c| {
            c.stop_at_else = true;
            parse_binary(c, 0)
        })?;
        cur.expect_ident("else")?;
        let otherwise = parse_ternary(cur)?;
        return Ok(Expr::Ternary {
            cond: Box::new(cond),
            then: Box::new(e),
            otherwise: Box::new(otherwise),
        });
    }
    Ok(e)
}

/// Binary operator table: (op, precedence, right-assoc).
fn binop_at(cur: &Cur) -> Option<(BinOp, usize, u8, bool)> {
    // returns (op, token count, precedence, right_assoc)
    if let Some(TokenTree::Ident(i)) = cur.peek() {
        let s = i.to_string();
        return match s.as_str() {
            "or" => Some((BinOp::Or, 1, 1, false)),
            "and" => Some((BinOp::And, 1, 2, false)),
            "in" => Some((BinOp::In, 1, 4, false)),
            "not" if cur.peek_ident_n(1, "in") => Some((BinOp::NotIn, 2, 4, false)),
            "is" if cur.peek_ident_n(1, "not") => Some((BinOp::IsNot, 2, 4, false)),
            "is" => Some((BinOp::Is, 1, 4, false)),
            _ => None,
        };
    }
    let (s, n) = cur.peek_op()?;
    let r = match s.as_str() {
        "||" => (BinOp::OrOr, 1, false),
        "&&" => (BinOp::AndAnd, 2, false),
        "==" => (BinOp::Eq, 4, false),
        "!=" => (BinOp::Ne, 4, false),
        "<" => (BinOp::Lt, 4, false),
        "<=" => (BinOp::Le, 4, false),
        ">" => (BinOp::Gt, 4, false),
        ">=" => (BinOp::Ge, 4, false),
        "|" => (BinOp::BitOr, 5, false),
        "^" => (BinOp::BitXor, 6, false),
        "&" => (BinOp::BitAnd, 7, false),
        "<<" => (BinOp::Shl, 8, false),
        ">>" => (BinOp::Shr, 8, false),
        "+" => (BinOp::Add, 9, false),
        "-" => (BinOp::Sub, 9, false),
        "*" => (BinOp::Mul, 10, false),
        "/" => (BinOp::Div, 10, false),
        "%" => (BinOp::Mod, 10, false),
        "**" => (BinOp::Pow, 12, true),
        _ => return None,
    };
    Some((r.0, n, r.1, r.2))
}

fn parse_binary(cur: &mut Cur, min_prec: u8) -> PResult<Expr> {
    // `..` ranges (Rust) at the lowest precedence
    let mut left = if cur.peek_op_is("..") || cur.peek_op_is("..=") {
        Expr::Range {
            start: None,
            end: None,
            inclusive: false,
        }
    } else {
        parse_unary(cur)?
    };
    loop {
        if cur.stop_at_else && cur.peek_ident("else") {
            break;
        }
        if cur.stop_at_for_if && (cur.peek_ident("for") || cur.peek_ident("if")) {
            break;
        }
        if cur.peek_op_is("..") || cur.peek_op_is("..=") {
            if min_prec > 0 {
                break;
            }
            let inclusive = cur.eat_op("..=") || {
                cur.eat_op("..");
                false
            };
            let end = if cur.at_end()
                || cur.peek_punct(',')
                || cur.peek_punct(';')
                || (cur.no_struct
                    && matches!(cur.peek(), Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace))
            {
                None
            } else {
                Some(Box::new(parse_unary(cur)?))
            };
            let start = match left {
                Expr::Range {
                    start: None,
                    end: None,
                    ..
                } => None,
                other => Some(Box::new(other)),
            };
            left = Expr::Range {
                start,
                end,
                inclusive,
            };
            continue;
        }
        let Some((op, n, prec, right_assoc)) = binop_at(cur) else {
            break;
        };
        if prec < min_prec {
            break;
        }
        let span = cur.span();
        cur.pos += n;
        let next_min = if right_assoc { prec } else { prec + 1 };
        let right = if op == BinOp::Pow {
            parse_unary_pow_rhs(cur)?
        } else {
            parse_binary(cur, next_min)?
        };
        left = Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            span,
        };
    }
    Ok(left)
}

fn parse_unary_pow_rhs(cur: &mut Cur) -> PResult<Expr> {
    parse_binary(cur, 11)
}

fn parse_unary(cur: &mut Cur) -> PResult<Expr> {
    let span = cur.span();
    if cur.eat_ident("not").is_some() {
        let e = parse_binary(cur, 3)?; // `not` binds looser than comparisons
        return Ok(Expr::Unary {
            op: UnOp::Not,
            expr: Box::new(e),
            span,
        });
    }
    if cur.eat_ident("await").is_some() {
        let e = parse_unary(cur)?;
        return Ok(Expr::Await(Box::new(e), span));
    }
    if let Some((s, n)) = cur.peek_op() {
        match s.as_str() {
            "-" => {
                cur.pos += n;
                let e = parse_unary(cur)?;
                return Ok(Expr::Unary {
                    op: UnOp::Neg,
                    expr: Box::new(e),
                    span,
                });
            }
            "+" => {
                cur.pos += n;
                let e = parse_unary(cur)?;
                return Ok(Expr::Unary {
                    op: UnOp::Pos,
                    expr: Box::new(e),
                    span,
                });
            }
            "!" => {
                cur.pos += n;
                let e = parse_unary(cur)?;
                return Ok(Expr::Unary {
                    op: UnOp::Bang,
                    expr: Box::new(e),
                    span,
                });
            }
            "~" => {
                cur.pos += n;
                let e = parse_unary(cur)?;
                return Ok(Expr::Unary {
                    op: UnOp::Invert,
                    expr: Box::new(e),
                    span,
                });
            }
            "*" => {
                cur.pos += n;
                let e = parse_unary(cur)?;
                return Ok(Expr::Unary {
                    op: UnOp::Deref,
                    expr: Box::new(e),
                    span,
                });
            }
            "&" => {
                cur.pos += n;
                let mutable = cur.eat_ident("mut").is_some();
                let e = parse_unary(cur)?;
                return Ok(Expr::Ref {
                    mutable,
                    expr: Box::new(e),
                });
            }
            "|" | "||" => return parse_closure(cur, TokenStream::new()),
            _ => {}
        }
    }
    parse_postfix(cur)
}

fn parse_closure(cur: &mut Cur, prefix: TokenStream) -> PResult<Expr> {
    // header: [move] |params|
    let mut header = prefix;
    if cur.eat_op("||") {
        header.extend(quote::quote!(||));
    } else {
        cur.eat_op("|");
        let params = cur.take_until(|c| c.peek_punct('|'));
        cur.eat_op("|");
        header.extend(quote::quote!(| #params |));
    }
    if cur.eat_op("->") {
        let ty = cur.take_until(
            |c| matches!(c.peek(), Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace),
        );
        header.extend(quote::quote!(-> #ty));
    }
    let body = parse_expr(cur)?;
    Ok(Expr::Closure {
        header,
        body: Box::new(body),
    })
}

fn parse_postfix(cur: &mut Cur) -> PResult<Expr> {
    let mut e = parse_primary(cur)?;
    loop {
        // `.name`, `.name(...)`, `.await`, `.0`
        if cur.peek_punct('.') && !cur.peek_op_is("..") && !cur.peek_op_is("..=") {
            let dot_span = cur.span();
            cur.next();
            match cur.next() {
                Some(TokenTree::Ident(name)) => {
                    if name == "await" {
                        e = Expr::RustAwait(Box::new(e));
                        continue;
                    }
                    let turbofish = if cur.peek_op_is("::") {
                        cur.eat_op("::");
                        let g = cur.take_until(|c| c.peek_op_is(">") || c.peek_op_is(">>"));
                        // take_until stops before '>'; consume one '>'
                        let close = cur.next();
                        Some(quote::quote!(#g #close))
                    } else {
                        None
                    };
                    if let Some(g) = cur.eat_group(Delimiter::Parenthesis) {
                        let args = parse_call_args(&g)?;
                        e = Expr::MethodCall {
                            recv: Box::new(e),
                            name,
                            turbofish,
                            args,
                            span: g.span(),
                        };
                    } else {
                        e = Expr::Attr {
                            base: Box::new(e),
                            name,
                        };
                    }
                }
                Some(TokenTree::Literal(l)) => {
                    // tuple field `.0`
                    e = Expr::TupleField {
                        base: Box::new(e),
                        index: l,
                    };
                }
                other => {
                    return err(
                        dot_span,
                        format!(
                            "unexpected `{}` after `.`",
                            other.map(|t| t.to_string()).unwrap_or_default()
                        ),
                    )
                }
            }
            continue;
        }
        if let Some(TokenTree::Group(g)) = cur.peek() {
            match g.delimiter() {
                Delimiter::Parenthesis => {
                    let g = g.clone();
                    cur.next();
                    let args = parse_call_args(&g)?;
                    e = Expr::Call {
                        callee: Box::new(e),
                        args,
                        span: g.span(),
                    };
                    continue;
                }
                Delimiter::Bracket => {
                    let g = g.clone();
                    cur.next();
                    let index = parse_index(&g)?;
                    e = Expr::Index {
                        base: Box::new(e),
                        index,
                        span: g.span(),
                    };
                    continue;
                }
                _ => {}
            }
        }
        if cur.peek_punct('?') {
            cur.next();
            e = Expr::Try(Box::new(e));
            continue;
        }
        if cur.peek_ident("as") && !cur.stop_at_as {
            cur.next();
            let ty = cur.take_until(|c| {
                c.peek_punct(',') || c.peek_punct(';') || c.peek_punct(')') || binop_at(c).is_some() || c.peek_ident("if") || c.peek_ident("else")
                    || (c.no_struct && matches!(c.peek(), Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace))
            });
            e = Expr::Cast {
                expr: Box::new(e),
                ty,
            };
            continue;
        }
        break;
    }
    Ok(e)
}

fn parse_index(g: &Group) -> PResult<IndexKind> {
    let tts: Vec<TokenTree> = g.stream().into_iter().collect();
    // slice if a top-level ':' exists
    // A `:` directly followed by another punct (`[:-1]`) is `Joint`; only
    // `::` (a path) is not a slice separator.
    let mut parts: Vec<Vec<TokenTree>> = vec![Vec::new()];
    let mut i = 0;
    while i < tts.len() {
        match &tts[i] {
            TokenTree::Punct(p) if p.as_char() == ':' => {
                // `s[::-1]` / `s[1::2]`: a `::` that is not between two
                // path segments is two slice separators.
                let joint_colon = p.spacing() == proc_macro2::Spacing::Joint
                    && matches!(tts.get(i + 1), Some(TokenTree::Punct(q)) if q.as_char() == ':')
                    && i > 0
                    && matches!(&tts[i - 1], TokenTree::Ident(_) | TokenTree::Punct(_))
                    && !matches!(&tts[i - 1], TokenTree::Punct(q) if q.as_char() != '>')
                    && matches!(tts.get(i + 2), Some(TokenTree::Ident(_)));
                if joint_colon {
                    parts.last_mut().unwrap().push(tts[i].clone());
                    parts.last_mut().unwrap().push(tts[i + 1].clone());
                    i += 2;
                    continue;
                }
                parts.push(Vec::new());
            }
            tt => parts.last_mut().unwrap().push(tt.clone()),
        }
        i += 1;
    }
    if parts.len() == 1 {
        let mut c = Cur::from_tts(tts.clone());
        let e = parse_expr(&mut c)?;
        if !c.at_end() {
            // `d["b", n]`: a tuple key (Python `d[("b", n)]`).
            if c.peek_punct(',') {
                let tuple = Group::new(Delimiter::Parenthesis, tts.into_iter().collect());
                return Ok(IndexKind::Expr(Box::new(Expr::Tuple(tuple))));
            }
            return err(g.span(), "unexpected tokens in index");
        }
        return Ok(IndexKind::Expr(Box::new(e)));
    }
    let parse_opt = |tts: Vec<TokenTree>| -> PResult<Option<Box<Expr>>> {
        if tts.is_empty() {
            return Ok(None);
        }
        let mut c = Cur::from_tts(tts);
        Ok(Some(Box::new(parse_expr(&mut c)?)))
    };
    let start = parse_opt(parts[0].clone())?;
    let stop = parse_opt(parts[1].clone())?;
    let step = if parts.len() > 2 {
        parse_opt(parts[2].clone())?
    } else {
        None
    };
    Ok(IndexKind::Slice { start, stop, step })
}

pub fn parse_call_args(g: &Group) -> PResult<Vec<CallArg>> {
    let mut out = Vec::new();
    for elem in crate::literal::split_commas(g.stream()) {
        if elem.is_empty() {
            continue;
        }
        let mut c = Cur::from_tts(elem.clone());
        if c.eat_op("**") {
            out.push(CallArg::DoubleStar(parse_expr(&mut c)?));
        } else if c.peek_op_is("*") {
            c.eat_op("*");
            out.push(CallArg::Star(parse_expr(&mut c)?));
        } else if elem.len() >= 3
            && matches!(&elem[0], TokenTree::Ident(_))
            && matches!(&elem[1], TokenTree::Punct(p) if p.as_char() == '='
                && (p.spacing() == proc_macro2::Spacing::Alone
                    // `k=-1` / `k=!x`: the `=` is `Joint` with the unary
                    // operator that follows, not part of `==` / `=>`
                    || matches!(&elem[2], TokenTree::Punct(q) if matches!(q.as_char(), '-' | '!' | '~'))))
        {
            let name = match &elem[0] {
                TokenTree::Ident(i) => i.clone(),
                _ => unreachable!(),
            };
            let mut c2 = Cur::from_tts(elem[2..].to_vec());
            out.push(CallArg::Kw(name, parse_expr(&mut c2)?));
            continue;
        } else {
            // generator expression as sole argument: f(x for x in it)
            if elem
                .iter()
                .any(|t| matches!(t, TokenTree::Ident(i) if i == "for"))
            {
                if let Some(comp) = try_parse_comprehension(&elem, CompKind::Generator, g.span())? {
                    out.push(CallArg::Pos(comp));
                    continue;
                }
            }
            out.push(CallArg::Pos(parse_expr(&mut c)?));
        }
        if !c.at_end() {
            return err(
                c.span(),
                format!("unexpected `{}` in argument", c.peek().unwrap()),
            );
        }
    }
    Ok(out)
}

/// `[elem for pat in iter if cond]` / `{k: v for ...}` / `(elem for ...)`.
fn try_parse_comprehension(tts: &[TokenTree], kind: CompKind, span: Span) -> PResult<Option<Expr>> {
    // find top-level `for`
    let for_pos = tts
        .iter()
        .position(|t| matches!(t, TokenTree::Ident(i) if i == "for"));
    let Some(for_pos) = for_pos else {
        return Ok(None);
    };
    let head = tts[..for_pos].to_vec();
    let rest = tts[for_pos + 1..].to_vec();
    let in_pos = rest
        .iter()
        .position(|t| matches!(t, TokenTree::Ident(i) if i == "in"));
    let Some(in_pos) = in_pos else {
        return Ok(None);
    };
    let pat: TokenStream = rest[..in_pos].iter().cloned().collect();
    let tail = rest[in_pos + 1..].to_vec();
    let mut c = Cur::from_tts(tail);
    c.stop_at_for_if = true;
    let iter = parse_binary(&mut c, 0)?;
    let cond = if c.eat_ident("if").is_some() {
        c.stop_at_for_if = false;
        Some(Box::new(parse_expr(&mut c)?))
    } else {
        None
    };
    if !c.at_end() {
        return err(span, "nested comprehensions are not supported");
    }
    // head: `elem` or `k: v`
    let (elem, value) = match kind {
        CompKind::Dict => {
            let colon = head.iter().position(|t| matches!(t, TokenTree::Punct(p) if p.as_char() == ':' && p.spacing() == proc_macro2::Spacing::Alone));
            match colon {
                Some(ci) => {
                    let mut ck = Cur::from_tts(head[..ci].to_vec());
                    let mut cv = Cur::from_tts(head[ci + 1..].to_vec());
                    (parse_expr(&mut ck)?, Some(Box::new(parse_expr(&mut cv)?)))
                }
                None => {
                    let mut ck = Cur::from_tts(head);
                    (parse_expr(&mut ck)?, None)
                }
            }
        }
        _ => {
            let mut ck = Cur::from_tts(head);
            let e = parse_expr(&mut ck)?;
            if !ck.at_end() {
                return err(span, "bad comprehension element");
            }
            (e, None)
        }
    };
    let kind = match (kind, &value) {
        (CompKind::Dict, None) => CompKind::Set,
        (k, _) => k,
    };
    Ok(Some(Expr::Comprehension {
        kind,
        elem: Box::new(elem),
        value,
        pat,
        iter: Box::new(iter),
        cond,
        span,
    }))
}

fn parse_primary(cur: &mut Cur) -> PResult<Expr> {
    let span = cur.span();
    let Some(tt) = cur.peek().cloned() else {
        return err(span, "expected expression");
    };
    match tt {
        TokenTree::Literal(l) => {
            cur.next();
            Ok(Expr::Lit(l))
        }
        TokenTree::Ident(i) => {
            let s = i.to_string();
            match s.as_str() {
                "None" => {
                    cur.next();
                    Ok(Expr::PyNone(i.span()))
                }
                "True" => {
                    cur.next();
                    Ok(Expr::PyBool(true, i.span()))
                }
                "False" => {
                    cur.next();
                    Ok(Expr::PyBool(false, i.span()))
                }
                "move" => {
                    cur.next();
                    parse_closure(cur, quote::quote!(move))
                }
                "if" => {
                    cur.next();
                    let cond = cur.with_flags(true, parse_expr)?;
                    let then = parse_block(&cur.expect_brace()?)?;
                    let otherwise = if cur.eat_ident("else").is_some() {
                        if cur.peek_ident("if") {
                            Some(Box::new(parse_primary(cur)?))
                        } else {
                            Some(Box::new(Expr::Block(parse_block(&cur.expect_brace()?)?)))
                        }
                    } else {
                        None
                    };
                    Ok(Expr::If {
                        cond: Box::new(cond),
                        then,
                        otherwise,
                    })
                }
                "match" => {
                    cur.next();
                    let scrutinee = cur.with_flags(true, parse_expr)?;
                    let g = cur.expect_brace()?;
                    let arms = parse_match_arms(&g)?;
                    Ok(Expr::Match {
                        scrutinee: Box::new(scrutinee),
                        arms,
                    })
                }
                "unsafe" => {
                    cur.next();
                    let g = cur.expect_brace()?;
                    Ok(Expr::Block(parse_block(&g)?))
                }
                // f-string: `f"..."`
                "f" if matches!(cur.peek_n(1), Some(TokenTree::Literal(l)) if l.to_string().starts_with('"')) =>
                {
                    cur.next();
                    let l = match cur.next() {
                        Some(TokenTree::Literal(l)) => l,
                        _ => unreachable!(),
                    };
                    Ok(Expr::FString(l))
                }
                _ => {
                    cur.next();
                    // `a::b::C` path
                    if cur.peek_op_is("::") {
                        let mut ts = quote::quote!(#i);
                        while cur.peek_op_is("::") {
                            cur.eat_op("::");
                            match cur.next() {
                                Some(TokenTree::Ident(seg)) => ts.extend(quote::quote!(:: #seg)),
                                Some(TokenTree::Punct(p)) if p.as_char() == '<' => {
                                    // turbofish on a path: collect until matching '>'
                                    let mut depth = 1;
                                    let mut inner = TokenStream::new();
                                    while depth > 0 {
                                        match cur.next() {
                                            Some(TokenTree::Punct(q)) if q.as_char() == '<' => {
                                                depth += 1;
                                                inner.extend(std::iter::once(TokenTree::Punct(q)));
                                            }
                                            Some(TokenTree::Punct(q)) if q.as_char() == '>' => {
                                                depth -= 1;
                                                if depth > 0 {
                                                    inner.extend(std::iter::once(
                                                        TokenTree::Punct(q),
                                                    ));
                                                }
                                            }
                                            Some(t) => inner.extend(std::iter::once(t)),
                                            None => return err(span, "unterminated turbofish"),
                                        }
                                    }
                                    ts.extend(quote::quote!(::<#inner>));
                                }
                                other => {
                                    return err(
                                        span,
                                        format!(
                                            "bad path segment `{}`",
                                            other.map(|t| t.to_string()).unwrap_or_default()
                                        ),
                                    )
                                }
                            }
                        }
                        return Ok(Expr::Path(ts));
                    }
                    // macro invocation `name!(...)`
                    if cur.peek_punct('!') && matches!(cur.peek_n(1), Some(TokenTree::Group(_))) {
                        cur.next();
                        let g = match cur.next() {
                            Some(TokenTree::Group(g)) => g,
                            _ => unreachable!(),
                        };
                        return Ok(Expr::Raw(quote::quote!(#i ! #g)));
                    }
                    Ok(Expr::Ident(i))
                }
            }
        }
        TokenTree::Group(g) => {
            cur.next();
            match g.delimiter() {
                Delimiter::Parenthesis => {
                    let elems = crate::literal::split_commas(g.stream());
                    let trailing_comma = g
                        .stream()
                        .into_iter()
                        .last()
                        .map(|t| matches!(t, TokenTree::Punct(p) if p.as_char() == ','))
                        .unwrap_or(false);
                    if elems.is_empty() || elems.len() > 1 || trailing_comma {
                        return Ok(Expr::Tuple(g));
                    }
                    let inner: Vec<TokenTree> = g.stream().into_iter().collect();
                    if inner
                        .iter()
                        .any(|t| matches!(t, TokenTree::Ident(i) if i == "for"))
                    {
                        if let Some(c) =
                            try_parse_comprehension(&inner, CompKind::Generator, g.span())?
                        {
                            return Ok(c);
                        }
                    }
                    let mut c = Cur::new(g.stream());
                    let e = parse_expr(&mut c)?;
                    if !c.at_end() {
                        return err(
                            g.span(),
                            format!(
                                "unexpected `{}` in parenthesised expression",
                                c.peek().unwrap()
                            ),
                        );
                    }
                    Ok(Expr::Paren(Box::new(e)))
                }
                Delimiter::Bracket => {
                    let inner: Vec<TokenTree> = g.stream().into_iter().collect();
                    if inner
                        .iter()
                        .any(|t| matches!(t, TokenTree::Ident(i) if i == "for"))
                    {
                        if let Some(c) = try_parse_comprehension(&inner, CompKind::List, g.span())?
                        {
                            return Ok(c);
                        }
                    }
                    Ok(Expr::List(g))
                }
                Delimiter::Brace => {
                    let inner: Vec<TokenTree> = g.stream().into_iter().collect();
                    if inner
                        .iter()
                        .any(|t| matches!(t, TokenTree::Ident(i) if i == "for"))
                        && !inner
                            .iter()
                            .any(|t| matches!(t, TokenTree::Punct(p) if p.as_char() == ';'))
                    {
                        if let Some(c) = try_parse_comprehension(&inner, CompKind::Dict, g.span())?
                        {
                            return Ok(c);
                        }
                    }
                    if crate::literal::is_py_brace_literal(&g) {
                        Ok(Expr::Dict(g))
                    } else {
                        Ok(Expr::Block(parse_block(&g)?))
                    }
                }
                Delimiter::None => {
                    let mut c = Cur::new(g.stream());
                    let e = parse_expr(&mut c)?;
                    Ok(e)
                }
            }
        }
        TokenTree::Punct(p) => err(p.span(), format!("unexpected `{}`", p.as_char())),
    }
}

fn parse_match_arms(g: &Group) -> PResult<Vec<(TokenStream, Expr)>> {
    let mut cur = Cur::new(g.stream());
    let mut arms = Vec::new();
    while !cur.at_end() {
        let pat = cur.take_until(|c| c.peek_op_is("=>"));
        if !cur.eat_op("=>") {
            return err(g.span(), "expected `=>` in match arm");
        }
        let body = cur.with_flags(false, parse_expr)?;
        cur.eat_punct(',');
        arms.push((pat, body));
    }
    Ok(arms)
}

pub fn ident_is_camel(i: &Ident) -> bool {
    is_camel(i)
}
