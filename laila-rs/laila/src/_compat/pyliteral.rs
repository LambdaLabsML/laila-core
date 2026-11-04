//! `ast.literal_eval(node_or_string)` for Python literal source text.
//!
//! Supported grammar (CPython's `ast.literal_eval` contract): `str` (every
//! quote form, `r`/`u`/`b` prefixes, escapes, implicit concatenation),
//! `bytes`, `int` of any size (underscores, `0x`/`0o`/`0b`), `float`,
//! imaginary literals (`1j`), `None`/`True`/`False`, unary `+`/`-` on a
//! number, `a + bj` / `a - bj` complex literals, tuples (`(1)` is the int,
//! `()`, trailing commas, bare top-level tuples), lists, dicts, sets and
//! `set()`, arbitrarily nested; comments, newlines inside brackets and
//! backslash line continuations.
//!
//! Errors follow CPython 3.14: the source is first *parsed* -- a text that is
//! not a Python expression raises `SyntaxError("<msg> (<unknown>, line N)")`
//! with the tokenizer's messages (`invalid syntax`, `'(' was never closed`,
//! `unterminated string literal (detected at line 1)`, `leading zeros in
//! decimal integer literals are not permitted; ...`, `invalid decimal
//! literal`, `bytes can only contain ASCII literal characters`, `cannot mix
//! bytes and nonbytes literals`, ...) -- and a valid expression that is not a
//! literal raises `ValueError("malformed node or string on line N: <node repr>")`
//! with CPython's `repr(node)` (`Name(id='foo', ctx=Load())`,
//! `BinOp(left=..., op=Add(), right=...)`, depth-truncated like CPython).
//! Unhashable dict keys / set elements raise CPython's `TypeError`.
//!
//! Known limits: `\N{NAME}` escapes need the Unicode name table and raise
//! `SyntaxError`; a lone surrogate (`'\ud800'`) decodes to U+FFFD; `...`,
//! comprehensions, `lambda`, `not`, `if`/`else`, `**` unpacking and keyword
//! call arguments are reported as `SyntaxError("invalid syntax")` rather than
//! as the `ValueError` CPython gives for those (valid but non-literal) nodes.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use num_bigint::BigInt;
use num_traits::ToPrimitive;

use crate::_compat::ndarray::Complex;
use crate::python::errors::ExcKind;
use crate::python::object::{Arg, IntoArg, PyMap, PySetInner};
use crate::python::sync::RwLock;
use crate::python::{LailaError, Result};

/// `ast.literal_eval(src)`
pub fn literal_eval(src: &str) -> Result<Arg> {
    // `node_or_string.lstrip(" \t")`
    let src = src.trim_start_matches([' ', '\t']);
    let chars: Vec<char> = src.chars().collect();
    if chars.iter().all(|c| c.is_whitespace()) {
        return Err(syntax_error("invalid syntax", 0));
    }
    let mut p = Parser {
        s: chars,
        i: 0,
        brackets: Vec::new(),
    };
    p.ws_lines();
    let node = p.expr()?;
    // bare top-level tuple: `1, 2`
    let node = if p.peek_ws() == Some(',') {
        let line = node.line;
        let mut items = alloc::vec![node];
        while p.peek_ws() == Some(',') {
            p.i += 1;
            p.ws();
            match p.peek() {
                None | Some('\n') | Some('#') => break,
                _ => items.push(p.expr()?),
            }
        }
        Node {
            kind: Kind::Tuple(items),
            line,
        }
    } else {
        node
    };
    p.ws_lines();
    if let Some(c) = p.peek() {
        return Err(p.unexpected_char(c));
    }
    convert(&node)
}

fn syntax_error(msg: &str, line: usize) -> LailaError {
    LailaError::new(
        ExcKind::SyntaxError,
        alloc::format!("{msg} (<unknown>, line {line})"),
    )
}

// --------------------------------------------------------------------------
// AST
// --------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum UnOp {
    UAdd,
    USub,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BinOpKind {
    Add,
    Sub,
}

enum Kind {
    /// `Constant(value=...)`: int, float, complex, str, bytes, bool, None.
    Const(Arg),
    Name(String),
    Call(Box<Node>, Vec<Node>),
    Subscript(Box<Node>, Box<Node>),
    Attribute(Box<Node>, String),
    Unary(UnOp, Box<Node>),
    BinOp(Box<Node>, BinOpKind, Box<Node>),
    Tuple(Vec<Node>),
    List(Vec<Node>),
    Set(Vec<Node>),
    Dict(Vec<(Node, Node)>),
    /// An f-string (never a literal).
    JoinedStr(String),
}

struct Node {
    kind: Kind,
    line: usize,
}

/// CPython 3.14 `repr(node)`: fields in order, nested nodes beyond depth 2
/// collapsed to `Name(...)`.
fn dump(n: &Node, depth: usize) -> String {
    fn leaf(name: &str, depth: usize) -> String {
        if depth >= 3 {
            alloc::format!("{name}(...)")
        } else {
            alloc::format!("{name}()")
        }
    }
    fn list(nodes: &[Node], depth: usize) -> String {
        let parts: Vec<String> = nodes.iter().map(|n| dump(n, depth)).collect();
        alloc::format!("[{}]", parts.join(", "))
    }
    let name = match &n.kind {
        Kind::Const(_) => "Constant",
        Kind::Name(_) => "Name",
        Kind::Call(..) => "Call",
        Kind::Subscript(..) => "Subscript",
        Kind::Attribute(..) => "Attribute",
        Kind::Unary(..) => "UnaryOp",
        Kind::BinOp(..) => "BinOp",
        Kind::Tuple(_) => "Tuple",
        Kind::List(_) => "List",
        Kind::Set(_) => "Set",
        Kind::Dict(_) => "Dict",
        Kind::JoinedStr(_) => "JoinedStr",
    };
    if depth >= 3 {
        return alloc::format!("{name}(...)");
    }
    let d = depth + 1;
    match &n.kind {
        Kind::Const(v) => alloc::format!("Constant(value={}, kind=None)", v.__repr__()),
        Kind::Name(id) => alloc::format!(
            "Name(id={}, ctx={})",
            crate::python::pyrepr::str_repr(id),
            leaf("Load", d)
        ),
        Kind::Call(f, args) => alloc::format!(
            "Call(func={}, args={}, keywords=[])",
            dump(f, d),
            list(args, d)
        ),
        Kind::Subscript(v, s) => {
            alloc::format!(
                "Subscript(value={}, slice={}, ctx={})",
                dump(v, d),
                dump(s, d),
                leaf("Load", d)
            )
        }
        Kind::Attribute(v, attr) => alloc::format!(
            "Attribute(value={}, attr={}, ctx={})",
            dump(v, d),
            crate::python::pyrepr::str_repr(attr),
            leaf("Load", d)
        ),
        Kind::Unary(op, operand) => alloc::format!(
            "UnaryOp(op={}, operand={})",
            leaf(if *op == UnOp::USub { "USub" } else { "UAdd" }, d),
            dump(operand, d)
        ),
        Kind::BinOp(l, op, r) => alloc::format!(
            "BinOp(left={}, op={}, right={})",
            dump(l, d),
            leaf(if *op == BinOpKind::Add { "Add" } else { "Sub" }, d),
            dump(r, d)
        ),
        Kind::Tuple(elts) => {
            alloc::format!("Tuple(elts={}, ctx={})", list(elts, d), leaf("Load", d))
        }
        Kind::List(elts) => alloc::format!("List(elts={}, ctx={})", list(elts, d), leaf("Load", d)),
        Kind::Set(elts) => alloc::format!("Set(elts={})", list(elts, d)),
        Kind::Dict(pairs) => {
            let keys: Vec<String> = pairs.iter().map(|(k, _)| dump(k, d)).collect();
            let values: Vec<String> = pairs.iter().map(|(_, v)| dump(v, d)).collect();
            alloc::format!(
                "Dict(keys=[{}], values=[{}])",
                keys.join(", "),
                values.join(", ")
            )
        }
        Kind::JoinedStr(text) => alloc::format!(
            "JoinedStr(values=[Constant(value={}, kind=None)])",
            crate::python::pyrepr::str_repr(text)
        ),
    }
}

// --------------------------------------------------------------------------
// `ast.literal_eval._convert`
// --------------------------------------------------------------------------

fn malformed(n: &Node) -> LailaError {
    LailaError::value_error(alloc::format!(
        "malformed node or string on line {}: {}",
        n.line,
        dump(n, 0)
    ))
}

fn is_num(v: &Arg) -> bool {
    matches!(v, Arg::Int(_) | Arg::Long(_) | Arg::Float(_)) || v.downcast_ref::<Complex>().is_some()
}

/// `_convert_num`
fn convert_num(n: &Node) -> Result<Arg> {
    match &n.kind {
        Kind::Const(v) if is_num(v) => Ok(v.clone()),
        _ => Err(malformed(n)),
    }
}

/// `_convert_signed_num`
fn convert_signed_num(n: &Node) -> Result<Arg> {
    if let Kind::Unary(op, operand) = &n.kind {
        let v = convert_num(operand)?;
        return Ok(match op {
            UnOp::UAdd => v,
            UnOp::USub => negate(&v),
        });
    }
    convert_num(n)
}

fn negate(v: &Arg) -> Arg {
    match v {
        Arg::Int(i) => match i.checked_neg() {
            Some(n) => Arg::Int(n),
            None => Arg::from_bigint(-BigInt::from(*i)),
        },
        Arg::Long(l) => Arg::from_bigint(-(**l).clone()),
        Arg::Float(f) => Arg::Float(-f),
        other => match other.downcast_ref::<Complex>() {
            Some(c) => Complex::new(-c.real, -c.imag).into_arg(),
            None => other.clone(),
        },
    }
}

fn unhashable(k: &Arg, what: &str) -> LailaError {
    LailaError::type_error(alloc::format!(
        "cannot use '{}' as a {what} (unhashable type: '{}')",
        k.type_name(),
        k.type_name()
    ))
}

/// `_convert`
fn convert(n: &Node) -> Result<Arg> {
    match &n.kind {
        Kind::Const(v) => Ok(v.clone()),
        Kind::Tuple(elts) => {
            let items: Result<Vec<Arg>> = elts.iter().map(convert).collect();
            Ok(Arg::Tuple(items?.into()))
        }
        Kind::List(elts) => {
            let items: Result<Vec<Arg>> = elts.iter().map(convert).collect();
            Ok(Arg::List(Arc::new(RwLock::new(items?))))
        }
        Kind::Set(elts) => {
            let mut set = PySetInner::default();
            for e in elts {
                let v = convert(e)?;
                v.__hash__().map_err(|_| unhashable(&v, "set element"))?;
                set.insert(v);
            }
            Ok(Arg::Set(Arc::new(RwLock::new(set))))
        }
        Kind::Call(f, args)
            if matches!(&f.kind, Kind::Name(id) if id == "set") && args.is_empty() =>
        {
            Ok(Arg::Set(Arc::new(RwLock::new(PySetInner::default()))))
        }
        Kind::Dict(pairs) => {
            let mut map: PyMap<Arg, Arg> = PyMap::default();
            for (k, v) in pairs {
                let k = convert(k)?;
                let v = convert(v)?;
                k.__hash__().map_err(|_| unhashable(&k, "dict key"))?;
                map.insert(k, v);
            }
            Ok(Arg::Dict(Arc::new(RwLock::new(map))))
        }
        Kind::BinOp(l, op, r) => {
            let left = convert_signed_num(l)?;
            let right = convert_num(r)?;
            let lf = match &left {
                Arg::Int(i) => Some(*i as f64),
                Arg::Long(b) => b.to_f64(),
                Arg::Float(f) => Some(*f),
                _ => None,
            };
            if let (Some(lf), Some(c)) = (lf, right.downcast_ref::<Complex>()) {
                // `left + right` / `left - right` with complex arithmetic
                return Ok(match op {
                    BinOpKind::Add => Complex::new(lf + c.real, 0.0 + c.imag),
                    BinOpKind::Sub => Complex::new(lf - c.real, 0.0 - c.imag),
                }
                .into_arg());
            }
            convert_signed_num(n)
        }
        _ => convert_signed_num(n),
    }
}

// --------------------------------------------------------------------------
// Parser (the subset of the Python tokenizer/grammar literals need)
// --------------------------------------------------------------------------

struct Parser {
    s: Vec<char>,
    i: usize,
    /// Open brackets (`(`, `[`, `{`) with their line, innermost last.
    brackets: Vec<(char, usize)>,
}

fn closing_of(open: char) -> char {
    match open {
        '(' => ')',
        '[' => ']',
        _ => '}',
    }
}

impl Parser {
    fn peek(&self) -> Option<char> {
        self.s.get(self.i).copied()
    }
    fn peek_at(&self, off: usize) -> Option<char> {
        self.s.get(self.i + off).copied()
    }
    /// 1-based line of position `pos` (a trailing newline belongs to its line).
    fn line_at(&self, pos: usize) -> usize {
        let pos = pos.min(self.s.len().saturating_sub(1));
        self.s[..pos].iter().filter(|c| **c == '\n').count() + 1
    }
    fn line(&self) -> usize {
        self.line_at(self.i)
    }
    fn err(&self, msg: &str) -> LailaError {
        syntax_error(msg, self.line())
    }

    /// Skips blanks, comments and backslash continuations; newlines only
    /// inside brackets (a newline at depth 0 ends the expression).
    fn ws(&mut self) {
        loop {
            match self.peek() {
                Some(' ' | '\t' | '\u{c}') => self.i += 1,
                Some('\\') if self.peek_at(1) == Some('\n') => self.i += 2,
                Some('\\') if self.peek_at(1) == Some('\r') && self.peek_at(2) == Some('\n') => {
                    self.i += 3
                }
                Some('#') => {
                    while !matches!(self.peek(), None | Some('\n')) {
                        self.i += 1;
                    }
                }
                Some('\n' | '\r') if !self.brackets.is_empty() => self.i += 1,
                _ => break,
            }
        }
    }
    /// Skips everything including newlines (before / after the expression).
    fn ws_lines(&mut self) {
        loop {
            self.ws();
            match self.peek() {
                Some('\n' | '\r') => self.i += 1,
                _ => break,
            }
        }
    }
    fn peek_ws(&mut self) -> Option<char> {
        self.ws();
        self.peek()
    }

    /// The `SyntaxError` for an unexpected character `c` at the cursor.
    fn unexpected_char(&self, c: char) -> LailaError {
        match c {
            ')' | ']' | '}' => match self.brackets.last() {
                None => self.err(&alloc::format!("unmatched '{c}'")),
                Some((open, _)) if closing_of(*open) != c => self.err(&alloc::format!(
                    "closing parenthesis '{c}' does not match opening parenthesis '{open}'"
                )),
                Some(_) => self.err("invalid syntax"),
            },
            _ => self.err("invalid syntax"),
        }
    }
    /// EOF inside brackets: `'(' was never closed` (innermost).
    fn never_closed(&self) -> LailaError {
        match self.brackets.last() {
            Some((open, line)) => syntax_error(&alloc::format!("'{open}' was never closed"), *line),
            None => self.err("invalid syntax"),
        }
    }

    fn starts_value(&self, c: char) -> bool {
        c == '('
            || c == '['
            || c == '{'
            || c == '\''
            || c == '"'
            || c == '-'
            || c == '+'
            || c == '.'
            || c.is_ascii_digit()
            || c.is_alphabetic()
            || c == '_'
    }

    /// Inside brackets, after an element: `,` / closer expected.
    fn seq_separator_error(&self) -> LailaError {
        match self.peek() {
            None => self.never_closed(),
            Some(c) if self.starts_value(c) => {
                self.err("invalid syntax. Perhaps you forgot a comma?")
            }
            Some(c) => self.unexpected_char(c),
        }
    }

    /// `expr := operand (('+' | '-') operand)*`
    fn expr(&mut self) -> Result<Node> {
        let mut left = self.operand()?;
        loop {
            match self.peek_ws() {
                Some(c @ ('+' | '-')) => {
                    self.i += 1;
                    let op = if c == '+' {
                        BinOpKind::Add
                    } else {
                        BinOpKind::Sub
                    };
                    let right = self.operand()?;
                    let line = left.line;
                    left = Node {
                        kind: Kind::BinOp(Box::new(left), op, Box::new(right)),
                        line,
                    };
                }
                _ => return Ok(left),
            }
        }
    }

    /// `operand := ('+' | '-') operand | primary trailer*`
    fn operand(&mut self) -> Result<Node> {
        self.ws();
        let line = self.line();
        let Some(c) = self.peek() else {
            return Err(self.never_closed());
        };
        if c == '+' || c == '-' {
            self.i += 1;
            let inner = self.operand()?;
            let op = if c == '+' { UnOp::UAdd } else { UnOp::USub };
            return Ok(Node {
                kind: Kind::Unary(op, Box::new(inner)),
                line,
            });
        }
        let mut node = self.primary()?;
        // trailers: call / subscript / attribute (valid Python, never literals)
        loop {
            match self.peek_ws() {
                Some('(') => {
                    let args = self.bracketed('(')?;
                    node = Node {
                        kind: Kind::Call(Box::new(node), args),
                        line,
                    };
                }
                Some('[') => {
                    let mut items = self.bracketed('[')?;
                    let slice = if items.len() == 1 {
                        items.pop().unwrap()
                    } else {
                        Node {
                            kind: Kind::Tuple(items),
                            line,
                        }
                    };
                    node = Node {
                        kind: Kind::Subscript(Box::new(node), Box::new(slice)),
                        line,
                    };
                }
                Some('.')
                    if self
                        .peek_at(1)
                        .is_some_and(|c| c.is_alphabetic() || c == '_') =>
                {
                    self.i += 1;
                    let attr = self.identifier();
                    node = Node {
                        kind: Kind::Attribute(Box::new(node), attr),
                        line,
                    };
                }
                _ => return Ok(node),
            }
        }
    }

    fn identifier(&mut self) -> String {
        let mut id = String::new();
        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '_' {
                id.push(c);
                self.i += 1;
            } else {
                break;
            }
        }
        id
    }

    /// Is the cursor at a string literal (optional prefix letters + quote)?
    fn at_string(&self) -> bool {
        let mut k = 0;
        while let Some(c) = self.peek_at(k) {
            if c.is_ascii_alphabetic() && k < 3 {
                k += 1;
            } else {
                return c == '\'' || c == '"';
            }
        }
        false
    }

    fn primary(&mut self) -> Result<Node> {
        let line = self.line();
        let c = self.peek().expect("operand checked for EOF");
        if c == '(' {
            self.push_bracket('(');
            self.ws();
            if self.peek() == Some(')') {
                self.pop_bracket();
                return Ok(Node {
                    kind: Kind::Tuple(Vec::new()),
                    line,
                });
            }
            let first = self.expr()?;
            match self.peek_ws() {
                Some(')') => {
                    self.pop_bracket();
                    return Ok(first); // parenthesised expression
                }
                Some(',') => {
                    let mut items = alloc::vec![first];
                    items.extend(self.rest_of_seq(')')?);
                    return Ok(Node {
                        kind: Kind::Tuple(items),
                        line,
                    });
                }
                _ => return Err(self.seq_separator_error()),
            }
        }
        if c == '[' {
            let items = self.bracketed('[')?;
            return Ok(Node {
                kind: Kind::List(items),
                line,
            });
        }
        if c == '{' {
            return self.dict_or_set();
        }
        if self.at_string() {
            return self.strings();
        }
        if c.is_ascii_digit() || (c == '.' && self.peek_at(1).is_some_and(|d| d.is_ascii_digit())) {
            let v = self.number()?;
            return Ok(Node {
                kind: Kind::Const(v),
                line,
            });
        }
        if c.is_alphabetic() || c == '_' {
            let id = self.identifier();
            let kind = match id.as_str() {
                "True" => Kind::Const(Arg::Bool(true)),
                "False" => Kind::Const(Arg::Bool(false)),
                "None" => Kind::Const(Arg::None),
                _ => Kind::Name(id),
            };
            return Ok(Node { kind, line });
        }
        Err(self.unexpected_char(c))
    }

    fn push_bracket(&mut self, open: char) {
        let line = self.line();
        self.brackets.push((open, line));
        self.i += 1;
    }
    fn pop_bracket(&mut self) {
        self.brackets.pop();
        self.i += 1;
    }

    /// `open expr (',' expr)* [','] close` starting *at* the opening bracket.
    fn bracketed(&mut self, open: char) -> Result<Vec<Node>> {
        self.push_bracket(open);
        let close = closing_of(open);
        self.ws();
        if self.peek() == Some(close) {
            self.pop_bracket();
            return Ok(Vec::new());
        }
        let first = self.expr()?;
        let mut items = alloc::vec![first];
        match self.peek_ws() {
            Some(c) if c == close => {
                self.pop_bracket();
            }
            Some(',') => items.extend(self.rest_of_seq(close)?),
            _ => return Err(self.seq_separator_error()),
        }
        Ok(items)
    }

    /// Cursor on a `,` after the first element: the remaining elements up to
    /// and including the closing bracket.
    fn rest_of_seq(&mut self, close: char) -> Result<Vec<Node>> {
        let mut items = Vec::new();
        loop {
            self.i += 1; // ','
            self.ws();
            match self.peek() {
                Some(c) if c == close => {
                    self.pop_bracket();
                    return Ok(items);
                }
                Some(',') => return Err(self.err("invalid syntax")),
                None => return Err(self.never_closed()),
                _ => items.push(self.expr()?),
            }
            match self.peek_ws() {
                Some(c) if c == close => {
                    self.pop_bracket();
                    return Ok(items);
                }
                Some(',') => continue,
                _ => return Err(self.seq_separator_error()),
            }
        }
    }

    fn dict_or_set(&mut self) -> Result<Node> {
        let line = self.line();
        self.push_bracket('{');
        self.ws();
        if self.peek() == Some('}') {
            self.pop_bracket();
            return Ok(Node {
                kind: Kind::Dict(Vec::new()),
                line,
            });
        }
        if self.peek() == Some(':') {
            return Err(self.err("invalid syntax"));
        }
        let first = self.expr()?;
        match self.peek_ws() {
            Some(':') => {
                self.i += 1;
                let v = self.dict_value()?;
                let mut pairs = alloc::vec![(first, v)];
                loop {
                    match self.peek_ws() {
                        Some('}') => break,
                        Some(',') => {
                            self.i += 1;
                            match self.peek_ws() {
                                Some('}') => break,
                                Some(',') => return Err(self.err("invalid syntax")),
                                None => return Err(self.never_closed()),
                                _ => {}
                            }
                            let k = self.expr()?;
                            match self.peek_ws() {
                                Some(':') => self.i += 1,
                                Some(',' | '}') => {
                                    return Err(self.err("':' expected after dictionary key"))
                                }
                                _ => return Err(self.seq_separator_error()),
                            }
                            let v = self.dict_value()?;
                            pairs.push((k, v));
                        }
                        _ => return Err(self.seq_separator_error()),
                    }
                }
                self.pop_bracket();
                Ok(Node {
                    kind: Kind::Dict(pairs),
                    line,
                })
            }
            Some('}') => {
                self.pop_bracket();
                Ok(Node {
                    kind: Kind::Set(alloc::vec![first]),
                    line,
                })
            }
            Some(',') => {
                let mut items = alloc::vec![first];
                items.extend(self.rest_of_seq('}')?);
                Ok(Node {
                    kind: Kind::Set(items),
                    line,
                })
            }
            _ => Err(self.seq_separator_error()),
        }
    }

    /// The value after `key:` in a dict display.
    fn dict_value(&mut self) -> Result<Node> {
        match self.peek_ws() {
            Some(',' | '}') => Err(self.err("expression expected after dictionary key and ':'")),
            None => Err(self.never_closed()),
            _ => self.expr(),
        }
    }

    // ---- numbers -----------------------------------------------------------

    /// `[0-9](_?[0-9])*` continuing after one digit was consumed.
    fn decimal_tail(&mut self, buf: &mut String) -> Result<()> {
        loop {
            match self.peek() {
                Some(c) if c.is_ascii_digit() => {
                    buf.push(c);
                    self.i += 1;
                }
                Some('_') => {
                    if self.peek_at(1).is_some_and(|c| c.is_ascii_digit()) {
                        self.i += 1;
                    } else {
                        return Err(self.err("invalid decimal literal"));
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    fn number(&mut self) -> Result<Arg> {
        let c = self.peek().expect("number start");
        let mut digits = String::new();
        if c == '0' {
            if let Some(p @ ('x' | 'X' | 'o' | 'O' | 'b' | 'B')) = self.peek_at(1) {
                let (radix, what) = match p {
                    'x' | 'X' => (16, "hexadecimal"),
                    'o' | 'O' => (8, "octal"),
                    _ => (2, "binary"),
                };
                self.i += 2;
                loop {
                    match self.peek() {
                        Some('_') => {
                            self.i += 1;
                            if !self.peek().is_some_and(|c| c.is_digit(radix)) {
                                return Err(self.err(&alloc::format!("invalid {what} literal")));
                            }
                        }
                        Some(c) if c.is_digit(radix) => {
                            digits.push(c);
                            self.i += 1;
                        }
                        Some(c) if c.is_ascii_digit() => {
                            return Err(
                                self.err(&alloc::format!("invalid digit '{c}' in {what} literal"))
                            );
                        }
                        Some(c) if c.is_alphanumeric() || c == '_' => {
                            return Err(self.err(&alloc::format!("invalid {what} literal")));
                        }
                        _ => break,
                    }
                }
                if digits.is_empty() {
                    return Err(self.err(&alloc::format!("invalid {what} literal")));
                }
                let big = BigInt::parse_bytes(digits.as_bytes(), radix).expect("validated digits");
                return Ok(Arg::from_bigint(big));
            }
        }
        // decimal integer part
        let mut is_float = false;
        if c.is_ascii_digit() {
            digits.push(c);
            self.i += 1;
            self.decimal_tail(&mut digits)?;
        }
        // fraction
        if self.peek() == Some('.') {
            is_float = true;
            digits.push('.');
            self.i += 1;
            if let Some(d) = self.peek() {
                if d.is_ascii_digit() {
                    digits.push(d);
                    self.i += 1;
                    self.decimal_tail(&mut digits)?;
                } else if d == '_' {
                    return Err(self.err("invalid decimal literal"));
                }
            }
        }
        // exponent
        if let Some('e' | 'E') = self.peek() {
            let mut k = 1;
            if let Some('+' | '-') = self.peek_at(1) {
                k = 2;
            }
            if self.peek_at(k).is_some_and(|c| c.is_ascii_digit()) {
                is_float = true;
                digits.push('e');
                if k == 2 {
                    digits.push(self.peek_at(1).unwrap());
                }
                self.i += k;
                let d = self.peek().unwrap();
                digits.push(d);
                self.i += 1;
                self.decimal_tail(&mut digits)?;
            } else {
                return Err(self.err("invalid decimal literal"));
            }
        }
        // imaginary
        let is_imag = matches!(self.peek(), Some('j' | 'J'));
        if is_imag {
            self.i += 1;
        }
        if self.peek().is_some_and(|c| c.is_alphanumeric() || c == '_') {
            return Err(self.err("invalid decimal literal"));
        }
        if is_imag {
            let f: f64 = digits.parse().expect("validated float literal");
            return Ok(Complex::new(0.0, f).into_arg());
        }
        if is_float {
            let f: f64 = digits.parse().expect("validated float literal");
            return Ok(Arg::Float(f));
        }
        if digits.len() > 1 && digits.starts_with('0') && digits.bytes().any(|b| b != b'0') {
            return Err(self.err(
                "leading zeros in decimal integer literals are not permitted; use an 0o prefix for octal integers",
            ));
        }
        let big: BigInt = digits.parse().expect("validated int literal");
        Ok(Arg::from_bigint(big))
    }

    // ---- strings -----------------------------------------------------------

    /// One or more adjacent string literals (implicit concatenation).
    fn strings(&mut self) -> Result<Node> {
        let line = self.line();
        let (mut is_bytes, mut is_f, mut text, mut bytes) =
            (false, false, String::new(), Vec::<u8>::new());
        let mut first = true;
        loop {
            let (b, f, t, by) = self.string_literal()?;
            if first {
                is_bytes = b;
                first = false;
            } else if b != is_bytes {
                return Err(self.err("cannot mix bytes and nonbytes literals"));
            }
            is_f |= f;
            text.push_str(&t);
            bytes.extend_from_slice(&by);
            self.ws();
            if !self.at_string() {
                break;
            }
        }
        let kind = if is_f {
            Kind::JoinedStr(text)
        } else if is_bytes {
            Kind::Const(Arg::Bytes(bytes.into()))
        } else {
            Kind::Const(Arg::Str(text.into()))
        };
        Ok(Node { kind, line })
    }

    /// One string literal -> `(is_bytes, is_fstring, text, bytes)`.
    fn string_literal(&mut self) -> Result<(bool, bool, String, Vec<u8>)> {
        let start_line = self.line();
        // prefix
        let mut prefix = String::new();
        while let Some(c) = self.peek() {
            if c.is_ascii_alphabetic() {
                prefix.push(c.to_ascii_lowercase());
                self.i += 1;
            } else {
                break;
            }
        }
        let (mut is_bytes, mut is_raw, mut is_f, mut is_u) = (false, false, false, false);
        for c in prefix.chars() {
            let flag = match c {
                'b' => &mut is_bytes,
                'r' => &mut is_raw,
                'f' => &mut is_f,
                'u' => &mut is_u,
                _ => return Err(self.err("invalid syntax")),
            };
            if *flag {
                return Err(self.err("invalid syntax"));
            }
            *flag = true;
        }
        if is_u && prefix.len() > 1 {
            let other = prefix.chars().find(|c| *c != 'u').unwrap();
            return Err(self.err(&alloc::format!(
                "'u' and '{other}' prefixes are incompatible"
            )));
        }
        if is_bytes && is_f {
            return Err(self.err("'b' and 'f' prefixes are incompatible"));
        }
        let q = self.peek().expect("quote");
        let triple = self.peek_at(1) == Some(q) && self.peek_at(2) == Some(q);
        self.i += if triple { 3 } else { 1 };
        let body_start = self.i;
        // find the end of the body (raw source), honouring `\x` pairs
        let mut end = self.i;
        loop {
            match self.s.get(end) {
                None => {
                    let detected = self.line_at(end);
                    let e = end.min(self.s.len());
                    let msg = if triple {
                        alloc::format!("unterminated triple-quoted string literal (detected at line {detected})")
                    } else if e >= body_start + 2 && self.s[e - 2] == '\\' && self.s[e - 1] == q {
                        alloc::format!(
                            "unterminated string literal (detected at line {detected}); perhaps you escaped the end quote?"
                        )
                    } else {
                        alloc::format!("unterminated string literal (detected at line {detected})")
                    };
                    return Err(syntax_error(&msg, start_line));
                }
                Some('\n') if !triple => {
                    let detected = self.line_at(end);
                    return Err(syntax_error(
                        &alloc::format!(
                            "unterminated string literal (detected at line {detected})"
                        ),
                        start_line,
                    ));
                }
                Some('\\') => end += 2,
                Some(c) if *c == q => {
                    if !triple
                        || (self.s.get(end + 1) == Some(&q) && self.s.get(end + 2) == Some(&q))
                    {
                        break;
                    }
                    end += 1;
                }
                Some(_) => end += 1,
            }
        }
        let end = end.min(self.s.len());
        let body: &[char] = &self.s[body_start..end];
        self.i = end + if triple { 3 } else { 1 };
        if is_bytes && body.iter().any(|c| !c.is_ascii()) {
            return Err(syntax_error(
                "bytes can only contain ASCII literal characters",
                start_line,
            ));
        }
        if is_raw {
            let text: String = body.iter().collect();
            let bytes: Vec<u8> = if is_bytes {
                body.iter().map(|c| *c as u8).collect()
            } else {
                Vec::new()
            };
            return Ok((is_bytes, is_f, text, bytes));
        }
        if is_bytes {
            let out = decode_bytes_escapes(body).map_err(|m| syntax_error(&m, start_line))?;
            return Ok((true, is_f, String::new(), out));
        }
        let out = decode_str_escapes(body).map_err(|m| syntax_error(&m, start_line))?;
        Ok((false, is_f, out, Vec::new()))
    }
}

fn hex_value(chars: &[char]) -> Option<u32> {
    let mut v = 0u32;
    for c in chars {
        v = (v << 4) | c.to_digit(16)?;
    }
    Some(v)
}

/// `codecs.decode(body, "unicode_escape")` as the compiler applies it to a
/// `str` literal body. Errors are the `(unicode error) ...` texts; positions
/// are UTF-8 byte offsets into the body, as in CPython.
fn decode_str_escapes(body: &[char]) -> core::result::Result<String, String> {
    let mut out = String::new();
    let mut i = 0;
    let mut byte_pos = 0usize;
    let truncated = |byte_pos: usize, consumed: usize, what: &str| {
        alloc::format!(
            "(unicode error) 'unicodeescape' codec can't decode bytes in position {}-{}: {what}",
            byte_pos,
            byte_pos + consumed - 1
        )
    };
    while i < body.len() {
        let c = body[i];
        if c != '\\' {
            out.push(c);
            byte_pos += c.len_utf8();
            i += 1;
            continue;
        }
        let Some(&e) = body.get(i + 1) else {
            // a trailing lone backslash cannot occur (it would escape the quote)
            out.push('\\');
            byte_pos += 1;
            i += 1;
            continue;
        };
        i += 2;
        let mut consumed = 2;
        match e {
            '\n' => {}
            '\\' => out.push('\\'),
            '\'' => out.push('\''),
            '"' => out.push('"'),
            'a' => out.push('\u{7}'),
            'b' => out.push('\u{8}'),
            'f' => out.push('\u{c}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'v' => out.push('\u{b}'),
            '0'..='7' => {
                let mut v = e.to_digit(8).unwrap();
                let mut n = 1;
                while n < 3 && body.get(i).is_some_and(|c| ('0'..='7').contains(c)) {
                    v = v * 8 + body[i].to_digit(8).unwrap();
                    i += 1;
                    n += 1;
                    consumed += 1;
                }
                out.push(char::from_u32(v).unwrap_or('\u{fffd}'));
            }
            'x' | 'u' | 'U' => {
                let (need, what) = match e {
                    'x' => (2, "truncated \\xXX escape"),
                    'u' => (4, "truncated \\uXXXX escape"),
                    _ => (8, "truncated \\UXXXXXXXX escape"),
                };
                let avail = body[i..]
                    .iter()
                    .take(need)
                    .take_while(|c| c.is_ascii_hexdigit())
                    .count();
                if avail < need {
                    return Err(truncated(byte_pos, consumed + avail, what));
                }
                let v = hex_value(&body[i..i + need]).unwrap();
                i += need;
                consumed += need;
                match char::from_u32(v) {
                    Some(ch) => out.push(ch),
                    None if v > 0x10ffff => {
                        return Err(truncated(byte_pos, consumed, "illegal Unicode character"));
                    }
                    // lone surrogate: legal in Python, not in Rust
                    None => out.push('\u{fffd}'),
                }
            }
            'N' => {
                let close = body[i..].iter().position(|c| *c == '}');
                let consumed = match (body.get(i), close) {
                    (Some('{'), Some(p)) => consumed + p + 1,
                    _ => consumed,
                };
                return Err(truncated(
                    byte_pos,
                    consumed,
                    "\\N{...} escapes are not supported",
                ));
            }
            other => {
                // unknown escape: kept verbatim (CPython warns)
                out.push('\\');
                out.push(other);
            }
        }
        byte_pos += body[i - consumed..i]
            .iter()
            .map(|c| c.len_utf8())
            .sum::<usize>();
    }
    Ok(out)
}

/// `PyBytes_DecodeEscape` for a `bytes` literal body (ASCII already checked).
fn decode_bytes_escapes(body: &[char]) -> core::result::Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < body.len() {
        let c = body[i];
        if c != '\\' {
            out.push(c as u8);
            i += 1;
            continue;
        }
        let Some(&e) = body.get(i + 1) else {
            out.push(b'\\');
            i += 1;
            continue;
        };
        let pos = i;
        i += 2;
        match e {
            '\n' => {}
            '\\' => out.push(b'\\'),
            '\'' => out.push(b'\''),
            '"' => out.push(b'"'),
            'a' => out.push(7),
            'b' => out.push(8),
            'f' => out.push(12),
            'n' => out.push(b'\n'),
            'r' => out.push(b'\r'),
            't' => out.push(b'\t'),
            'v' => out.push(11),
            '0'..='7' => {
                let mut v = e.to_digit(8).unwrap();
                let mut n = 1;
                while n < 3 && body.get(i).is_some_and(|c| ('0'..='7').contains(c)) {
                    v = v * 8 + body[i].to_digit(8).unwrap();
                    i += 1;
                    n += 1;
                }
                out.push((v & 0xff) as u8);
            }
            'x' => {
                let ok = body.get(i).is_some_and(|c| c.is_ascii_hexdigit())
                    && body.get(i + 1).is_some_and(|c| c.is_ascii_hexdigit());
                if !ok {
                    return Err(alloc::format!(
                        "(value error) invalid \\x escape at position {pos}"
                    ));
                }
                out.push(hex_value(&body[i..i + 2]).unwrap() as u8);
                i += 2;
            }
            other => {
                out.push(b'\\');
                out.push(other as u8);
            }
        }
    }
    Ok(out)
}
