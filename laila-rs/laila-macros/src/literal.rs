//! Python literal syntax -> `Arg` expression (`py!`), and the call-argument
//! syntax -> `Args` expression (`args!`).
//!
//! Both work on token trees, so nested dict/list/tuple literals and
//! keyword arguments (`k = v`), `*x`, `**kw` splats are recognised without
//! the tokens having to be valid Rust expressions.

use proc_macro2::{Delimiter, Group, Ident, Punct, Spacing, Span, TokenStream, TokenTree};
use quote::{quote, quote_spanned};

/// Split a token stream on top-level commas.
pub fn split_commas(ts: TokenStream) -> Vec<Vec<TokenTree>> {
    let mut out: Vec<Vec<TokenTree>> = vec![Vec::new()];
    // Inside `lambda a, b: ..` the commas separate *parameters*, not
    // elements: they are kept until the `:` that ends the parameter list.
    let mut in_lambda_params = false;
    for tt in ts {
        match &tt {
            TokenTree::Ident(i) if i == "lambda" => {
                in_lambda_params = true;
                out.last_mut().unwrap().push(tt);
            }
            TokenTree::Punct(p) if p.as_char() == ':' && in_lambda_params => {
                in_lambda_params = false;
                out.last_mut().unwrap().push(tt);
            }
            TokenTree::Punct(p) if p.as_char() == ',' && !in_lambda_params => out.push(Vec::new()),
            _ => out.last_mut().unwrap().push(tt),
        }
    }
    if out.last().map(|v| v.is_empty()).unwrap_or(false) {
        out.pop();
    }
    out
}

/// Split a token list on the first top-level `:` (dict `key: value`).
fn split_colon(tts: &[TokenTree]) -> Option<(Vec<TokenTree>, Vec<TokenTree>)> {
    for (i, tt) in tts.iter().enumerate() {
        if let TokenTree::Punct(p) = tt {
            if p.as_char() == ':' {
                // `::` path separator is two joint colons; skip those.
                let joint_prev = i > 0
                    && matches!(&tts[i - 1], TokenTree::Punct(q) if q.as_char() == ':' && q.spacing() == Spacing::Joint);
                if p.spacing() == Spacing::Joint || joint_prev {
                    continue;
                }
                return Some((tts[..i].to_vec(), tts[i + 1..].to_vec()));
            }
        }
    }
    None
}

fn is_ident(tt: &TokenTree, s: &str) -> bool {
    matches!(tt, TokenTree::Ident(i) if i == s)
}

/// Does this brace group look like a Python dict/set literal rather than a
/// Rust block? (empty, or first element starts with a literal/ident key
/// followed by `:` at top level, or comma-separated without `;`)
pub fn is_py_brace_literal(g: &Group) -> bool {
    if g.delimiter() != Delimiter::Brace {
        return false;
    }
    let tts: Vec<TokenTree> = g.stream().into_iter().collect();
    if tts.is_empty() {
        return true;
    }
    if tts
        .iter()
        .any(|t| matches!(t, TokenTree::Punct(p) if p.as_char() == ';'))
    {
        return false;
    }
    // Rust struct literal `Name { field: v }` is handled by the caller (the
    // preceding token is an ident). Here: a leading literal or `**` means dict.
    match &tts[0] {
        TokenTree::Literal(_) => true,
        TokenTree::Punct(p) if p.as_char() == '*' => true,
        TokenTree::Ident(i) if i == "None" || i == "True" || i == "False" => true,
        TokenTree::Group(_) => true, // ("a", 1): v  tuple key
        TokenTree::Ident(_) => {
            // `{x: 1}` with an identifier key is a dict in Python but a type
            // ascription in Rust; treat as dict only if a top-level `,` or
            // `:` makes it unambiguous and no `=>`/`let` appears.
            let has_colon = split_colon(&tts).is_some();
            let has_comma = tts
                .iter()
                .any(|t| matches!(t, TokenTree::Punct(p) if p.as_char() == ','));
            (has_colon || has_comma)
                && !tts
                    .iter()
                    .any(|t| is_ident(t, "let") || is_ident(t, "return") || is_ident(t, "if"))
        }
        _ => false,
    }
}

/// Lower one element of a literal / argument list to an `Arg` expression.
/// `rewrite` is applied to non-literal element tokens (the `#[python]`
/// rewriter passes itself; `py!` passes the identity).
pub fn lower_value(tts: &[TokenTree], rewrite: &dyn Fn(TokenStream) -> TokenStream) -> TokenStream {
    if tts.len() == 1 {
        match &tts[0] {
            TokenTree::Ident(i) if i == "None" => {
                return quote_spanned!(i.span() => ::laila::python::Arg::None)
            }
            TokenTree::Ident(i) if i == "True" => {
                return quote_spanned!(i.span() => ::laila::python::Arg::Bool(true))
            }
            TokenTree::Ident(i) if i == "False" => {
                return quote_spanned!(i.span() => ::laila::python::Arg::Bool(false))
            }
            TokenTree::Group(g) => match g.delimiter() {
                Delimiter::Brace if is_py_brace_literal(g) => return lower_brace(g, rewrite),
                Delimiter::Bracket => return lower_list(g, rewrite),
                Delimiter::Parenthesis => {
                    let elems = split_commas(g.stream());
                    let trailing_comma = g
                        .stream()
                        .into_iter()
                        .last()
                        .map(|t| matches!(t, TokenTree::Punct(p) if p.as_char() == ','))
                        .unwrap_or(false);
                    if elems.is_empty() || elems.len() > 1 || trailing_comma {
                        return lower_tuple(g, rewrite);
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }
    // A bare name shares its object (Python list / tuple elements are
    // references): `[a, b]` leaves `a` usable afterwards.
    if let [TokenTree::Ident(i)] = tts {
        if i != "self" {
            let ts: TokenStream = tts.iter().cloned().collect();
            let ts = rewrite(ts);
            return quote!(::laila::python::IntoArg::into_arg(&(#ts)));
        }
    }
    let ts: TokenStream = tts.iter().cloned().collect();
    let ts = rewrite(ts);
    quote!(::laila::python::IntoArg::into_arg(#ts))
}

/// The `Vec<Arg>` of a sequence literal; `*rest` elements are spread
/// (`[first, *queued]`).
fn lower_seq_vec(g: &Group, rewrite: &dyn Fn(TokenStream) -> TokenStream) -> TokenStream {
    let elems = split_commas(g.stream());
    if !elems.iter().any(|e| starts_with_star(e)) {
        let elems: Vec<TokenStream> = elems.iter().map(|e| lower_value(e, rewrite)).collect();
        return quote_spanned!(g.span() => ::laila::python::__rt::vec![#(#elems),*] as ::laila::python::__rt::Vec<::laila::python::Arg>);
    }
    let stmts: Vec<TokenStream> = elems
        .iter()
        .map(|e| {
            if starts_with_star(e) {
                let inner: TokenStream = e[1..].iter().cloned().collect();
                let inner = rewrite(inner);
                quote!(for __x in ::laila::python::IntoArg::into_arg(#inner).__iter__().expect("sequence literal: `*` of a non-iterable") { __v.push(__x); })
            } else {
                let v = lower_value(e, rewrite);
                quote!(__v.push(#v);)
            }
        })
        .collect();
    quote_spanned!(g.span() => {
        let mut __v: ::laila::python::__rt::Vec<::laila::python::Arg> = ::laila::python::__rt::Vec::new();
        #(#stmts)*
        __v
    })
}

pub fn lower_list(g: &Group, rewrite: &dyn Fn(TokenStream) -> TokenStream) -> TokenStream {
    let v = lower_seq_vec(g, rewrite);
    quote_spanned!(g.span() => ::laila::python::IntoArg::into_arg(#v))
}

pub fn lower_tuple(g: &Group, rewrite: &dyn Fn(TokenStream) -> TokenStream) -> TokenStream {
    let v = lower_seq_vec(g, rewrite);
    quote_spanned!(g.span() => ::laila::python::Arg::Tuple(::laila::python::__rt::Arc::from(#v)))
}

/// `{k: v, **other}` dict or `{a, b}` set.
pub fn lower_brace(g: &Group, rewrite: &dyn Fn(TokenStream) -> TokenStream) -> TokenStream {
    let elems = split_commas(g.stream());
    let is_set = !elems.is_empty()
        && elems
            .iter()
            .all(|e| split_colon(e).is_none() && !starts_with_double_star(e));
    if is_set {
        let items: Vec<TokenStream> = elems.iter().map(|e| lower_value(e, rewrite)).collect();
        // `{a, b}` -> `Arg::Set` (an unhashable element is Python's `TypeError`
        // at the literal; it panics here like the dict literal).
        return quote_spanned!(g.span() => ::laila::python::builtins::set(::laila::python::Args::from(
            ::laila::python::IntoArg::into_arg(::laila::python::__rt::vec![#(#items),*] as ::laila::python::__rt::Vec<::laila::python::Arg>)
        )).expect("set literal"));
    }
    let mut stmts = Vec::new();
    for e in &elems {
        if starts_with_double_star(e) {
            let inner: TokenStream = e[2..].iter().cloned().collect();
            let inner = rewrite(inner);
            stmts.push(quote!(__laila_dict_extend(&__d, #inner)?;));
            continue;
        }
        let (k, v) = match split_colon(e) {
            Some(kv) => kv,
            None => {
                return quote_spanned!(g.span() => compile_error!("py!: dict entries must be `key: value`"))
            }
        };
        let k = lower_value(&k, rewrite);
        let v = lower_value(&v, rewrite);
        stmts.push(quote!(__d.__setitem__(#k, #v)?;));
    }
    // Wrapped in a closure so the literal is usable outside `Result` contexts;
    // the only failure mode is an unhashable key (Python: TypeError at the
    // literal), which panics here.
    quote_spanned!(g.span() => (|| -> ::laila::python::Result<::laila::python::Arg> {
        #[allow(unused_variables, dead_code)]
        fn __laila_dict_extend(__laila_d: &::laila::python::Arg, __laila_src: ::laila::python::Arg) -> ::laila::python::Result<()> {
            for __laila_k in __laila_src.__iter__()? { __laila_d.__setitem__(__laila_k.clone(), __laila_src.__getitem__(__laila_k)?)?; }
            Ok(())
        }
        let __d = ::laila::python::builtins::dict(::laila::python::Args::new())?;
        #(#stmts)*
        Ok(__d)
    })().expect("dict literal"))
}

fn starts_with_star(e: &[TokenTree]) -> bool {
    e.len() >= 2
        && matches!(&e[0], TokenTree::Punct(p) if p.as_char() == '*')
        && !matches!(&e[1], TokenTree::Punct(p) if p.as_char() == '*')
}

fn starts_with_double_star(e: &[TokenTree]) -> bool {
    e.len() >= 2
        && matches!(&e[0], TokenTree::Punct(p) if p.as_char() == '*')
        && matches!(&e[1], TokenTree::Punct(p) if p.as_char() == '*')
}

/// `py!(<python literal>)`.
pub fn expand_py(input: TokenStream) -> TokenStream {
    let tts: Vec<TokenTree> = input.into_iter().collect();
    lower_value(&tts, &|t| t)
}

/// `args!(a, b, k = v, *rest, **kw)` -> `Args`.
pub fn expand_args(
    input: TokenStream,
    rewrite: &dyn Fn(TokenStream) -> TokenStream,
) -> TokenStream {
    let mut stmts = Vec::new();
    for e in split_commas(input) {
        if e.is_empty() {
            continue;
        }
        if starts_with_double_star(&e) {
            let inner: TokenStream = e[2..].iter().cloned().collect();
            let inner = rewrite(inner);
            stmts.push(quote!(__a.extend_keywords(#inner)?;));
            continue;
        }
        if matches!(&e[0], TokenTree::Punct(p) if p.as_char() == '*') {
            let inner: TokenStream = e[1..].iter().cloned().collect();
            let inner = rewrite(inner);
            stmts.push(quote!(__a.extend_positional(#inner)?;));
            continue;
        }
        // `name = value` (single `=`, not `==`).
        if e.len() >= 3 {
            if let (TokenTree::Ident(name), TokenTree::Punct(p)) = (&e[0], &e[1]) {
                if p.as_char() == '='
                    && p.spacing() == Spacing::Alone
                    && !matches!(e.get(2), Some(TokenTree::Punct(q)) if q.as_char() == '=')
                {
                    let key = name.to_string();
                    let key = key.strip_prefix("r#").unwrap_or(&key).to_string();
                    let v = lower_value(&e[2..], rewrite);
                    stmts.push(quote!(__a.push_keyword(#key, #v);));
                    continue;
                }
            }
        }
        let v = lower_value(&e, rewrite);
        stmts.push(quote!(__a.push_positional(#v);));
    }
    quote!({
        let mut __a = ::laila::python::Args::new();
        #(#stmts)*
        __a
    })
}

#[allow(dead_code)]
pub fn ident(s: &str) -> Ident {
    Ident::new(s, Span::call_site())
}
#[allow(dead_code)]
pub fn punct(c: char) -> Punct {
    Punct::new(c, Spacing::Alone)
}
