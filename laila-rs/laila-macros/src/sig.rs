//! `#[laila::pysig("name(a, b=1, *args, key=None, **kwargs)")]`
//!
//! Parses the Python signature text and prepends a binding prologue to the
//! function body: every parameter becomes a local `Arg` (defaults applied),
//! `*args` a `Vec<Arg>`, `**kwargs` an `Args`. Binding errors are Python's
//! `TypeError`s. The signature string is also what the conformance checker
//! diffs against `inspect.signature` on the Python side.

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, quote_spanned};
use syn::{parse_quote, FnArg, ImplItemFn, Pat, ReturnType, Type};

#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    PosOrKw,
    VarPos,
    KwOnly,
    VarKw,
}

#[derive(Debug, Clone)]
pub struct PyParam {
    pub name: String,
    pub kind: Kind,
    pub default: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PySig {
    pub name: String,
    pub params: Vec<PyParam>,
}

/// Split on top-level commas (ignores commas inside brackets/strings).
fn split_top_level(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for c in s.chars() {
        match quote {
            Some(q) => {
                cur.push(c);
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '"' | '\'' => {
                    quote = Some(c);
                    cur.push(c);
                }
                '(' | '[' | '{' => {
                    depth += 1;
                    cur.push(c);
                }
                ')' | ']' | '}' => {
                    depth -= 1;
                    cur.push(c);
                }
                ',' if depth == 0 => {
                    out.push(cur.trim().to_string());
                    cur.clear();
                }
                _ => cur.push(c),
            },
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

pub fn parse_sig(text: &str) -> Result<PySig, String> {
    let text = text.trim();
    let open = text
        .find('(')
        .ok_or("signature must look like name(params)")?;
    if !text.ends_with(')') {
        return Err("signature must end with ')'".into());
    }
    let name = text[..open].trim().to_string();
    let inner = &text[open + 1..text.len() - 1];
    let mut params = Vec::new();
    let mut kw_only = false;
    for item in split_top_level(inner) {
        if item == "self" || item == "cls" {
            continue;
        }
        if item == "*" {
            kw_only = true;
            continue;
        }
        if item == "/" {
            continue; // positional-only marker: treated as positional-or-keyword
        }
        if let Some(rest) = item.strip_prefix("**") {
            params.push(PyParam {
                name: strip_annotation(rest),
                kind: Kind::VarKw,
                default: None,
            });
            continue;
        }
        if let Some(rest) = item.strip_prefix('*') {
            params.push(PyParam {
                name: strip_annotation(rest),
                kind: Kind::VarPos,
                default: None,
            });
            kw_only = true;
            continue;
        }
        let (lhs, default) = match split_default(&item) {
            Some((l, d)) => (l, Some(d)),
            None => (item.clone(), None),
        };
        params.push(PyParam {
            name: strip_annotation(&lhs),
            kind: if kw_only { Kind::KwOnly } else { Kind::PosOrKw },
            default,
        });
    }
    Ok(PySig { name, params })
}

/// `name: annotation` -> `name`.
fn strip_annotation(s: &str) -> String {
    // Annotations may contain ':'? (not at top level); split at first ':'.
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        match c {
            '[' | '(' => depth += 1,
            ']' | ')' => depth -= 1,
            ':' if depth == 0 => return s[..i].trim().to_string(),
            _ => {}
        }
    }
    s.trim().to_string()
}

/// `name[: ann] = default` -> (`name[: ann]`, `default`), honouring brackets/quotes.
fn split_default(s: &str) -> Option<(String, String)> {
    let mut depth = 0;
    let mut quote: Option<char> = None;
    for (i, c) in s.char_indices() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '"' | '\'' => quote = Some(c),
                '[' | '(' | '{' => depth += 1,
                ']' | ')' | '}' => depth -= 1,
                '=' if depth == 0 => {
                    return Some((s[..i].trim().to_string(), s[i + 1..].trim().to_string()))
                }
                _ => {}
            },
        }
    }
    None
}

/// Lower a Python default-value text to a Rust `Arg` expression.
pub fn default_expr(text: &str) -> TokenStream {
    match text {
        "None" => quote!(::laila::python::Arg::None),
        "True" => quote!(::laila::python::Arg::Bool(true)),
        "False" => quote!(::laila::python::Arg::Bool(false)),
        "[]" => quote!(::laila::python::IntoArg::into_arg(
            ::laila::python::__rt::Vec::<::laila::python::Arg>::new()
        )),
        "{}" => quote!(::laila::python::builtins::dict(::laila::python::Args::new()).unwrap()),
        "()" => quote!(::laila::python::Arg::Tuple(
            ::laila::python::__rt::Arc::from(
                ::laila::python::__rt::Vec::<::laila::python::Arg>::new()
            )
        )),
        t if t.starts_with('"') || t.starts_with('\'') => {
            let inner = &t[1..t.len() - 1];
            quote!(::laila::python::Arg::Str(::laila::python::__rt::Arc::from(#inner)))
        }
        t => {
            if let Ok(i) = t.parse::<i64>() {
                return quote!(::laila::python::Arg::Int(#i));
            }
            if let Ok(f) = t.parse::<f64>() {
                return quote!(::laila::python::Arg::Float(#f));
            }
            // A `CONSTANT`-case name: a module constant (`static NAME:
            // LazyLock<Arg>` / a `const`), read as a value.
            if !t.is_empty()
                && t.chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
                && t.chars().any(|c| c.is_ascii_uppercase())
            {
                let id = format_ident!("{}", t, span = Span::call_site());
                return quote!(::laila::python::builtins::module_value(&#id));
            }
            // Anything else is a Rust expression in scope (a constant, a call).
            match syn::parse_str::<syn::Expr>(t) {
                Ok(e) => quote!(::laila::python::IntoArg::into_arg(#e)),
                Err(_) => {
                    let msg = format!("pysig: cannot lower default `{t}`");
                    quote!(compile_error!(#msg))
                }
            }
        }
    }
}

/// Is a default-value text a literal (`None`, `True`, `3`, `"x"`, `[]`, ...)
/// -- one the signature can carry as a value (`Param::default`)? Arbitrary
/// Rust expressions (constants, `Self::..`) stay body-only.
fn default_is_literal(text: &str) -> bool {
    matches!(text, "None" | "True" | "False" | "[]" | "{}" | "()")
        || text.starts_with('"')
        || text.starts_with('\'')
        || text.parse::<i64>().is_ok()
        || text.parse::<f64>().is_ok()
}

/// `static __LAILA_SIG: Signature = ..;` for *sig*.
pub fn signature_static(sig: &PySig) -> TokenStream {
    let fname = &sig.name;
    let param_specs = sig.params.iter().map(|p| {
        let name = &p.name;
        let kind = match p.kind {
            Kind::PosOrKw => quote!(::laila::python::ParamKind::PositionalOrKeyword),
            Kind::VarPos => quote!(::laila::python::ParamKind::VarPositional),
            Kind::KwOnly => quote!(::laila::python::ParamKind::KeywordOnly),
            Kind::VarKw => quote!(::laila::python::ParamKind::VarKeyword),
        };
        let has_default = p.default.is_some();
        let default = match &p.default {
            Some(d) if default_is_literal(d) => {
                let e = default_expr(d);
                quote!(::core::option::Option::Some(|| #e))
            }
            _ => quote!(::core::option::Option::None),
        };
        quote!(::laila::python::Param { name: #name, kind: #kind, has_default: #has_default, default: #default })
    });
    quote! {
        static __LAILA_SIG: ::laila::python::Signature = ::laila::python::Signature {
            name: #fname,
            params: &[ #(#param_specs),* ],
        };
    }
}

/// `let mut <param>: Arg = __laila_bound.take(i, || default);` per parameter.
pub fn bindings(sig: &PySig) -> Vec<TokenStream> {
    sig.params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let ident = format_ident!("{}", rustify(&p.name), span = Span::call_site());
            match p.kind {
                Kind::VarPos => quote!(let mut #ident: ::laila::python::__rt::Vec<::laila::python::Arg> = ::core::mem::take(&mut __laila_bound.varargs);),
                Kind::VarKw => quote!(let mut #ident: ::laila::python::Args = ::core::mem::take(&mut __laila_bound.kwargs);),
                _ => {
                    let default = match &p.default {
                        Some(d) => default_expr(d),
                        None => quote!(::laila::python::Arg::None),
                    };
                    // `mut`: a Python body may rebind its parameter (`v = v or ..`).
                    quote!(let mut #ident: ::laila::python::Arg = __laila_bound.take(#i, || #default);)
                }
            }
        })
        .collect()
}

pub fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    let lit: syn::LitStr = match syn::parse2(attr) {
        Ok(l) => l,
        Err(e) => return e.to_compile_error(),
    };
    let sig = match parse_sig(&lit.value()) {
        Ok(s) => s,
        Err(msg) => return quote_spanned!(lit.span() => compile_error!(#msg);),
    };
    let mut func: ImplItemFn = match syn::parse2(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    // The Rust parameter that carries the Python arguments: the last
    // non-self parameter (conventionally `args: impl Into<Args>`).
    let args_ident = func
        .sig
        .inputs
        .iter()
        .filter_map(|a| match a {
            FnArg::Typed(t) => match &*t.pat {
                Pat::Ident(p) => Some(p.ident.clone()),
                _ => None,
            },
            _ => None,
        })
        .next_back();
    let args_ident = match args_ident {
        Some(i) => i,
        None => {
            return quote_spanned!(func.sig.ident.span() =>
                compile_error!("pysig: function needs an `args: impl Into<Args>` parameter"););
        }
    };

    // Return type must be a Result for `?` in the prologue.
    if let ReturnType::Default = func.sig.output {
        let ty: Type = parse_quote!(::laila::python::Result<()>);
        func.sig.output = ReturnType::Type(Default::default(), Box::new(ty));
    }

    let sig_static = signature_static(&sig);
    let bindings = bindings(&sig);
    let prologue: TokenStream = quote! {
        #sig_static
        let __laila_args: ::laila::python::Args = ::core::convert::Into::into(#args_ident);
        #[allow(unused_mut)]
        let mut __laila_bound = __LAILA_SIG.bind(__laila_args)?;
        #(#[allow(unused_variables, unused_mut)] #bindings)*
    };

    let body_stmts = std::mem::take(&mut func.block.stmts);
    let prologue_block: syn::Block = parse_quote!({ #prologue });
    let mut stmts = prologue_block.stmts;
    stmts.extend(body_stmts);
    func.block.stmts = stmts;
    quote!(#func)
}

/// Python names that are Rust keywords become raw identifiers.
pub fn rustify(name: &str) -> String {
    const KW: &[&str] = &[
        "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn",
        "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
        "return", "static", "struct", "super", "trait", "true", "type", "unsafe", "use", "where",
        "while", "async", "await", "dyn", "abstract", "become", "box", "do", "final", "macro",
        "override", "priv", "typeof", "unsized", "virtual", "yield", "try", "gen",
    ];
    if KW.contains(&name) {
        format!("r#{name}")
    } else {
        name.to_string()
    }
}
