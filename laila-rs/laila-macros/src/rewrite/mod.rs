//! `#[laila::python]` / `#[laila::python(unittest)]` / `#[laila::python(dynamic)]`
//! on a `fn`, an `impl` block or a `mod`.

pub mod ast;
pub mod gen;
pub mod model;
pub mod parse;

use std::collections::{HashMap, HashSet};

use proc_macro2::{Delimiter, Group, Ident, Span, TokenStream, TokenTree};
use quote::{quote, quote_spanned};

use gen::RawParam;
use gen::{Ctx, Gen, RetKind, TESTCASE_METHODS};

#[derive(Default, Debug, Clone)]
pub struct Opts {
    pub unittest: bool,
    pub dynamic: bool,
    /// Python functions imported from elsewhere, callable by bare name.
    pub functions: HashSet<String>,
    /// Python *modules* imported as values (`from laila.macros import
    /// strings`): attribute reads on them go through `__getattr__`.
    pub modules: HashSet<String>,
    /// Classes (imported Rust types) whose instances are attribute-dynamic
    /// (`#![dyn_classes(A, B)]`): `x = A(); x.attr` reads through
    /// `__getattr__`, as for the block's own `py_model!` classes.
    pub dyn_classes: HashSet<String>,
}

/// Leading `#![unittest]` / `#![dynamic]` / `#![functions(a, b)]` /
/// `#![modules(a, b)]` inner attributes.
fn parse_inner_opts(tts: &mut Vec<TokenTree>) -> Result<Opts, syn::Error> {
    let mut o = Opts::default();
    loop {
        let is_inner = matches!(tts.first(), Some(TokenTree::Punct(p)) if p.as_char() == '#')
            && matches!(tts.get(1), Some(TokenTree::Punct(p)) if p.as_char() == '!')
            && matches!(tts.get(2), Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Bracket);
        if !is_inner {
            break;
        }
        let g = match &tts[2] {
            TokenTree::Group(g) => g.clone(),
            _ => unreachable!(),
        };
        tts.drain(..3);
        let inner: Vec<TokenTree> = g.stream().into_iter().collect();
        match inner.first() {
            Some(TokenTree::Ident(i)) if i == "unittest" => {
                o.unittest = true;
                o.dynamic = true;
            }
            Some(TokenTree::Ident(i)) if i == "dynamic" => o.dynamic = true,
            Some(TokenTree::Ident(i)) if i == "functions" => {
                if let Some(TokenTree::Group(list)) = inner.get(1) {
                    for t in list.stream() {
                        if let TokenTree::Ident(f) = t {
                            o.functions.insert(f.to_string());
                        }
                    }
                }
            }
            Some(TokenTree::Ident(i)) if i == "modules" => {
                if let Some(TokenTree::Group(list)) = inner.get(1) {
                    for t in list.stream() {
                        if let TokenTree::Ident(f) = t {
                            o.modules.insert(f.to_string());
                        }
                    }
                }
            }
            Some(TokenTree::Ident(i)) if i == "dyn_classes" => {
                if let Some(TokenTree::Group(list)) = inner.get(1) {
                    for t in list.stream() {
                        if let TokenTree::Ident(f) = t {
                            o.dyn_classes.insert(f.to_string());
                        }
                    }
                }
            }
            other => {
                return Err(syn::Error::new(
                    other.map(|t| t.span()).unwrap_or_else(Span::call_site),
                    "laila::python!: unknown option (expected `#![unittest]`, `#![dynamic]`, `#![functions(..)]` or `#![modules(..)]`)",
                ))
            }
        }
    }
    Ok(o)
}

/// `laila::python! { #![opts] items... }`
pub fn expand(input: TokenStream) -> TokenStream {
    let mut tts: Vec<TokenTree> = input.into_iter().collect();
    let opts = match parse_inner_opts(&mut tts) {
        Ok(o) => o,
        Err(e) => return e.to_compile_error(),
    };
    match expand_items(tts, &opts) {
        Ok(ts) => ts,
        Err(e) => e.to_compile_error(),
    }
}

fn item_kind(item: &[TokenTree]) -> Option<String> {
    // first keyword after attributes / visibility / qualifiers
    let mut skip_group = false;
    for t in item {
        match t {
            TokenTree::Punct(p) if p.as_char() == '#' => skip_group = true,
            TokenTree::Group(_) if skip_group => skip_group = false,
            TokenTree::Ident(i) => {
                let s = i.to_string();
                match s.as_str() {
                    "pub" | "async" | "unsafe" | "const" | "extern" | "crate" | "default" => {}
                    "fn" | "impl" | "mod" | "struct" | "enum" | "use" | "static" | "type"
                    | "trait" | "macro_rules" => return Some(s),
                    _ => return Some(s),
                }
            }
            TokenTree::Group(_) => {} // `pub(crate)`
            _ => {}
        }
    }
    None
}

/// Mixin traits whose implementors get the secret gate installed on their
/// public methods (Python does it from `__getattribute__`; see
/// `laila::atomic::definitions::globally_atomic_identifiable_object`).
const GATED_MIXINS: &[&str] = &["_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT"];

/// Mixin traits that inherit `_LAILA_IDENTIFIABLE_OBJECT`: a `py_class!`
/// deriving from one (`impl Mixin for C {}`) gets its identity from the
/// instance `__dict__` (`laila::dict_identity!(C)`), the Rust spelling of
/// the pydantic private attributes the Python base class would hold.
const IDENTIFIABLE_MIXINS: &[&str] = &[
    "_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT",
    "_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT",
];

/// Names the gate never wraps (the mixins' and pydantic's own API); the
/// Rust twin of `_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT._ungated()`.
const UNGATED_NAMES: &[&str] = &[
    "atomic",
    "lock",
    "unlock",
    "locked",
    "creation_timestamp",
    "lock_global",
    "unlock_global",
    "global_locked",
    "global_id",
    "uuid",
    "scopes",
    "evolution",
    "process_global_id",
    "generate_uuid_from_nickname",
    "resolve_global_id",
    "as_dict",
    "from_dict",
    "model_dump",
    "model_dump_json",
    "model_validate",
    "model_validate_json",
    "model_construct",
    "model_copy",
    "model_fields",
    "model_post_init",
    "model_json_schema",
    "dict",
    "json",
    "copy",
    "new",
];

/// `impl <Trait> for <Type> {}` -> `(Trait last segment, Type)`.
fn trait_impl_parts(item: &[TokenTree]) -> Option<(String, String)> {
    let mut seen_impl = false;
    let mut after: Vec<String> = Vec::new();
    for t in item {
        match t {
            TokenTree::Ident(i) if i == "impl" => seen_impl = true,
            TokenTree::Group(g) if g.delimiter() == Delimiter::Brace => break,
            TokenTree::Ident(i) if seen_impl => after.push(i.to_string()),
            _ => {}
        }
    }
    let pos = after.iter().position(|s| s == "for")?;
    let tr = after[..pos].last()?.clone();
    let ty = after.get(pos + 1)?.clone();
    Some((tr, ty))
}

/// How a `py_class!` method is reached from a dynamic call
/// (`obj.__call_method__(name, args)`).
#[derive(Clone, Copy, Debug)]
enum DispatchKind {
    /// `fn m(&self, args: Args)` / `#[pysig] fn m(&self, args: Args)`: the
    /// argument bag is passed through.
    Bag,
    /// `fn m(&self, a: Arg, b: Arg)`: the positional arguments, in order
    /// (missing ones are `None`).
    Positional(usize),
    /// `#[property] fn p(&self)`: read as an attribute.
    Property,
}

/// The dispatch kind of a method item, or `None` when it cannot be reached
/// dynamically (a static method, a typed parameter).
fn dispatch_kind(item: &[TokenTree]) -> Option<DispatchKind> {
    let mut it = item.iter();
    let mut params: Option<TokenStream> = None;
    while let Some(t) = it.next() {
        if matches!(t, TokenTree::Ident(i) if i == "fn") {
            it.next();
            if let Some(TokenTree::Group(g)) = it.next() {
                if g.delimiter() == Delimiter::Parenthesis {
                    params = Some(g.stream());
                }
            }
            break;
        }
    }
    let pieces = crate::literal::split_commas(params?);
    let mut has_self = false;
    let mut typed: Vec<Vec<String>> = Vec::new();
    for piece in &pieces {
        match piece
            .iter()
            .position(|t| matches!(t, TokenTree::Punct(p) if p.as_char() == ':'))
        {
            None => {
                if piece
                    .iter()
                    .any(|t| matches!(t, TokenTree::Ident(i) if i == "self"))
                {
                    has_self = true;
                }
            }
            Some(colon) => typed.push(piece[colon + 1..].iter().map(|t| t.to_string()).collect()),
        }
    }
    if !has_self {
        return None;
    }
    // `#[property]` and `#[class_attr]` getters are read as attribute
    // *values* (`proto.rpc_timeout`), never called.
    if typed.is_empty() && (has_attr(item, "property") || has_attr(item, "class_attr")) {
        return Some(DispatchKind::Property);
    }
    if typed.len() == 1 && typed[0] == ["Args"] {
        return Some(DispatchKind::Bag);
    }
    if typed.iter().all(|ty| ty == &["Arg"]) {
        return Some(DispatchKind::Positional(typed.len()));
    }
    None
}

/// `py_class!(Name ..);` plus the dispatch of its `impl` methods: the
/// invocation gains the `dispatch` flag (its `__call_method__` consults
/// [`laila::python::PyClassDispatch`]) and the trait impl is emitted.
fn py_class_with_dispatch(
    item: &[TokenTree],
    name: &str,
    methods: &[(Ident, DispatchKind)],
    identifiable: bool,
) -> TokenStream {
    let ty = Ident::new(name, Span::call_site());
    let inner: TokenStream = item
        .iter()
        .find_map(|t| match t {
            TokenTree::Group(g) => Some(g.stream()),
            _ => None,
        })
        .unwrap_or_default();
    let mut props: Vec<TokenStream> = methods
        .iter()
        .filter(|(_, k)| matches!(k, DispatchKind::Property))
        .map(|(m, _)| {
            let s = m.to_string();
            quote!(#s => Some(self.#m().map(::laila::python::IntoArg::into_arg)),)
        })
        .collect();
    // A subclass of an identifiable mixin: `obj.global_id` / `obj.uuid`
    // are the identity properties ([`laila::dict_identity!`]).
    if identifiable {
        props.push(quote! {
            "global_id" => Some(Ok(::laila::python::Arg::Str(
                ::laila::atomic::definitions::HasIdentity::global_id(self).into(),
            ))),
            "uuid" => Some(Ok(::laila::python::Arg::Str(
                ::laila::atomic::definitions::HasIdentity::uuid(self).into(),
            ))),
        });
    }
    let names: Vec<String> = methods
        .iter()
        .filter(|(_, k)| !matches!(k, DispatchKind::Property))
        .map(|(m, _)| m.to_string())
        .collect();
    let arms: Vec<TokenStream> = methods
        .iter()
        .filter(|(_, k)| !matches!(k, DispatchKind::Property))
        .map(|(m, k)| {
            let s = m.to_string();
            match k {
                DispatchKind::Property => unreachable!(),
                DispatchKind::Bag => quote! {
                    #s => Some(self.#m(args).map(::laila::python::IntoArg::into_arg)),
                },
                DispatchKind::Positional(n) => {
                    let params: Vec<Ident> = (0..*n)
                        .map(|i| Ident::new(&format!("__laila_p{i}"), Span::call_site()))
                        .collect();
                    quote! {
                        #s => {
                            let mut __laila_it = args.positional.into_iter();
                            #(let #params = __laila_next(&mut __laila_it);)*
                            Some(self.#m(#(#params),*).map(::laila::python::IntoArg::into_arg))
                        }
                    }
                }
            }
        })
        .collect();
    quote! {
        ::laila::py_class!(#inner; dispatch);
        impl ::laila::python::PyClassDispatch for #ty {
            #[allow(unused_variables, unused_mut, clippy::all)]
            fn __laila_dispatch__(
                &self,
                name: &str,
                args: ::laila::python::Args,
            ) -> Option<::laila::python::Result<::laila::python::Arg>> {
                #[allow(dead_code)]
                fn __laila_next(
                    it: &mut ::laila::python::__rt::vec::IntoIter<::laila::python::Arg>,
                ) -> ::laila::python::Arg {
                    it.next().unwrap_or(::laila::python::Arg::None)
                }
                match name {
                    #(#arms)*
                    _ => None,
                }
            }
            #[allow(unused_variables, clippy::all)]
            fn __laila_property__(&self, name: &str) -> Option<::laila::python::Result<::laila::python::Arg>> {
                match name {
                    #(#props)*
                    _ => None,
                }
            }
            fn __laila_method_names__() -> &'static [&'static str] {
                &[#(#names),*]
            }
        }
    }
}

/// `py_class!(Name; ..);` -> `Name`.
/// The `py_class!(Name; ..)` item *item* of a function body with the
/// `PyClassDispatch` impl derived from the sibling `impl Name { .. }` in
/// *block* (a class declared inside a test method: `py_class!(_Stub);`).
pub(crate) fn nested_py_class(item: &[TokenTree], block: &ast::Block) -> Option<TokenStream> {
    let name = py_class_name(item)?;
    let mut methods: Vec<(Ident, DispatchKind)> = Vec::new();
    for st in &block.stmts {
        let ast::Stmt::Item(ts) = st else { continue };
        let tts: Vec<TokenTree> = ts.clone().into_iter().collect();
        if item_kind(&tts).as_deref() != Some("impl") || trait_impl_parts(&tts).is_some() {
            continue;
        }
        if inherent_impl_type(&tts).as_deref() != Some(name.as_str()) {
            continue;
        }
        if let Some(TokenTree::Group(body)) = tts.last() {
            for m in split_items(body.stream()) {
                if is_fn_item(&m) {
                    if let (Some(n), Some(k)) = (fn_name(&m), dispatch_kind(&m)) {
                        methods.push((n, k));
                    }
                }
            }
        }
    }
    let identifiable = block.stmts.iter().any(|st| {
        let ast::Stmt::Item(ts) = st else { return false };
        let tts: Vec<TokenTree> = ts.clone().into_iter().collect();
        matches!(trait_impl_parts(&tts), Some((tr, ty)) if ty == name && IDENTIFIABLE_MIXINS.contains(&tr.as_str()))
    });
    Some(py_class_with_dispatch(item, &name, &methods, identifiable))
}

pub(crate) fn py_class_name(item: &[TokenTree]) -> Option<String> {
    if !(matches!(item.first(), Some(TokenTree::Ident(i)) if i == "py_class")
        && matches!(item.get(1), Some(TokenTree::Punct(p)) if p.as_char() == '!'))
    {
        return None;
    }
    let body = item.iter().find_map(|t| match t {
        TokenTree::Group(g) => Some(g.clone()),
        _ => None,
    })?;
    body.stream().into_iter().find_map(|t| match t {
        TokenTree::Ident(i) => Some(i.to_string()),
        _ => None,
    })
}

/// `impl <Type> {}` (no trait) -> `Type`.
fn inherent_impl_type(item: &[TokenTree]) -> Option<String> {
    let mut seen_impl = false;
    for t in item {
        match t {
            TokenTree::Ident(i) if i == "impl" => seen_impl = true,
            TokenTree::Ident(i) if seen_impl => return Some(i.to_string()),
            TokenTree::Group(g) if g.delimiter() == Delimiter::Brace => break,
            _ => {}
        }
    }
    None
}

/// A sequence of items: fns and impls are rewritten, everything else passes through.
fn expand_items(tts: Vec<TokenTree>, opts: &Opts) -> Result<TokenStream, syn::Error> {
    let items = split_items(tts.into_iter().collect());
    let mut local_fns: HashSet<String> = opts.functions.clone();
    let mut raw_params: HashMap<String, Vec<RawParam>> = HashMap::new();
    // Functions / methods of the block declared `-> Arg`: a local bound
    // to their result is an `Arg` local (`ch = _chan()`).
    let mut arg_fns: HashSet<String> = HashSet::new();
    let mut pysig_fns: HashSet<String> = HashSet::new();
    let mut gated_types: HashSet<String> = HashSet::new();
    let mut identifiable_types: HashSet<String> = HashSet::new();
    let mut dyn_classes: HashSet<String> = opts.dyn_classes.clone();
    let mut local_classes: HashSet<String> = HashSet::new();
    let mut local_methods: HashSet<String> = HashSet::new();
    let mut module_consts: HashSet<String> = HashSet::new();
    // `py_class!` classes of the block and, per class, the methods its
    // `impl` defines that an instance passed as a value can be called on
    // dynamically (`obj.m(..)` from Rust code: `_FakeComm._send_rpc`).
    let mut py_classes: HashSet<String> = HashSet::new();
    let mut class_dispatch: HashMap<String, Vec<(Ident, DispatchKind)>> = HashMap::new();
    // `test_*` methods defined by more than one class of the block [P12c].
    let mut seen_tests: HashSet<String> = HashSet::new();
    let mut shared_tests: HashSet<String> = HashSet::new();
    for it in &items {
        if item_kind(it).as_deref() == Some("static") {
            if let Some(n) = static_name(it) {
                module_consts.insert(n.to_string());
            }
        }
        if item_kind(it).as_deref() == Some("fn") {
            if let Some(n) = fn_name(it) {
                local_fns.insert(n.to_string());
                if has_pysig(it) {
                    pysig_fns.insert(n.to_string());
                }
                let raw = fn_raw_params(it);
                if raw.iter().any(|r| *r != RawParam::Dyn) {
                    raw_params.insert(n.to_string(), raw);
                }
                if fn_returns_arg(it) {
                    arg_fns.insert(n.to_string());
                }
            }
        }
        if model::is_py_model(it) {
            if let Some(n) = model::py_model_name(it) {
                dyn_classes.insert(n);
            }
        }
        // `py_class!(Name; ..)`: instances carry `__init__`-assigned
        // attributes, read dynamically (`fake.calls`).
        if let Some(n) = py_class_name(it) {
            dyn_classes.insert(n.clone());
            py_classes.insert(n);
        }
        if item_kind(it).as_deref() == Some("impl") {
            if let Some((tr, ty)) = trait_impl_parts(it) {
                if GATED_MIXINS.contains(&tr.as_str()) {
                    gated_types.insert(ty.clone());
                }
                if IDENTIFIABLE_MIXINS.contains(&tr.as_str()) {
                    identifiable_types.insert(ty);
                }
            } else if let Some(ty) = inherent_impl_type(it) {
                local_classes.insert(ty.clone());
                if let Some(TokenTree::Group(body)) = it.last() {
                    for m in split_items(body.stream()) {
                        if is_fn_item(&m) {
                            if let Some(n) = fn_name(&m) {
                                local_methods.insert(n.to_string());
                                if let Some(k) = dispatch_kind(&m) {
                                    class_dispatch.entry(ty.clone()).or_default().push((n, k));
                                }
                            }
                        }
                    }
                }
            }
            // `#[pysig]` methods of every class in the block: a call on an
            // instance from another class (`c.bump()`) passes `Args` too.
            if let Some(TokenTree::Group(body)) = it.last() {
                for m in split_items(body.stream()) {
                    if is_fn_item(&m) {
                        if let Some(n) = fn_name(&m) {
                            if has_pysig(&m) {
                                pysig_fns.insert(n.to_string());
                            }
                            if fn_returns_arg(&m) {
                                arg_fns.insert(n.to_string());
                            }
                            let s = n.to_string();
                            if opts.unittest
                                && trait_impl_parts(it).is_none()
                                && s.starts_with("test")
                                && !seen_tests.insert(s.clone())
                            {
                                shared_tests.insert(s);
                            }
                        }
                    }
                }
            }
        }
    }
    let ctx = Ctx {
        pysig_methods: pysig_fns.clone(),
        local_fns: local_fns.clone(),
        raw_params: raw_params.clone(),
        dynamic_self: opts.dynamic,
        dyn_objs: opts.modules.clone(),
        alias_classes: opts.dyn_classes.clone(),
        alias_objs: HashSet::new(),
        arg_locals: HashSet::new(),
        dyn_classes: dyn_classes.clone(),
        local_classes: local_classes.clone(),
        local_methods: local_methods.clone(),
        module_consts: module_consts.clone(),
        arg_fns: arg_fns.clone(),
        ..Default::default()
    };
    let mut out = TokenStream::new();
    for it in items {
        if model::is_py_model(&it) {
            let g = Gen::new(&ctx);
            out.extend(model::expand_py_model(&it, &g));
            continue;
        }
        if let Some(n) = py_class_name(&it) {
            if py_classes.contains(&n) {
                out.extend(py_class_with_dispatch(
                    &it,
                    &n,
                    class_dispatch.get(&n).map(Vec::as_slice).unwrap_or(&[]),
                    identifiable_types.contains(&n),
                ));
                continue;
            }
        }
        match item_kind(&it).as_deref() {
            Some("fn") => out.extend(expand_fn(it, &ctx)?),
            Some("impl") => {
                if let Some((tr, ty)) = trait_impl_parts(&it) {
                    if IDENTIFIABLE_MIXINS.contains(&tr.as_str()) {
                        let ty = Ident::new(&ty, Span::call_site());
                        out.extend(quote!(::laila::dict_identity!(#ty);));
                    }
                }
                out.extend(expand_impl(
                    it,
                    opts,
                    &local_fns,
                    &raw_params,
                    &dyn_classes,
                    &local_classes,
                    &local_methods,
                    &gated_types,
                    &pysig_fns,
                    &module_consts,
                    &shared_tests,
                    &arg_fns,
                )?)
            }
            Some("mod") => out.extend(expand_mod(it, opts)?),
            _ => out.extend(it.into_iter().collect::<TokenStream>()),
        }
    }
    Ok(out)
}

/// The secret gate of `_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT` on one
/// public method: the method is renamed `__gated__<name>` and a wrapper with
/// the original signature strips / verifies `secret=` through
/// `self.__gate__(name, args)` before forwarding (Python's `_gated` closure).
fn gate_method(item: Vec<TokenTree>, name: &Ident) -> (Vec<TokenTree>, TokenStream) {
    let (header, _) = match split_fn(&item) {
        Ok(h) => h,
        Err(_) => return (item, TokenStream::new()),
    };
    let (full_header, _) = return_type(&header);
    // `-> Result<..>` tokens of the (normalised) signature.
    let arrow = full_header.windows(2).position(|w| {
        matches!(&w[0], TokenTree::Punct(p) if p.as_char() == '-' && p.spacing() == proc_macro2::Spacing::Joint)
            && matches!(&w[1], TokenTree::Punct(p) if p.as_char() == '>')
    });
    let ret_ts: TokenStream = match arrow {
        Some(i) => full_header[i..].iter().cloned().collect(),
        None => quote!(-> ::laila::python::Result<()>),
    };
    // Parameter group after the name.
    let params = header.iter().enumerate().find_map(|(i, t)| {
        if matches!(t, TokenTree::Ident(n) if n == name) {
            match header.get(i + 1) {
                Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis => {
                    Some(g.clone())
                }
                _ => None,
            }
        } else {
            None
        }
    });
    let params = match params {
        Some(p) => p,
        None => return (item, TokenStream::new()),
    };
    let inner = Ident::new(&format!("__gated__{name}"), name.span());
    // Rename the method itself.
    let renamed: Vec<TokenTree> = {
        let mut out = Vec::with_capacity(item.len());
        let mut after_fn = false;
        for t in item {
            match &t {
                TokenTree::Ident(i) if i == "fn" => {
                    after_fn = true;
                    out.push(t);
                }
                TokenTree::Ident(i) if after_fn && i == name => {
                    after_fn = false;
                    out.push(TokenTree::Ident(inner.clone()));
                }
                _ => out.push(t),
            }
        }
        out
    };
    let name_s = name.to_string();
    let gate = quote!(<Self as ::laila::atomic::definitions::_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT>::__gate__);
    let wrapper = if has_pysig(&header) {
        quote! {
            pub fn #name(&self, args: impl ::core::convert::Into<::laila::python::Args>) #ret_ts {
                let __laila_args: ::laila::python::Args = ::core::convert::Into::into(args);
                let __laila_args = #gate(self, #name_s, __laila_args)?;
                self.#inner(__laila_args)
            }
        }
    } else {
        let names: Vec<Ident> = group_param_idents(&params);
        let params_ts = params.stream();
        quote! {
            pub fn #name(#params_ts) #ret_ts {
                #gate(self, #name_s, ::laila::python::Args::new())?;
                self.#inner(#(#names),*)
            }
        }
    };
    (renamed, wrapper)
}

/// Parameter names of a `(&self, a: T, b: U)` group, in order, without `self`.
fn group_param_idents(g: &Group) -> Vec<Ident> {
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

/// Split `[attrs] [vis] [async] fn name(...) [-> T] { body }` into header and body.
fn split_fn(tts: &[TokenTree]) -> Result<(Vec<TokenTree>, Group), syn::Error> {
    match tts.last() {
        Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace => {
            Ok((tts[..tts.len() - 1].to_vec(), g.clone()))
        }
        _ => Err(syn::Error::new(
            Span::call_site(),
            "laila::python: function needs a `{ ... }` body",
        )),
    }
}

fn fn_name(header: &[TokenTree]) -> Option<Ident> {
    let mut it = header.iter();
    while let Some(t) = it.next() {
        if matches!(t, TokenTree::Ident(i) if i == "fn") {
            if let Some(TokenTree::Ident(n)) = it.next() {
                return Some(n.clone());
            }
        }
    }
    None
}

/// Is the item a `fn` declared `-> Arg` (the header up to its body)?
fn fn_returns_arg(item: &[TokenTree]) -> bool {
    let mut i = 0;
    while i + 2 < item.len() {
        if let TokenTree::Group(g) = &item[i] {
            if g.delimiter() == Delimiter::Brace {
                return false;
            }
        }
        if matches!(&item[i], TokenTree::Punct(p) if p.as_char() == '-')
            && matches!(&item[i + 1], TokenTree::Punct(p) if p.as_char() == '>')
        {
            let ty: Vec<&TokenTree> = item[i + 2..]
                .iter()
                .take_while(
                    |t| !matches!(t, TokenTree::Group(g) if g.delimiter() == Delimiter::Brace),
                )
                .collect();
            let text = ty
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>()
                .join("");
            return matches!(
                text.as_str(),
                "Arg" | "::laila::python::Arg" | "laila::Arg" | "laila::python::Arg"
            );
        }
        i += 1;
    }
    false
}

/// Per positional parameter of a `fn name(a: T, b: U)` header: `true` when
/// the declared type is a plain Rust type rather than `Arg` (so a call
/// passes the lowered expression itself). `&self` / `self` are skipped.
fn fn_raw_params(item: &[TokenTree]) -> Vec<RawParam> {
    let mut it = item.iter();
    let mut params: Option<TokenStream> = None;
    while let Some(t) = it.next() {
        if matches!(t, TokenTree::Ident(i) if i == "fn") {
            it.next();
            if let Some(TokenTree::Group(g)) = it.next() {
                if g.delimiter() == Delimiter::Parenthesis {
                    params = Some(g.stream());
                }
            }
            break;
        }
    }
    let Some(params) = params else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for piece in crate::literal::split_commas(params) {
        let Some(colon) = piece
            .iter()
            .position(|t| matches!(t, TokenTree::Punct(p) if p.as_char() == ':'))
        else {
            continue; // `&self` / `self`
        };
        let ty: Vec<String> = piece[colon + 1..].iter().map(|t| t.to_string()).collect();
        let plain = !ty.is_empty()
            && !ty.iter().any(|t| t == "Arg" || t == "impl" || t == "Args")
            && ty[0] != "&";
        out.push(if !plain {
            RawParam::Dyn
        } else if ty.len() == 1 && ty[0] == "String" {
            RawParam::Str
        } else {
            RawParam::Plain
        });
    }
    out
}

/// The name of a `static NAME: T = ..;` item.
fn static_name(item: &[TokenTree]) -> Option<Ident> {
    let mut it = item.iter();
    while let Some(t) = it.next() {
        if matches!(t, TokenTree::Ident(i) if i == "static") {
            if let Some(TokenTree::Ident(n)) = it.next() {
                return Some(n.clone());
            }
        }
    }
    None
}

fn has_pysig(header: &[TokenTree]) -> bool {
    has_attr(header, "pysig")
}

/// Remove the outer `#[name ..]` attribute from an item, returning it.
fn take_attr(item: Vec<TokenTree>, name: &str) -> (Vec<TokenTree>, Option<TokenStream>) {
    let mut out = Vec::with_capacity(item.len());
    let mut found = None;
    let mut i = 0;
    while i < item.len() {
        if let (TokenTree::Punct(p), Some(TokenTree::Group(g))) = (&item[i], item.get(i + 1)) {
            if p.as_char() == '#'
                && g.delimiter() == Delimiter::Bracket
                && g.stream()
                    .into_iter()
                    .next()
                    .is_some_and(|t| matches!(t, TokenTree::Ident(id) if id == name))
            {
                found = Some(quote!(#p #g));
                i += 2;
                continue;
            }
        }
        out.push(item[i].clone());
        i += 1;
    }
    (out, found)
}

/// Every outer `#[name ..]` attribute, in order (stacked
/// `@unittest.skipUnless` decorators each become one attribute).
fn take_attrs(item: Vec<TokenTree>, name: &str) -> (Vec<TokenTree>, Vec<TokenStream>) {
    let mut out = Vec::with_capacity(item.len());
    let mut found = Vec::new();
    let mut i = 0;
    while i < item.len() {
        if let (TokenTree::Punct(p), Some(TokenTree::Group(g))) = (&item[i], item.get(i + 1)) {
            if p.as_char() == '#'
                && g.delimiter() == Delimiter::Bracket
                && g.stream()
                    .into_iter()
                    .next()
                    .is_some_and(|t| matches!(t, TokenTree::Ident(id) if id == name))
            {
                found.push(quote!(#p #g));
                i += 2;
                continue;
            }
        }
        out.push(item[i].clone());
        i += 1;
    }
    (out, found)
}

/// The body of a `#[skip_unless(cond, "reason")]` / `#[skip_if(..)]`
/// attribute as the statement that raises `unittest.SkipTest` when the
/// condition (a Python expression) says so.
fn class_skip_check(
    attr: TokenStream,
    unless: bool,
    module_consts: &HashSet<String>,
    local_fns: &HashSet<String>,
) -> Result<TokenStream, syn::Error> {
    let group = attr.into_iter().find_map(|t| match t {
        TokenTree::Group(g) if g.delimiter() == Delimiter::Bracket => Some(g),
        _ => None,
    });
    let inner = group
        .and_then(|g| {
            g.stream().into_iter().find_map(|t| match t {
                TokenTree::Group(g) if g.delimiter() == Delimiter::Parenthesis => Some(g.stream()),
                _ => None,
            })
        })
        .ok_or_else(|| {
            syn::Error::new(
                Span::call_site(),
                "laila::python: `#[skip_unless(cond, \"reason\")]`",
            )
        })?;
    let parts = crate::literal::split_commas(inner);
    let (cond, reason): (Vec<TokenTree>, TokenStream) = match parts.as_slice() {
        [c, r] => (c.clone(), r.iter().cloned().collect()),
        _ => {
            return Err(syn::Error::new(
                Span::call_site(),
                "laila::python: `#[skip_unless(cond, \"reason\")]` takes two arguments",
            ))
        }
    };
    let mut cur = parse::Cur::from_tts(cond);
    let cond_expr = parse::parse_expr(&mut cur)?;
    let ctx = Ctx {
        module_consts: module_consts.clone(),
        local_fns: local_fns.clone(),
        ..Default::default()
    };
    let g = Gen::new(&ctx);
    let cond_ts = g.cond(&cond_expr);
    let test = if unless {
        quote!(!(#cond_ts))
    } else {
        quote!(#cond_ts)
    };
    Ok(quote!(if #test { return ::laila::python::unittest::skip_test(#reason); }))
}

/// Does the item carry an outer `#[name ..]` attribute?
fn has_attr(header: &[TokenTree], name: &str) -> bool {
    header.windows(2).any(|w| {
        matches!(&w[0], TokenTree::Punct(p) if p.as_char() == '#')
            && matches!(&w[1], TokenTree::Group(g) if g.delimiter() == Delimiter::Bracket
                && g.stream().into_iter().any(|t| matches!(t, TokenTree::Ident(i) if i == name)))
    })
}

/// Classify the declared return type and produce the final one.
fn return_type(header: &[TokenTree]) -> (Vec<TokenTree>, RetKind) {
    let arrow = header.windows(2).position(|w| {
        matches!(&w[0], TokenTree::Punct(p) if p.as_char() == '-' && p.spacing() == proc_macro2::Spacing::Joint)
            && matches!(&w[1], TokenTree::Punct(p) if p.as_char() == '>')
    });
    match arrow {
        None => {
            let mut h = header.to_vec();
            // `where` clauses are not expected on Python-shaped fns.
            h.extend(quote!(-> ::laila::python::Result<()>));
            (h, RetKind::Unit)
        }
        Some(i) => {
            let ty: TokenStream = header[i + 2..].iter().cloned().collect();
            let text = ty.to_string().replace(' ', "");
            let head: Vec<TokenTree> = header[..i].to_vec();
            if text.contains("Result<") {
                let inner_arg = text.contains("Result<Arg>")
                    || text.contains("Result<::laila::python::Arg>")
                    || text.contains("Result<laila::Arg>");
                let kind = if inner_arg {
                    RetKind::Arg
                } else if text.contains("Result<()>") {
                    RetKind::Unit
                } else {
                    RetKind::UserResult
                };
                let mut h = head;
                h.extend(quote!(-> #ty));
                (h, kind)
            } else if text == "Arg"
                || text == "::laila::python::Arg"
                || text == "laila::Arg"
                || text == "laila::python::Arg"
            {
                let mut h = head;
                h.extend(quote!(-> ::laila::python::Result<::laila::python::Arg>));
                (h, RetKind::Arg)
            } else if text == "()" {
                let mut h = head;
                h.extend(quote!(-> ::laila::python::Result<()>));
                (h, RetKind::Unit)
            } else {
                let mut h = head;
                h.extend(quote!(-> ::laila::python::Result<#ty>));
                (h, RetKind::Other)
            }
        }
    }
}

pub fn expand_fn(tts: Vec<TokenTree>, ctx: &Ctx) -> Result<TokenStream, syn::Error> {
    let (header, body) = split_fn(&tts)?;
    let (header, ret) = return_type(&header);
    let block = parse::parse_block(&body)?;
    let mut bound = param_names(&header);
    let mut dynamic_vars = HashSet::new();
    gen::scan_bindings(&block, &mut bound, &mut dynamic_vars);
    let cell_vars = gen::scan_cell_vars(&block, &dynamic_vars);
    // `py_model!` classes declared inside the body are dynamic classes too.
    let mut dyn_classes = ctx.dyn_classes.clone();
    gen::scan_model_items(&block, &mut dyn_classes);
    let mut dyn_objs = ctx.dyn_objs.clone();
    gen::scan_dyn_objs(&block, &mut dyn_objs, &dyn_classes);
    let mut alias_objs = ctx.alias_objs.clone();
    gen::scan_dyn_objs(&block, &mut alias_objs, &ctx.alias_classes);
    let mut arg_locals = ctx.arg_locals.clone();
    // `fn f(&self, pair: Arg)`: an `Arg`-typed parameter is an `Arg` local.
    arg_locals.extend(param_arg_names(&header));
    if ctx.dynamic_self {
        gen::scan_arg_locals_dyn(&block, &mut arg_locals, &ctx.arg_fns);
    } else {
        gen::scan_arg_locals(&block, &mut arg_locals, &ctx.arg_fns);
    }
    let mut bare_instances = ctx.bare_instances.clone();
    gen::scan_bare_instances(&block, &mut bare_instances);
    let ctx = Ctx {
        ret: ret.clone(),
        bound,
        dynamic_vars,
        cell_vars,
        dyn_objs,
        alias_objs,
        arg_locals,
        dyn_classes,
        bare_instances,
        ..ctx.clone()
    };
    let g = Gen::new(&ctx);
    let tail = match ret {
        RetKind::Unit => Some(quote!(::core::result::Result::Ok(()))),
        RetKind::Arg => Some(quote!(::core::result::Result::Ok(
            ::laila::python::Arg::None
        ))),
        RetKind::Other => None,
        RetKind::UserResult => None,
    };
    let body_ts = match (&ret, block.stmts.last()) {
        (RetKind::Other, Some(ast::Stmt::Expr { semi: false, .. })) => {
            // trailing expression becomes Ok(expr)
            let mut stmts = block.stmts.clone();
            let last = stmts.pop().unwrap();
            let head = ast::Block {
                stmts,
                span: block.span,
            };
            let last_ts = match last {
                ast::Stmt::Expr { expr, .. } => g.expr(&expr),
                _ => unreachable!(),
            };
            g.block(&head, Some(quote!(::core::result::Result::Ok(#last_ts))))
        }
        // `def f() -> bool: try: ..; return True except: return False`: the
        // `try` carries the returns [P16c]; falling off the end is Python's
        // implicit `None`, a type error for `-> T`.
        (RetKind::Other, Some(ast::Stmt::Try { .. })) => g.block(
            &block,
            Some(quote!(::core::result::Result::Err(
                ::laila::python::LailaError::type_error(
                    "function with a typed return fell off its end (implicit None)"
                )
            ))),
        ),
        _ => g.block(&block, tail),
    };
    let header_ts: TokenStream = header.into_iter().collect();
    Ok(
        quote!(#header_ts { #[allow(unreachable_code, unused_braces, unused_parens, clippy::all)] #body_ts }),
    )
}

/// Parameter names of `fn name(a: T, mut b: U, (c, d): V)` (the first
/// parenthesised group after the name), plus `#[pysig]`-bound names.
/// The parameters of `header` whose type is exactly `Arg` (`pair: Arg`).
fn param_arg_names(header: &[TokenTree]) -> HashSet<String> {
    let mut out = HashSet::new();
    let mut seen_fn = false;
    for t in header {
        match t {
            TokenTree::Ident(i) if i == "fn" => seen_fn = true,
            TokenTree::Group(g) if seen_fn && g.delimiter() == Delimiter::Parenthesis => {
                let tts: Vec<TokenTree> = g.stream().into_iter().collect();
                // split on top-level commas
                let mut depth = 0i32;
                let mut cur: Vec<&TokenTree> = Vec::new();
                let mut parts: Vec<Vec<&TokenTree>> = Vec::new();
                for p in &tts {
                    match p {
                        TokenTree::Punct(pu) if pu.as_char() == '<' => depth += 1,
                        TokenTree::Punct(pu) if pu.as_char() == '>' => depth -= 1,
                        TokenTree::Punct(pu) if pu.as_char() == ',' && depth == 0 => {
                            parts.push(core::mem::take(&mut cur));
                            continue;
                        }
                        _ => {}
                    }
                    cur.push(p);
                }
                if !cur.is_empty() {
                    parts.push(cur);
                }
                for part in parts {
                    let part: Vec<&TokenTree> = part
                        .into_iter()
                        .filter(|t| !matches!(t, TokenTree::Ident(i) if i == "mut"))
                        .collect();
                    if let [TokenTree::Ident(name), TokenTree::Punct(c), TokenTree::Ident(ty)] =
                        part[..]
                    {
                        if c.as_char() == ':' && ty == "Arg" && name != "self" {
                            out.insert(name.to_string());
                        }
                    }
                }
                break;
            }
            TokenTree::Group(g) if g.delimiter() == Delimiter::Bracket => {
                // `#[pysig("f(a, b=1, *args, **kw)")]`: every plain bound name
                // is an `Arg` (`*args` is a `Vec`, `**kw` an `Args`).
                let text = g.stream().to_string();
                if let Some(start) = text.find("pysig") {
                    for name in pysig_bound_names(&text[start..]) {
                        out.insert(name);
                    }
                }
            }
            _ => {}
        }
    }
    out
}

fn param_names(header: &[TokenTree]) -> HashSet<String> {
    let mut out = HashSet::new();
    let mut seen_fn = false;
    for t in header {
        match t {
            TokenTree::Ident(i) if i == "fn" => seen_fn = true,
            TokenTree::Group(g) if seen_fn && g.delimiter() == Delimiter::Parenthesis => {
                let mut depth = 0usize;
                let mut expect_name = true;
                for p in g.stream() {
                    match p {
                        TokenTree::Punct(pu) if pu.as_char() == ',' && depth == 0 => {
                            expect_name = true
                        }
                        TokenTree::Punct(pu) if pu.as_char() == ':' && depth == 0 => {
                            expect_name = false
                        }
                        TokenTree::Punct(pu) if pu.as_char() == '<' => depth += 1,
                        TokenTree::Punct(pu) if pu.as_char() == '>' => {
                            depth = depth.saturating_sub(1)
                        }
                        TokenTree::Ident(i)
                            if expect_name
                                && depth == 0
                                && i != "mut"
                                && i != "self"
                                && i != "ref" =>
                        {
                            out.insert(i.to_string());
                        }
                        TokenTree::Group(inner) if expect_name && depth == 0 => {
                            for q in inner.stream() {
                                if let TokenTree::Ident(i) = q {
                                    out.insert(i.to_string());
                                }
                            }
                        }
                        _ => {}
                    }
                }
                break;
            }
            TokenTree::Group(g) if g.delimiter() == Delimiter::Bracket => {
                // #[pysig("f(a, b=1, *args, **kw)")] binds those names as locals
                let text = g.stream().to_string();
                if let Some(start) = text.find("pysig") {
                    for name in pysig_all_names(&text[start..]) {
                        out.insert(name);
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// The parameter list of a `pysig("f(a, b=1, *args, **kw)")` attribute text:
/// `[("a", false), ("b", false), ("args", true), ("kw", true)]` (the bool is
/// "starred"). Parens of the *signature* (inside the quotes), not the outer
/// `pysig(...)` call, are what delimit the list.
fn pysig_params(attr_text: &str) -> Vec<(String, bool)> {
    let mut out = Vec::new();
    // the quoted signature
    let Some(q0) = attr_text.find('"') else {
        return out;
    };
    // the closing quote: the first unescaped `"` (`codec=\"json\"` defaults
    // are escaped inside the attribute's string literal)
    let rest = &attr_text[q0 + 1..];
    let mut q1 = None;
    let mut prev_backslash = false;
    for (i, c) in rest.char_indices() {
        if c == '"' && !prev_backslash {
            q1 = Some(i);
            break;
        }
        prev_backslash = c == '\\' && !prev_backslash;
    }
    let Some(q1) = q1 else {
        return out;
    };
    let sig = &rest[..q1];
    let (Some(l), Some(r)) = (sig.find('('), sig.rfind(')')) else {
        return out;
    };
    if r <= l {
        return out;
    }
    for part in sig[l + 1..r].split(',') {
        let part = part.trim();
        let starred = part.starts_with('*');
        let name = part
            .trim_start_matches('*')
            .split('=')
            .next()
            .unwrap_or("")
            .split(':')
            .next()
            .unwrap_or("")
            .trim();
        if !name.is_empty()
            && name != "self"
            && name.chars().all(|c| c.is_alphanumeric() || c == '_')
        {
            out.push((name.to_string(), starred));
        }
    }
    out
}

/// Plain (non-starred) `#[pysig]` names: each is bound as an `Arg`.
fn pysig_bound_names(attr_text: &str) -> Vec<String> {
    pysig_params(attr_text)
        .into_iter()
        .filter(|(_, starred)| !starred)
        .map(|(n, _)| n)
        .collect()
}

/// Every `#[pysig]` name including `*args` / `**kw`: all are locals.
fn pysig_all_names(attr_text: &str) -> Vec<String> {
    pysig_params(attr_text)
        .into_iter()
        .map(|(n, _)| n)
        .collect()
}

/// Items inside an impl/mod block, each as its token list.
fn split_items(ts: TokenStream) -> Vec<Vec<TokenTree>> {
    let mut out: Vec<Vec<TokenTree>> = Vec::new();
    let mut cur: Vec<TokenTree> = Vec::new();
    let mut saw_fn = false;
    for tt in ts {
        match &tt {
            TokenTree::Punct(p) if p.as_char() == ';' && !saw_fn => {
                cur.push(tt);
                out.push(std::mem::take(&mut cur));
            }
            TokenTree::Group(g) if g.delimiter() == Delimiter::Brace && saw_fn => {
                cur.push(tt);
                out.push(std::mem::take(&mut cur));
                saw_fn = false;
            }
            TokenTree::Ident(i) if i == "fn" => {
                saw_fn = true;
                cur.push(tt);
            }
            TokenTree::Group(g) if g.delimiter() == Delimiter::Brace && !saw_fn => {
                // struct/enum/impl bodies: end of a non-fn item
                cur.push(tt);
                out.push(std::mem::take(&mut cur));
            }
            _ => cur.push(tt),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn is_fn_item(item: &[TokenTree]) -> bool {
    item.iter()
        .any(|t| matches!(t, TokenTree::Ident(i) if i == "fn"))
}

#[allow(clippy::too_many_arguments)]
pub fn expand_impl(
    tts: Vec<TokenTree>,
    opts: &Opts,
    local_fns: &HashSet<String>,
    raw_params: &HashMap<String, Vec<RawParam>>,
    dyn_classes: &HashSet<String>,
    local_classes: &HashSet<String>,
    local_methods: &HashSet<String>,
    gated_types: &HashSet<String>,
    block_pysig: &HashSet<String>,
    module_consts: &HashSet<String>,
    shared_tests: &HashSet<String>,
    arg_fns: &HashSet<String>,
) -> Result<TokenStream, syn::Error> {
    let (header, body) = match tts.last() {
        Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace => {
            (tts[..tts.len() - 1].to_vec(), g.clone())
        }
        _ => {
            return Err(syn::Error::new(
                Span::call_site(),
                "laila::python: impl needs a body",
            ))
        }
    };
    // `#[skip_unless(cond, "reason")]` / `#[skip_if(cond, "reason")]` on a
    // TestCase impl (`@unittest.skipUnless` on the class): every generated
    // `#[test]` reports itself skipped when the condition says so.
    let (header, skip_unless) = take_attrs(header, "skip_unless");
    let (header, skip_if) = take_attrs(header, "skip_if");
    let class_skip: Option<TokenStream> = {
        let mut checks = Vec::new();
        for a in skip_unless {
            checks.push(class_skip_check(a, true, module_consts, local_fns)?);
        }
        for a in skip_if {
            checks.push(class_skip_check(a, false, module_consts, local_fns)?);
        }
        if checks.is_empty() {
            None
        } else {
            Some(quote!(#(#checks)*))
        }
    };
    // Self type: the last path segment before the body (after `for`, if any).
    let self_ty: TokenStream = {
        let mut after_impl: Vec<TokenTree> = Vec::new();
        let mut seen_impl = false;
        for t in &header {
            if seen_impl {
                after_impl.push(t.clone());
            }
            if matches!(t, TokenTree::Ident(i) if i == "impl") {
                seen_impl = true;
            }
        }
        if let Some(pos) = after_impl
            .iter()
            .position(|t| matches!(t, TokenTree::Ident(i) if i == "for"))
        {
            after_impl[pos + 1..].iter().cloned().collect()
        } else {
            after_impl.into_iter().collect()
        }
    };
    let is_trait_impl = header
        .iter()
        .any(|t| matches!(t, TokenTree::Ident(i) if i == "for"));
    let items = split_items(body.stream());
    let exit_fn: Option<Vec<TokenTree>> = items
        .iter()
        .find(|it| is_fn_item(it) && fn_name(it).is_some_and(|n| n == "__exit__"))
        .cloned();
    let mut methods: HashSet<String> = HashSet::new();
    let mut pysig_methods: HashSet<String> = block_pysig.clone();
    // Methods with plainly typed parameters (`fn _write(&self, name: String)`,
    // from `def _write(self, name: str)`): `self._write(..)` passes the
    // lowered expressions, like a module-level `fn` with the same header.
    let mut raw_params = raw_params.clone();
    let mut class_attrs: HashSet<String> = HashSet::new();
    for it in &items {
        if is_fn_item(it) {
            if let Some(n) = fn_name(it) {
                methods.insert(n.to_string());
                if has_pysig(it) {
                    pysig_methods.insert(n.to_string());
                }
                if has_attr(it, "class_attr") {
                    class_attrs.insert(n.to_string());
                }
                let raw = fn_raw_params(it);
                if raw.iter().any(|r| *r != RawParam::Dyn) {
                    raw_params.insert(n.to_string(), raw);
                }
            }
        }
    }
    if opts.unittest {
        for m in TESTCASE_METHODS {
            methods.insert((*m).to_string());
        }
    }
    if opts.dynamic {
        methods.insert("__getattr__".into());
        methods.insert("__setattr__".into());
    }
    let ctx = Ctx {
        impl_methods: methods,
        class_attrs,
        pysig_methods,
        local_fns: local_fns.clone(),
        raw_params: raw_params.clone(),
        dynamic_self: opts.dynamic,
        dyn_objs: opts.modules.clone(),
        alias_classes: opts.dyn_classes.clone(),
        alias_objs: HashSet::new(),
        arg_locals: HashSet::new(),
        dyn_classes: dyn_classes.clone(),
        local_classes: local_classes.clone(),
        local_methods: local_methods.clone(),
        module_consts: module_consts.clone(),
        arg_fns: arg_fns.clone(),
        ..Default::default()
    };
    let gated = !is_trait_impl && gated_types.contains(&self_ty.to_string().replace(' ', ""));
    let mut out_items = Vec::new();
    let mut test_names: Vec<Ident> = Vec::new();
    let mut ignores: Vec<Option<TokenStream>> = Vec::new();
    let mut has_setup = false;
    let mut has_teardown = false;
    let mut has_class_setup = false;
    let mut has_class_teardown = false;
    for it in items {
        if is_fn_item(&it) {
            let name = fn_name(&it);
            // `#[classmethod]` (py2rs: `@classmethod`): the method takes the
            // class handle as `&self` (`cls` is `Self::__class__()`); the
            // marker itself has no Rust meaning.
            let (it, _) = take_attr(it, "classmethod");
            let mut it = it;
            if let Some(n) = &name {
                let s = n.to_string();
                if s == "setUpClass" {
                    has_class_setup = true;
                }
                if s == "tearDownClass" {
                    has_class_teardown = true;
                }
                if s.starts_with("test_")
                    || s.starts_with("test")
                        && s.chars()
                            .nth(4)
                            .map(|c| c.is_ascii_uppercase() || c == '_')
                            .unwrap_or(false)
                {
                    test_names.push(n.clone());
                    // `#[ignore = ".."]` (py2rs `skip_tests`, `@unittest.skip`)
                    // belongs on the generated `#[test]`, not on the method.
                    let (stripped, ignore) = take_attr(it, "ignore");
                    it = stripped;
                    ignores.push(ignore);
                }
                if s == "setUp" {
                    has_setup = true;
                }
                if s == "tearDown" {
                    has_teardown = true;
                }
                // Public callables of a globally-atomic subclass are gated
                // (class attributes -- `#[class_attr]` value getters -- are not callables).
                if gated
                    && !s.starts_with('_')
                    && !UNGATED_NAMES.contains(&s.as_str())
                    && !TESTCASE_METHODS.contains(&s.as_str())
                    && !has_attr(&it, "class_attr")
                {
                    let (renamed, wrapper) = gate_method(it, n);
                    it = renamed;
                    out_items.push(wrapper);
                }
            }
            out_items.push(expand_fn(it, &ctx)?);
        } else {
            out_items.push(it.into_iter().collect());
        }
    }
    let header_ts: TokenStream = header.into_iter().collect();
    let mut out = quote!(#header_ts { #(#out_items)* });
    // A class defining `__exit__` is a context manager: `with Cls(..) as x:`
    // runs it on the normal exit path (`RaisingExit`), with the three
    // `None`s Python passes when no exception is active.
    if !is_trait_impl {
        if let Some(exit_fn) = exit_fn {
            let exit_args: TokenStream = if has_pysig(&exit_fn) {
                quote!(::laila::python::Args::new())
            } else {
                let n = fn_raw_params(&exit_fn).len();
                let nones = (0..n).map(|_| quote!(::laila::python::Arg::None));
                quote!(#(#nones),*)
            };
            out.extend(quote!(
                impl ::laila::python::context::RaisingExit for #self_ty {
                    fn __laila_exit(&self) -> ::laila::python::Result<()> {
                        #[allow(unused_imports)]
                        use ::laila::python::context::ExitOutcome as _;
                        self.__exit__(#exit_args).__laila_exit_outcome()
                    }
                }
            ));
        }
    }
    if opts.unittest && !is_trait_impl {
        out.extend(quote!(impl ::laila::python::unittest::TestCase for #self_ty {}));
        let class_name = self_ty.to_string().replace(' ', "");
        let total = test_names.len();
        // `setUpClass` once before the class's first test, `tearDownClass`
        // once after its last (see `unittest::class_set_up`).
        let class_setup = if has_class_setup || has_class_teardown {
            let set_up = if has_class_setup {
                quote!(|| <#self_ty>::__class__().setUpClass())
            } else {
                quote!(|| ::core::result::Result::Ok(()))
            };
            let tear_down = if has_class_teardown {
                quote!(|| <#self_ty>::__class__().tearDownClass())
            } else {
                quote!(|| ::core::result::Result::Ok(()))
            };
            quote!(::laila::python::unittest::class_set_up(#class_name, #total, #set_up, #tear_down)?;)
        } else {
            quote!()
        };
        let class_done = if has_class_setup || has_class_teardown {
            quote!(let __laila_cd = ::laila::python::unittest::class_test_done(#class_name);)
        } else {
            quote!(let __laila_cd: ::laila::python::Result<()> = Ok(());)
        };
        for (t, ignore) in test_names.into_iter().zip(ignores) {
            let qual = format!("{}.{}", class_name, t);
            // A test method several classes of the block define (a mixin's
            // test copied into each test case, P12b) gets a class-qualified
            // wrapper, `Class__test_x` -- pytest's `Class::test_x` [P12c].
            let wrapper = if shared_tests.contains(&t.to_string()) {
                Ident::new(&format!("{class_name}__{t}"), t.span())
            } else {
                t.clone()
            };
            let setup = if has_setup {
                quote!(__laila_t.setUp()?;)
            } else {
                quote!()
            };
            let teardown = if has_teardown {
                quote!(let __laila_td = __laila_t.tearDown();)
            } else {
                quote!(let __laila_td: ::laila::python::Result<()> = Ok(());)
            };
            let skip = class_skip.as_ref().map(|c| quote!(#c));
            out.extend(quote_spanned!(t.span() =>
                #[test]
                #ignore
                fn #wrapper() {
                    let __laila_lock = ::laila::python::unittest::test_lock();
                    let __laila_t = <#self_ty as ::core::default::Default>::default();
                    ::laila::python::unittest::run_test(#qual, || {
                        #skip
                        #class_setup
                        #setup
                        let __laila_r = __laila_t.#t();
                        #teardown
                        #class_done
                        __laila_r?;
                        __laila_td?;
                        __laila_cd
                    });
                    drop(__laila_lock);
                }
            ));
        }
    }
    Ok(out)
}

pub fn expand_mod(tts: Vec<TokenTree>, opts: &Opts) -> Result<TokenStream, syn::Error> {
    let (header, body) = match tts.last() {
        Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace => {
            (tts[..tts.len() - 1].to_vec(), g.clone())
        }
        _ => {
            return Err(syn::Error::new(
                Span::call_site(),
                "laila::python: mod needs a body",
            ))
        }
    };
    let mut inner: Vec<TokenTree> = body.stream().into_iter().collect();
    let mut mod_opts = parse_inner_opts(&mut inner)?;
    mod_opts.unittest |= opts.unittest;
    mod_opts.dynamic |= opts.dynamic;
    mod_opts.functions.extend(opts.functions.iter().cloned());
    let items = expand_items(inner, &mod_opts)?;
    let header_ts: TokenStream = header.into_iter().collect();
    Ok(quote!(#header_ts { #items }))
}
