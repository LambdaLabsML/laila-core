//! `py_model! { Name(Base, ..) { field: ann = value, .. } }` -- a pydantic
//! model class (P14b).
//!
//! py2rs emits this for a Python class whose body has annotated class
//! attributes (`host: str = Field(default="0.0.0.0")`), the pydantic way of
//! declaring fields. The macro lowers each member's value expression with
//! the ordinary Tier 2 rules (so `Field(..)`, `PrivateAttr(..)`,
//! `CLIExempt(..)`, `lambda: [..]` all work) and emits:
//!
//! * a unit struct `Name` -- the *class* as a value (`_eligible_model_fields(Name)`),
//! * `Name::__class__()` -- the lazily built `ModelClass` descriptor,
//! * `Name::new(args)` -- `Name(**kwargs)` returning the instance as an `Arg`,
//! * `Name::model_fields()` -- `Name.model_fields`,
//! * `ModelBase` / `PyArg` / `IsInstance` impls so the class can be a base of
//!   another `py_model!`, be passed around, and be used in `isinstance`.
//!
//! Instances are attribute-dynamic (`comm.host` lowers to `__getattr__`), so
//! the class name is recorded as a dynamic class for the enclosing block.

use proc_macro2::{Delimiter, Ident, Span, TokenStream, TokenTree};
use quote::{quote, quote_spanned};

use super::gen::Gen;
use super::parse::{parse_expr, Cur};

/// Is this item / statement a `py_model! { .. }` invocation?
pub fn is_py_model(tts: &[TokenTree]) -> bool {
    matches!(tts.first(), Some(TokenTree::Ident(i)) if i == "py_model")
        && matches!(tts.get(1), Some(TokenTree::Punct(p)) if p.as_char() == '!')
}

/// The class name declared by a `py_model!` invocation.
pub fn py_model_name(tts: &[TokenTree]) -> Option<String> {
    let body = tts.iter().find_map(|t| match t {
        TokenTree::Group(g) => Some(g.clone()),
        _ => None,
    })?;
    body.stream().into_iter().find_map(|t| match t {
        TokenTree::Ident(i) => Some(i.to_string()),
        _ => None,
    })
}

struct Member {
    name: Ident,
    annotation: String,
    value: TokenStream,
}

fn err(span: Span, msg: &str) -> TokenStream {
    let msg = format!("py_model!: {msg}");
    quote_spanned!(span => compile_error!(#msg);)
}

/// Lower `py_model! { Name(Bases..) { members.. } }` to the class items.
pub fn expand_py_model(tts: &[TokenTree], g: &Gen<'_>) -> TokenStream {
    let span = tts
        .first()
        .map(|t| t.span())
        .unwrap_or_else(Span::call_site);
    let group = match tts.iter().find_map(|t| match t {
        TokenTree::Group(gr) if gr.delimiter() == Delimiter::Brace => Some(gr.clone()),
        _ => None,
    }) {
        Some(gr) => gr,
        None => return err(span, "expected `py_model! { Name(Bases) { .. } }`"),
    };
    let mut cur = Cur::new(group.stream());
    let name = match cur.next() {
        Some(TokenTree::Ident(i)) => i,
        _ => return err(span, "expected the class name"),
    };
    let bases: Vec<TokenStream> = match cur.eat_group(Delimiter::Parenthesis) {
        Some(b) => crate::literal::split_commas(b.stream())
            .into_iter()
            .filter(|t| !t.is_empty())
            .map(|t| t.into_iter().collect())
            .collect(),
        None => Vec::new(),
    };
    let body = match cur.eat_group(Delimiter::Brace) {
        Some(b) => b,
        None => return err(span, "expected the member list `{ .. }`"),
    };
    let mut members: Vec<Member> = Vec::new();
    for item in crate::literal::split_commas(body.stream()) {
        if item.is_empty() {
            continue;
        }
        let mut c = Cur::from_tts(item.clone());
        let mname = match c.next() {
            Some(TokenTree::Ident(i)) => i,
            Some(other) => return err(other.span(), "expected a member name"),
            None => continue,
        };
        let mut annotation = String::new();
        if c.eat_punct(':') {
            let ann = c.take_until(|cc| cc.peek_punct('=') && !cc.peek_op_is("=="));
            annotation = ann.to_string().replace(' ', "");
        }
        if !c.eat_punct('=') {
            return err(mname.span(), "expected `name: annotation = value`");
        }
        let value = match parse_expr(&mut c) {
            Ok(e) => g.expr(&e),
            Err(e) => return e.to_compile_error(),
        };
        members.push(Member {
            name: mname,
            annotation,
            value,
        });
    }
    let name_str = name.to_string();
    let base_exprs = bases
        .iter()
        .map(|b| quote!(<#b as ::laila::_compat::pydantic::ModelBase>::__class__()));
    let member_stmts = members.iter().map(|m| {
        let n = m.name.to_string();
        let n = n.strip_prefix("r#").unwrap_or(&n).to_string();
        let ann = &m.annotation;
        let v = &m.value;
        quote!(::laila::_compat::pydantic::classify_member(
            #n, #ann, ::laila::python::IntoArg::into_arg(#v), &__bases,
            &mut __fields, &mut __private, &mut __class_vars,
        );)
    });
    quote_spanned!(span =>
        #[allow(non_camel_case_types, dead_code)]
        #[derive(Clone, Copy, Debug, Default)]
        pub struct #name;
        #[allow(dead_code, non_snake_case)]
        impl #name {
            /// The class descriptor (built once).
            pub fn __class__() -> &'static ::laila::_compat::pydantic::ModelClass {
                static CLASS: ::laila::_compat::pydantic::LazyClass =
                    ::laila::_compat::pydantic::LazyClass::new(|| {
                        (|| -> ::laila::python::Result<::laila::_compat::pydantic::ModelClass> {
                            let __bases: ::laila::python::__rt::Vec<&'static ::laila::_compat::pydantic::ModelClass> =
                                ::laila::python::__rt::vec![#(#base_exprs),*];
                            let mut __fields = ::laila::python::__rt::Vec::new();
                            let mut __private = ::laila::python::__rt::Vec::new();
                            let mut __class_vars = ::laila::python::__rt::Vec::new();
                            #(#member_stmts)*
                            ::core::result::Result::Ok(::laila::_compat::pydantic::ModelClass {
                                name: #name_str,
                                module: module_path!(),
                                bases: __bases,
                                fields: __fields,
                                private_attrs: __private,
                                class_vars: __class_vars,
                                hooks: ::core::option::Option::None,
                            })
                        })()
                        .unwrap_or_else(|e| panic!("py_model! {}: {}", #name_str, e))
                    });
                let c: &'static ::laila::_compat::pydantic::ModelClass = &CLASS;
                ::laila::basics::definitions::cli_capable::register_class(c);
                c
            }
            /// `Name(**kwargs)`
            #[allow(clippy::new_ret_no_self)]
            pub fn new(args: impl ::core::convert::Into<::laila::python::Args>) -> ::laila::python::Result<::laila::python::Arg> {
                ::laila::_compat::pydantic::ModelInstance::construct(Self::__class__(), args.into())
            }
            /// `Name.model_fields`
            pub fn model_fields() -> ::laila::python::Arg {
                Self::__class__().model_fields()
            }
            /// `Name.__private_attributes__`
            pub fn __private_attributes__() -> ::laila::python::Arg {
                Self::__class__().__private_attributes__()
            }
            /// `Name.__name__`
            pub fn __name__() -> ::laila::python::Arg {
                ::laila::python::Arg::Str(#name_str.into())
            }
        }
        impl ::laila::_compat::pydantic::ModelBase for #name {
            fn __class__() -> &'static ::laila::_compat::pydantic::ModelClass {
                #name::__class__()
            }
        }
        impl ::laila::python::PyArg for #name {
            fn to_arg(&self) -> ::laila::python::Arg {
                ::laila::python::IntoArg::into_arg(Self::__class__().as_pyclass())
            }
        }
        impl ::laila::python::builtins::IsInstance for #name {
            fn isinstance(x: &::laila::python::Arg) -> bool {
                ::laila::_compat::pydantic::isinstance_model(x, #name_str)
            }
        }
    )
}
