//! Proc-macros for `laila-core`.
//!
//! - `laila::python! { ... }`: items whose bodies use Python's expression
//!   syntax (dict literals, `k=v` calls, `not`/`and`/`or`/`in`/`is`, `with`,
//!   `raise`, f-strings, comprehensions, lambdas). Options are inner
//!   attributes at the top of the block: `#![unittest]`, `#![dynamic]`,
//!   `#![functions(a, b)]` (imported Python functions callable by name).
//!   This is function-like (not an attribute) because rustc parses the body
//!   of an attributed item as Rust before the macro ever sees it.
//! - `#[laila::pysig("f(a, b=1, *args, **kw)")]`: bind `Args` with Python's rules.
//! - `laila::args!(a, k = v, *rest, **kw)`: build an `Args`.
//! - `laila::py!({"k": [1, 2]})`: build an `Arg` from a Python literal.

mod literal;
mod rewrite;
mod sig;

use proc_macro::TokenStream;

#[proc_macro]
pub fn python(input: TokenStream) -> TokenStream {
    let out = rewrite::expand(input.into());
    // `LAILA_PY_DUMP=<dir>`: write every expansion to `<dir>/<n>.rs` (a
    // debugging aid for errors rustc reports against the whole block).
    if let Ok(dir) = std::env::var("LAILA_PY_DUMP") {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static N: AtomicUsize = AtomicUsize::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(format!("{dir}/{pid}_{n}.rs"), out.to_string());
    }
    out.into()
}

#[proc_macro_attribute]
pub fn pysig(attr: TokenStream, item: TokenStream) -> TokenStream {
    sig::expand(attr.into(), item.into()).into()
}

/// `#[class_attr]` marks the `fn name(&self) -> Arg` py2rs emits for a class
/// attribute / pydantic field (`class C: n: int = 0`, rule P14). The function
/// is a *value*, not a Python callable: `laila::python!` reads it as the
/// instance-attribute default and never installs the secret gate on it. The
/// attribute itself is a no-op marker.
#[proc_macro_attribute]
pub fn class_attr(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// `#[property]` marks a `fn name(&self) -> T` py2rs emits for a Python
/// `@property`: `laila::python!` reaches it from a dynamic attribute read on
/// an instance passed as a value (`pr.ca`). A no-op marker.
#[proc_macro_attribute]
pub fn property(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro]
pub fn args(input: TokenStream) -> TokenStream {
    literal::expand_args(input.into(), &|t| t).into()
}

#[proc_macro]
pub fn py(input: TokenStream) -> TokenStream {
    literal::expand_py(input.into()).into()
}
