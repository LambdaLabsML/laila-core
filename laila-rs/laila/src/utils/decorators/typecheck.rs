//! `laila.utils.decorators.typecheck` -- lightweight runtime type coercion.
//!
//! Exports only [`ensure_list`], which removes the boilerplate at the top
//! of every "accept either a single item or an iterable" function. The
//! decorator inspects the wrapped function's signature, finds the named
//! parameter, and -- if its bound value is not a `list` / `set` /
//! `frozenset` -- wraps it in a single-element list before forwarding
//! the call.
//!
//! The typed twin used inside the crate (`Memory::memorize` and friends)
//! is the plain value coercion [`ensure_list_value`]; the decorator form
//! here is what Python-shaped code (`@ensure_list("items")`) reaches.

use alloc::sync::Arc;

use crate::python::object::Function;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, Result};

/// The value coercion of `@ensure_list`: a `list` / `set` / `frozenset`
/// passes through unchanged (identity preserved); anything else --
/// including plain strings -- becomes a single-element list.
pub fn ensure_list_value(value: Arg) -> Arg {
    match value {
        Arg::List(_) | Arg::Set(_) | Arg::FrozenSet(_) => value,
        other => alloc::vec![other].into_arg(),
    }
}

/// `ensure_list(arg_name)`: wrap a single value in a list when *arg_name*
/// is not iterable.
///
/// Returns a decorator. Applied to a function object, the wrapper binds
/// the call to the function's signature (defaults applied), coerces the
/// named parameter and forwards `fn(*bound.args, **bound.kwargs)`. The
/// wrapper keeps the wrapped function's `__name__` (`functools.wraps`).
///
/// `TypeError` at call time if *arg_name* is not bound by the call (the
/// parameter is missing from the function's signature).
#[pysig("ensure_list(arg_name)")]
pub fn ensure_list(args: impl Into<Args>) -> Result<Arg> {
    let arg_name = arg_name.__str__();
    let decorator = move |a: Args| -> Result<Arg> {
        let fn_ = a.positional.into_iter().next().unwrap_or(Arg::None);
        let Some(f) = Function::of(&fn_) else {
            return Err(LailaError::type_error(alloc::format!(
                "{} is not a Python function (no signature to bind)",
                fn_.__repr__()
            )));
        };
        let (sig, name, inner) = (f.sig, f.name.clone(), f.call.clone());
        let arg_name = arg_name.clone();
        let fname = name.clone();
        let wrapper = move |call: Args| -> Result<Arg> {
            let mut bound = sig.bind(call)?;
            bound.apply_defaults(sig);
            let Some(value) = bound.get(sig, &arg_name).cloned() else {
                return Err(LailaError::type_error(alloc::format!(
                    "Argument '{arg_name}' not found in {fname}"
                )));
            };
            bound.set(sig, &arg_name, ensure_list_value(value));
            inner(bound.into_args(sig))
        };
        Ok(Arg::Object(Arc::new(Function {
            name,
            sig,
            call: Arc::new(wrapper),
        })))
    };
    Ok(Arg::Callable(Arc::new(decorator)))
}
