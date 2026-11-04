//! Hand-written twins of the module-level helpers in `tests/deep_eval`
//! that execute Python source (`exec`) -- see the `skip_functions` /
//! `use:` overrides under `tests/overrides/tests/deep_eval`.

use laila::entry::constitution::constitution::_exec_one_fn;
use laila::prelude::*;

/// `_run_code(code, value)` of `test_04_compdata_transforms_deep.py`:
/// `exec` the recovery code and call its single callable on *value*.
/// The port resolves recovery code natively (`_exec_one_fn` recognises
/// every laila recovery-code template), so the callable is the native
/// `backward` of the transformation that emitted the code.
pub fn _run_code(args: Args) -> Result<Arg> {
    let code = args.positional.first().cloned().unwrap_or(Arg::None);
    let value = args.positional.get(1).cloned().unwrap_or(Arg::None);
    let f = _exec_one_fn(&code.__str__())?;
    f(&value)
}

/// Python-shaped twin of the `_data_transformation` *class object* of
/// `laila.entry.compdata.transformation.base` (the Rust twin is the
/// `Transformation` handle type): `REGISTRY` as the `name -> class` dict,
/// and calling the abstract base raises `TypeError` -- see the
/// `import_twins` override of `test_04_compdata_transforms_deep.py`.
pub mod compdata {
    use laila::entry::compdata::transformation::base::REGISTRY;
    use laila::prelude::*;
    use laila::python::methods::PyDictBuilder;
    use laila::python::pyclass::{ClassHooks, PyClass};
    use std::sync::Arc;

    struct Hooks;
    impl ClassHooks for Hooks {
        fn getattr(&self, name: &str) -> Option<Arg> {
            match name {
                "REGISTRY" => {
                    let mut d = PyDictBuilder::new();
                    for n in REGISTRY() {
                        let cls = match n.as_str() {
                            "msgpack" => "MsgpackSerializer",
                            "pickle" => "PickleSerializer",
                            "numpy" => "NumpySerializer",
                            "torch" => "TorchSerializer",
                            "base64" => "Base64",
                            "zlib" => "Zlib",
                            "json_string" => "JsonString",
                            "fernet" => "FernetEncryption",
                            _ => continue,
                        };
                        let cls = laila::entry::compdata::transformation
                            .__getattr__(cls)
                            .ok()?;
                        d.insert(n.as_str(), cls);
                    }
                    Some(d.finish())
                }
                _ => None,
            }
        }
    }

    /// `_data_transformation` as a class value: `.REGISTRY` reads through
    /// the hooks, calling it raises (abstract base).
    #[allow(non_upper_case_globals)]
    pub static _data_transformation: std::sync::LazyLock<Arg> = std::sync::LazyLock::new(|| {
        PyClass::with_ctor(
            "_data_transformation",
            "laila.entry.compdata.transformation.base",
            Arc::new(|_a: Args| -> Result<Arg> {
                Err(LailaError::type_error(
                    "Can't instantiate abstract class _data_transformation",
                ))
            }),
        )
        .with_hooks(Arc::new(Hooks))
        .into_arg()
    });
}

/// Python-shaped twin of the `Constitution` *class object* of
/// `laila.entry.constitution.constitution` (the Rust twin is an enum whose
/// `from_dict` returns the typed `Option<Constitution>`): `from_dict`
/// returns the subclass instance as a Python value (`None` for `None`), and
/// calling the abstract base raises `TypeError` -- see the `import_twins`
/// override of `test_05_constitution_build_manifest_deep.py`.
pub mod constitution {
    use laila::entry::constitution::constitution::Constitution as Typed;
    use laila::prelude::*;

    /// The `Constitution` class as a path: `Constitution.from_dict(d)` and
    /// `Constitution()` lower to associated functions.
    pub struct Constitution;

    impl Constitution {
        /// `Constitution.from_dict(d)`: the subclass instance as a Python
        /// value (`None` for `None`).
        pub fn from_dict(d: impl IntoArg) -> Result<Arg> {
            Typed::from_dict(d).map(|c| c.into_arg())
        }

        /// `Constitution()`: the abstract base cannot be instantiated.
        #[allow(clippy::new_ret_no_self)]
        pub fn new(_args: impl Into<Args>) -> Result<Arg> {
            Err(LailaError::type_error(
                "Can't instantiate abstract class Constitution with abstract methods _from_dict, as_dict, build",
            ))
        }
    }

    /// `register_builder(scope, sync_fn, async_fn)` with Python callables
    /// (the Rust original takes `&str` and typed builders).
    pub fn register_builder(args: Args) -> Result<Arg> {
        let scope = args.positional.first().cloned().unwrap_or(Arg::None);
        let sync_fn = args.positional.get(1).cloned().unwrap_or(Arg::None);
        let async_fn = args.positional.get(2).cloned().unwrap_or(Arg::None);
        laila::entry::constitution::build_maps::register_builder(
            &scope.__str__(),
            sync_fn,
            async_fn,
        );
        Ok(Arg::None)
    }
}

/// Python-shaped twins of the global-id helpers of
/// `laila.basics.definitions.identifiable_object` (the Rust originals take
/// typed slices / `Option<&str>`; the tests call them with Python values --
/// see the `import_twins` override of `test_02_identity_gid_deep.py`).
pub mod gid {
    use laila::basics::definitions::identifiable_object as io;
    use laila::prelude::*;
    use laila::python::methods::PyDictBuilder;
    use laila::python::stdlib::re;

    fn opt_str(a: &Arg) -> Result<Option<String>> {
        match a {
            Arg::None => Ok(None),
            Arg::Str(s) => Ok(Some(s.to_string())),
            other => Err(LailaError::type_error(format!(
                "expected str or None, got {}",
                other.type_name()
            ))),
        }
    }

    fn pairs_dict(pairs: Vec<(String, String)>) -> Arg {
        let mut d = PyDictBuilder::new();
        for (k, v) in pairs {
            d.insert(k, v);
        }
        d.finish()
    }

    /// `parse_global_id_attributes(attributes) -> dict`
    pub fn parse_global_id_attributes(args: Args) -> Result<Arg> {
        let a = args.positional.first().cloned().unwrap_or(Arg::None);
        let s = opt_str(&a)?;
        Ok(pairs_dict(io::parse_global_id_attributes(s.as_deref())?))
    }

    /// `format_global_id_attributes(mapping) -> str`
    pub fn format_global_id_attributes(args: Args) -> Result<Arg> {
        let a = args.positional.first().cloned().unwrap_or(Arg::None);
        let mut pairs: Vec<(String, Option<String>)> = Vec::new();
        let Arg::Dict(d) = &a else {
            return Err(LailaError::type_error(format!(
                "expected dict, got {}",
                a.type_name()
            )));
        };
        for (k, v) in d.read().iter() {
            let v = match v {
                Arg::None => None,
                other => Some(other.__str__()),
            };
            pairs.push((k.__str__(), v));
        }
        Ok(Arg::Str(io::format_global_id_attributes(&pairs).into()))
    }

    /// `split_global_id_attributes(reference) -> (head, dict)`
    pub fn split_global_id_attributes(args: Args) -> Result<Arg> {
        let a = args.positional.first().cloned().unwrap_or(Arg::None);
        let (head, pairs) = io::split_global_id_attributes(&a.__str__())?;
        Ok(Arg::Tuple(
            vec![Arg::Str(head.into()), pairs_dict(pairs)].into(),
        ))
    }

    /// `GLOBAL_ID_REGEX_PATTERN`: the compiled `re.Pattern` value (a
    /// module constant read through `ModuleValue`, like the lazy tables).
    #[allow(non_snake_case)]
    pub fn GLOBAL_ID_REGEX_PATTERN() -> Arg {
        re.compile(io::GLOBAL_ID_REGEX_PATTERN)
            .expect("GLOBAL_ID_REGEX_PATTERN compiles")
            .into_arg()
    }
}

/// Python-shaped twins of the pool-index key helpers of
/// `laila.data.schema.pool_index` (the Rust originals take `&str` /
/// `Option<i64>`; the tests call them with Python values -- see the
/// `import_twins` override of `test_07_pool_index_deep.py`).
pub mod pool_index {
    use laila::data::schema::pool_index as typed;
    use laila::prelude::*;

    fn evolution_of(a: &Arg) -> Result<Option<i64>> {
        match a {
            Arg::None => Ok(None),
            Arg::Int(i) => Ok(Some(*i)),
            Arg::Bool(b) => Ok(Some(*b as i64)),
            other => Err(LailaError::type_error(format!(
                "'{}' object cannot be interpreted as an integer",
                other.type_name()
            ))),
        }
    }

    /// `_key_evolution(key) -> int | None`
    pub fn _key_evolution(args: Args) -> Result<Arg> {
        let key = args.positional.first().cloned().unwrap_or(Arg::None);
        Ok(typed::_key_evolution(&key.__str__()).into_arg())
    }

    /// `_evolution_key(base, evolution) -> str`
    pub fn _evolution_key(args: Args) -> Result<Arg> {
        let base = args.positional.first().cloned().unwrap_or(Arg::None);
        let evolution = args.positional.get(1).cloned().unwrap_or(Arg::None);
        Ok(Arg::Str(
            typed::_evolution_key(&base.__str__(), evolution_of(&evolution)?).into(),
        ))
    }

    /// `_rank(evolution) -> int`
    pub fn _rank(args: Args) -> Result<Arg> {
        let evolution = args.positional.first().cloned().unwrap_or(Arg::None);
        Ok(Arg::Int(typed::_rank(evolution_of(&evolution)?)))
    }
}

/// Python-shaped twins of the command helpers of
/// `laila.policy.central.command.schema.{exceptions,parking}` and
/// `taskforce.base` (the Rust originals are typed: `&str`, `i64`,
/// `FnOnce`, futures; the tests call them with Python values -- see the
/// `import_twins` override of `test_09_command_futures_taskforce_deep.py`).
pub mod command {
    use laila::policy::central::command::schema::exceptions as exc;
    use laila::policy::central::command::schema::parking;
    use laila::policy::central::command::taskforce::base as tf_base;
    use laila::prelude::*;
    use laila::python::object::Function;
    use laila::python::stdlib::asyncio;

    fn first(args: &Args) -> Arg {
        args.positional.first().cloned().unwrap_or(Arg::None)
    }

    fn ident(a: &Arg) -> Result<i64> {
        a.as_i64().ok_or_else(|| {
            LailaError::type_error(format!(
                "'{}' object cannot be interpreted as an integer",
                a.type_name()
            ))
        })
    }

    /// `@no_command_submit` as a decorator *value* (`#[decorate(..)]`
    /// takes it by value): the wrapped function keeps its `__name__` /
    /// signature (`functools.wraps`).
    #[derive(Clone, Copy, Debug, Default)]
    pub struct NoCommandSubmit;
    #[allow(non_upper_case_globals)]
    pub static no_command_submit: NoCommandSubmit = NoCommandSubmit;

    fn wrap_no_command_submit(args: Args) -> Result<Arg> {
        let f = first(&args);
        let name: String = Function::of(&f)
            .map(|fo| fo.name.to_string())
            .unwrap_or_else(|| "<lambda>".to_string());
        let wrapped = exc::no_command_submit_fn(f.clone(), name.clone());
        Ok(match Function::of(&f) {
            Some(fo) => Function::new(&name, fo.sig, move |a: Args| wrapped.__call__(a)),
            None => wrapped,
        })
    }
    impl IntoArg for NoCommandSubmit {
        fn into_arg(self) -> Arg {
            laila::python::callable(wrap_no_command_submit)
        }
    }
    impl PyArg for NoCommandSubmit {
        fn to_arg(&self) -> Arg {
            laila::python::callable(wrap_no_command_submit)
        }
    }
    impl laila::python::builtins::CallValue for NoCommandSubmit {
        fn call_value(&self, args: Args) -> Result<Arg> {
            wrap_no_command_submit(args)
        }
    }

    pub fn _check_no_pending_submit_owner(_args: Args) -> Result<Arg> {
        exc::_check_no_pending_submit_owner()?;
        Ok(Arg::None)
    }

    pub fn _check_not_loop_thread(_args: Args) -> Result<Arg> {
        exc::_check_not_loop_thread()?;
        Ok(Arg::None)
    }

    pub fn _register_async_loop_thread(args: Args) -> Result<Arg> {
        exc::_register_async_loop_thread(ident(&first(&args))?);
        Ok(Arg::None)
    }

    pub fn _unregister_async_loop_thread(args: Args) -> Result<Arg> {
        exc::_unregister_async_loop_thread(ident(&first(&args))?);
        Ok(Arg::None)
    }

    pub fn ensure_coroutine_function(args: Args) -> Result<Arg> {
        Ok(exc::ensure_coroutine_function(first(&args)))
    }

    fn gids(args: &Args) -> Vec<String> {
        args.positional.iter().map(|a| a.__str__()).collect()
    }

    /// `check_resolve_cycle(*global_ids)`
    pub fn check_resolve_cycle(args: Args) -> Result<Arg> {
        let owned = gids(&args);
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        parking::check_resolve_cycle(&refs)?;
        Ok(Arg::None)
    }

    /// `resolve_chain_with(*global_ids) -> tuple[str, ...]`
    pub fn resolve_chain_with(args: Args) -> Result<Arg> {
        let owned = gids(&args);
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        let chain = parking::resolve_chain_with(&refs);
        Ok(Arg::Tuple(
            chain
                .iter()
                .map(|s| Arg::Str(s.as_str().into()))
                .collect::<Vec<_>>()
                .into(),
        ))
    }

    /// `park_sync(fn, *args, **kwargs)`
    pub fn park_sync(mut args: Args) -> Result<Arg> {
        let f = if args.positional.is_empty() {
            Arg::None
        } else {
            args.positional.remove(0)
        };
        parking::park_sync(|| f.__call__(args))
    }

    /// `park_async(awaitable)` -> a coroutine object.
    pub fn park_async(args: Args) -> Result<Arg> {
        let awaitable = first(&args);
        Ok(asyncio::coroutine(async move {
            parking::park_async(core::future::IntoFuture::into_future(awaitable)).await
        }))
    }

    /// `_live_taskforces_snapshot() -> list[TaskForce]`
    pub fn _live_taskforces_snapshot(_args: Args) -> Result<Arg> {
        Ok(tf_base::_live_taskforces_snapshot().into_arg())
    }
}
