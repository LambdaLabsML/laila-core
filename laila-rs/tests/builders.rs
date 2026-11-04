//! Native twins of the constitution *source strings* the Python suites
//! define (`def combine(manifest): ...`).
//!
//! Rust cannot `exec` Python source: a complex constitution's `code` is a
//! *builder token* resolved through
//! `laila::entry::constitution::register_builder_token`. A test file's
//! `builder_tokens:` override (rule P31) maps the module constant holding
//! the source to one of these functions; py2rs registers it under the
//! constant's exact text, so the serialized constitution (`code`, `_hmac`)
//! stays byte-identical to Python's while the body runs natively.

use laila::prelude::*;

/// `tests/functional/entry/constitution/unit_tests/test_constitution_build.py::_CONSTITUTION_SRC`
///
/// ```python
/// def combine(manifest):
///     d = manifest.realized
///     return d['a'].data + d['b'].data
/// ```
pub fn combine(manifest: &Arg) -> Result<Arg> {
    let d = manifest.__getattr__("realized")?;
    let a = d.__getitem__("a")?.__getattr__("data")?;
    let b = d.__getitem__("b")?.__getattr__("data")?;
    a.__add__(&b)
}

/// `tests/functional/entry/edge_cases/unit_tests/test_fidelity_and_trust.py::TestConstitutionTrustGate.test_complex_constitution_round_trips_when_signed`
///
/// ```python
/// def build(m):
///     return m.realized['a'].data * 2
/// ```
pub fn realized_a_times_two(m: &Arg) -> Result<Arg> {
    m.__getattr__("realized")?
        .__getitem__("a")?
        .__getattr__("data")?
        .__mul__(&Arg::Int(2))
}

/// `tests/functional/entry/edge_cases/unit_tests/test_fidelity_and_trust.py::TestManifestFixes.test_manifest_realized_from_named_pool`
///
/// ```python
/// def build(m):
///     return sum(v.data for v in m.realized.values())
/// ```
pub fn sum_realized_data(m: &Arg) -> Result<Arg> {
    let realized = m.__getattr__("realized")?;
    let mut acc: Vec<Arg> = Vec::new();
    for v in realized.values()?.__iter__()? {
        acc.push(v.__getattr__("data")?);
    }
    sum(acc)
}

/// `tests/functional/entry/constitution/unit_tests/test_constitution_build.py::TestConstitutionBuild.test_concurrent_builds_coalesce`
///
/// ```python
/// def build(m):
///     import time
///     time.sleep(0.2)
///     return 42
/// ```
pub fn sleep_then_42(_m: &Arg) -> Result<Arg> {
    std::thread::sleep(std::time::Duration::from_millis(200));
    Ok(Arg::Int(42))
}

/// `tests/functional/policy/memory/manifest/unit_tests/test_manifest_heavy_workloads.py::_WALK_NESTED`
///
/// ```python
/// def walk(manifest):
///     total = 0
///     cur = manifest
///     depth = 0
///     while cur is not None:
///         d = cur.realized
///         total += d['x'].data
///         cur = d.get('inner')
///         depth += 1
///     return (total, depth)
/// ```
pub fn walk_nested(manifest: &Arg) -> Result<Arg> {
    let mut total = Arg::Int(0);
    let mut cur = manifest.clone();
    let mut depth: i64 = 0;
    while !cur.is_none() {
        let d = cur.__getattr__("realized")?;
        total = total.__add__(&d.__getitem__("x")?.__getattr__("data")?)?;
        cur = d.get("inner")?;
        depth += 1;
    }
    Ok(Arg::Tuple(vec![total, Arg::Int(depth)].into()))
}

/// `tests/functional/policy/memory/manifest/unit_tests/test_manifest_heavy_workloads.py::_build_body_for`
///
/// ```python
/// def dep(manifest):
///     import builtins, laila
///     reg = getattr(builtins, '_laila_heavy_manifest_test_registry')
///     prev = reg['e3']
///     if prev.state.name != 'READY':
///         laila.build(prev).wait(120)
///     d = manifest.realized
///     return prev.data + d['x'].data
/// ```
///
/// One twin serves every generated body: the registry name and the key
/// are read back from the source text (`getattr(builtins, '<name>')`,
/// `reg['<key>']`).
pub fn build_body_for(source: &str, manifest: &Arg) -> Result<Arg> {
    fn quoted_after<'a>(text: &'a str, marker: &str) -> Result<&'a str> {
        let start = text.find(marker).ok_or_else(|| {
            LailaError::value_error(format!("constitution source has no `{marker}`"))
        })? + marker.len();
        let rest = &text[start..];
        let end = rest
            .find('\'')
            .ok_or_else(|| LailaError::value_error("unterminated quoted name"))?;
        Ok(&rest[..end])
    }
    let registry_name = quoted_after(source, "getattr(builtins, '")?;
    let key = quoted_after(source, "prev = reg['")?;
    let reg = laila::python::object::PyObject::__getattr__(
        &laila::python::builtins::builtins,
        registry_name,
    )?;
    let prev = reg.__getitem__(key)?;
    if prev.__getattr__("state")?.__getattr__("name")?.__str__() != "READY" {
        laila.build(prev.clone())?.wait(Arg::Int(120))?;
    }
    let d = manifest.__getattr__("realized")?;
    prev.__getattr__("data")?
        .__add__(&d.__getitem__("x")?.__getattr__("data")?)
}

// ---------------------------------------------------------------------------
// tests/deep_eval/test_05_constitution_build_manifest_deep.py
//
// Simple-constitution bodies take the entry's current payload (`x`);
// complex ones take the manifest (`m`).

/// `def f(x): return x * 2`
pub fn x_times_two(x: &Arg) -> Result<Arg> {
    x.__mul__(&Arg::Int(2))
}

/// `def f(x): return [x]`
pub fn x_in_list(x: &Arg) -> Result<Arg> {
    Ok(vec![x.clone()].into_arg())
}

/// `def f(x): return x is None`
pub fn x_is_none(x: &Arg) -> Result<Arg> {
    Ok(Arg::Bool(x.is_none()))
}

/// `def f(x): return None`
pub fn always_none(_x: &Arg) -> Result<Arg> {
    Ok(Arg::None)
}

/// `def f(x): return 1`
pub fn always_one(_x: &Arg) -> Result<Arg> {
    Ok(Arg::Int(1))
}

/// `def f(x): return x + 1` / `def a(x): return x + 1`
pub fn x_plus_one(x: &Arg) -> Result<Arg> {
    x.__add__(&Arg::Int(1))
}

/// `def b(x): return x * 10`
pub fn x_times_ten(x: &Arg) -> Result<Arg> {
    x.__mul__(&Arg::Int(10))
}

/// `def f(x): raise KeyError('k')`
pub fn raise_key_error_k(_x: &Arg) -> Result<Arg> {
    Err(LailaError::key_error("'k'"))
}

/// ```python
/// def f(m):
///     d = m.realized
///     return d['x'].data * d['y'].data
/// ```
pub fn realized_x_times_y(m: &Arg) -> Result<Arg> {
    let d = m.__getattr__("realized")?;
    let x = d.__getitem__("x")?.__getattr__("data")?;
    let y = d.__getitem__("y")?.__getattr__("data")?;
    x.__mul__(&y)
}

/// ```python
/// def f(m):
///     return m.realized['x'].data
/// ```
pub fn realized_x(m: &Arg) -> Result<Arg> {
    m.__getattr__("realized")?
        .__getitem__("x")?
        .__getattr__("data")
}

/// ```python
/// def f(m):
///     return m.realized['x'].data + m.realized['y'].data
/// ```
pub fn realized_x_plus_y(m: &Arg) -> Result<Arg> {
    let d = m.__getattr__("realized")?;
    let x = d.__getitem__("x")?.__getattr__("data")?;
    let y = d.__getitem__("y")?.__getattr__("data")?;
    x.__add__(&y)
}

/// ```python
/// def f(m):
///     raise ValueError('boom')
/// ```
pub fn raise_value_error_boom(_m: &Arg) -> Result<Arg> {
    Err(LailaError::value_error("boom"))
}

/// ```python
/// def f(m):
///     return m.realized['x'].data + 1
/// ```
pub fn realized_x_plus_one(m: &Arg) -> Result<Arg> {
    realized_x(m)?.__add__(&Arg::Int(1))
}

// ---------------------------------------------------------------------------
// tests/deep_eval/test_09_command_futures_taskforce_deep.py

/// `test_cycle_detected_through_laila_build`
///
/// ```python
/// def f(m):
///     import laila
///     return laila.remember('<gid>').wait().data
/// ```
///
/// A body that remembers *itself*: the gid is read back from the source
/// text so the cycle runs through the real resolve chain.
pub fn remember_gid_from_source(source: &str, _manifest: &Arg) -> Result<Arg> {
    let marker = "laila.remember('";
    let start = source
        .find(marker)
        .ok_or_else(|| LailaError::value_error("constitution source has no `laila.remember('`"))?
        + marker.len();
    let rest = &source[start..];
    let end = rest
        .find('\'')
        .ok_or_else(|| LailaError::value_error("unterminated gid"))?;
    let gid = &rest[..end];
    laila.remember(gid)?.wait(Args::new())?.__getattr__("data")
}
