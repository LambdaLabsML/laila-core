//! Shared helpers for dtype-travel round-trip tests across pool backends --
//! the Rust twin of `tests/functional/pools/base/compdata_roundtrip.py`.
//!
//! Used by (rule P19b):
//!     tests/functional/pools/mempool/unit_tests/test_memory_compdata_roundtrip.rs
//!     tests/functional/pools/{filesystem,sqlite,duckdb,hdf5}/unit_tests/test_*_compdata_roundtrip.rs
//!
//! Provides:
//!     - `NUMPY_DTYPES` / `TORCH_DTYPES` (torch list empty: the port has no
//!       torch runtime, as a Python environment without torch)
//!     - `build_np(dtype, shape)`: payload generator
//!     - `envelope_encode(cd)` / `envelope_decode(envelope)`: wrap/unwrap a
//!       serialized `ComputationalData` as a JSON-encodable dict carrying the
//!       transformed blob and its full recovery sequence
//!     - `pool_roundtrip(pool, key, cd, store_as_json_string, chain)`
//!     - [`dtype_matrix!`]: the test-method matrix `register_dtype_matrix`
//!       attaches to a `TestCase` class -- 14 numpy dtypes x 2 modes plus
//!       the numpy edge cases, each as one `#[test]` named like pytest's.

use laila::_compat::ndarray::{NDArray, Scalar};
use laila::entry::compdata::transformation::{Base64, TransformationSequence, Zlib};
use laila::entry::compdata::ComputationalData;
use laila::entry::constitution::SimpleConstitution;
use laila::entry::transformation_base64;
use laila::prelude::*;

// =============================================================================
// Payload generators
// =============================================================================

pub const NUMPY_DTYPES: &[&str] = &[
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float16",
    "float32",
    "float64",
    "bool_",
    "complex64",
    "complex128",
];

/// Torch is an optional dependency of the Python suite; the port has none.
pub const TORCH_DTYPES: &[&str] = &[];

/// `np.linspace(a, b, num=size)` as plain floats.
fn linspace(a: f64, b: f64, size: usize) -> Vec<f64> {
    match size {
        0 => Vec::new(),
        1 => vec![a],
        n => (0..n)
            .map(|i| a + (b - a) * (i as f64) / ((n - 1) as f64))
            .collect(),
    }
}

/// Build a deterministic numpy array covering positive + negative values.
pub fn build_np(dtype_name: &str, shape: &[usize]) -> Result<NDArray> {
    let dt = if dtype_name == "bool_" {
        "bool"
    } else {
        dtype_name
    };
    let size: usize = if shape.is_empty() {
        1
    } else {
        shape.iter().product()
    };
    let kind = match dt {
        "bool" => 'b',
        d if d.starts_with("uint") => 'u',
        d if d.starts_with("int") => 'i',
        d if d.starts_with("float") => 'f',
        d if d.starts_with("complex") => 'c',
        _ => '?',
    };
    let flat: Vec<Scalar> = match kind {
        'b' => (0..size).map(|i| Scalar::Bool(i % 2 == 0)).collect(),
        'u' => (0..size).map(|i| Scalar::UInt(i as u64)).collect(),
        // Mix of negatives and positives, safe for int8
        'i' => (0..size)
            .map(|i| Scalar::Int(i as i64 - (size / 2) as i64))
            .collect(),
        'f' => linspace(-1.0, 1.0, size)
            .into_iter()
            .map(Scalar::Float)
            .collect(),
        'c' => {
            let real = linspace(-1.0, 1.0, size);
            let imag = linspace(0.5, -0.5, size);
            real.into_iter()
                .zip(imag)
                .map(|(r, i)| Scalar::Complex(r, i))
                .collect()
        }
        _ => (0..size).map(|_| Scalar::Int(0)).collect(),
    };
    if shape.is_empty() {
        return NDArray::from_scalars(dt, vec![], false, &flat[..1]);
    }
    NDArray::from_scalars(dt, shape.to_vec(), false, &flat)
}

// =============================================================================
// Envelope: wraps a serialized ComputationalData for storage in a pool
// =============================================================================

/// Serialize *cd* and wrap it in a JSON-encodable dict:
/// `{"blob": <str>, "recovery": [<inverse codes...>, <serializer_backward>]}`.
/// *chain* defaults to the pool-standard Base64 chain so the blob is a
/// UTF-8 string.
pub fn envelope_encode(
    cd: &ComputationalData,
    chain: Option<&TransformationSequence>,
) -> Result<Arg> {
    let (payload, ser_back) = cd.serialize(Args::new())?;
    let default = transformation_base64();
    let ts = chain.unwrap_or(&default);
    let (transformed, inverse_codes) = ts.forward(payload)?;
    let mut recovery: Vec<Arg> = inverse_codes.into_iter().map(Arg::from).collect();
    recovery.push(Arg::from(ser_back));
    Ok(py!({"blob": transformed, "recovery": recovery}))
}

/// Reverse `envelope_encode`; returns the recovered raw payload (not the CD).
pub fn envelope_decode(envelope: &Arg) -> Result<Arg> {
    let codes = envelope.__getitem__("recovery")?;
    let recovered =
        SimpleConstitution::new(args!(codes = codes))?.build(envelope.__getitem__("blob")?)?;
    Ok(match ComputationalData::from_arg(&recovered) {
        Some(cd) => cd.data(),
        None => recovered,
    })
}

// =============================================================================
// Pool round-trip helper
// =============================================================================

/// Put *cd* into *pool* under *key*, read it back, and return the recovered
/// payload. Backends that go through JSON accept a JSON string (or a dict
/// which they json-dump); the in-memory base pool accepts any object --
/// `store_as_json_string` selects the encoding.
pub fn pool_roundtrip(
    pool: &Arg,
    key: &str,
    cd: &ComputationalData,
    store_as_json_string: bool,
    chain: Option<&TransformationSequence>,
) -> Result<Arg> {
    let env = envelope_encode(cd, chain)?;
    if store_as_json_string {
        pool.__setitem__(key, laila::python::stdlib::json.dumps(args!(&env))?)?;
    } else {
        pool.__setitem__(key, env)?;
    }
    let mut got = pool.__getitem__(key)?;
    // Backends that go through JSON re-parse during read, yielding a dict.
    if let Arg::Str(s) = &got {
        got = laila::python::stdlib::json.loads(args!(s.as_ref()))?;
    }
    envelope_decode(&got)
}

// =============================================================================
// Assertion helpers
// =============================================================================

fn as_ndarray(a: &Arg, what: &str) -> NDArray {
    match a {
        Arg::NDArray(n) => (**n).clone(),
        other => panic!("{what}: expected an ndarray, got {}", other.__repr__()),
    }
}

/// numpy dtype / shape / value equality (NaN-aware for floats).
pub fn assert_np_roundtrip(recovered: &Arg, original: &NDArray) {
    let recovered = as_ndarray(recovered, "recovered");
    assert_eq!(
        recovered.dtype().__str__(),
        original.dtype().__str__(),
        "numpy dtype drift: {} -> {}",
        original.dtype().__str__(),
        recovered.dtype().__str__()
    );
    assert_eq!(recovered.shape, original.shape);
    assert!(
        recovered.array_equal(original, true),
        "values differ: {} vs {}",
        recovered.__repr__(),
        original.__repr__()
    );
}

// =============================================================================
// Test bodies (one per registered method shape)
// =============================================================================

/// The chain selected by *mode*: `None` (the Base64 default) for
/// `"raw_b64"`, `Base64 + Zlib` for `"chain_b64_zlib"`.
pub fn chain_for(mode: &str) -> Result<Option<TransformationSequence>> {
    Ok(if mode == "raw_b64" {
        None
    } else {
        Some(TransformationSequence::new(args!(
            transformations = vec![
                Base64::new(args!())?.into_arg(),
                Zlib::new(args!())?.into_arg()
            ]
        ))?)
    })
}

/// `test_pool_np_{dtype}_{mode}`
pub fn run_np_dtype(
    dtype_name: &str,
    mode: &str,
    make_pool: &dyn Fn() -> Result<Arg>,
    store_as_json_string: bool,
) -> Result<()> {
    let arr = build_np(dtype_name, &[2, 3])?;
    let pool = make_pool()?;
    let cd = ComputationalData::new(args!(Arg::NDArray(std::sync::Arc::new(arr.clone()))))?;
    let chain = chain_for(mode)?;
    let recovered = pool_roundtrip(
        &pool,
        &format!("np_{dtype_name}_{mode}"),
        &cd,
        store_as_json_string,
        chain.as_ref(),
    )?;
    assert_np_roundtrip(&recovered, &arr);
    safe_close(&pool);
    Ok(())
}

/// The numpy edge-case arrays by name (`test_pool_np_edge_<name>`).
pub fn edge_array(name: &str) -> Result<NDArray> {
    let f = |dt: &str, shape: Vec<usize>, v: Vec<f64>| NDArray::from_f64s(dt, shape, &v);
    let ints = |dt: &str, shape: Vec<usize>, v: Vec<i64>| {
        let s: Vec<Scalar> = v.into_iter().map(Scalar::Int).collect();
        NDArray::from_scalars(dt, shape, false, &s)
    };
    Ok(match name {
        // np.arange(24, float32).reshape(4, 6)[:, ::2] -- the strided view's values
        "noncontiguous_slice" => f(
            "float32",
            vec![4, 3],
            (0..4)
                .flat_map(|r| (0..3).map(move |c| (r * 6 + c * 2) as f64))
                .collect(),
        )?,
        // np.arange(12, int32).reshape(3, 4).T
        "noncontiguous_transpose" => NDArray::arange(12, "int32")?.reshaped(vec![3, 4])?.T(),
        // np.asfortranarray(np.arange(12, float64).reshape(3, 4))
        "fortran_order" => NDArray::arange(12, "float64")?
            .reshaped(vec![3, 4])?
            .to_f_order(),
        "big_endian_i4" => NDArray::arange(8, "int64")?.astype(">i4")?,
        "little_endian_i4" => NDArray::arange(8, "int64")?.astype("<i4")?,
        "big_endian_f8" => f("float64", vec![6], linspace(-1.0, 1.0, 6))?.astype(">f8")?,
        "nan_inf_float32" => f(
            "float32",
            vec![5],
            vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, 1.0],
        )?,
        "subnormal_float64" => f(
            "float64",
            vec![3],
            vec![f64::MIN_POSITIVE / 2.0, f64::MIN_POSITIVE, 0.0],
        )?,
        "min_max_int64" => ints("int64", vec![3], vec![i64::MIN, 0, i64::MAX])?,
        "zero_d_scalar" => f("float32", vec![], vec![3.5])?,
        "empty_1d" => ints("int32", vec![0], vec![])?,
        "complex_with_imag" => NDArray::from_scalars(
            "complex64",
            vec![3],
            false,
            &[
                Scalar::Complex(1.0, 2.0),
                Scalar::Complex(-3.0, 4.0),
                Scalar::Complex(0.0, -1.0),
            ],
        )?,
        // np.arange(10, int64)[::-1]
        "negative_strides_view" => ints("int64", vec![10], (0..10).rev().collect())?,
        other => {
            return Err(LailaError::value_error(format!(
                "compdata_roundtrip: unknown numpy edge case {other:?}"
            )))
        }
    })
}

/// `test_pool_np_edge_{name}`
pub fn run_np_edge(
    name: &str,
    make_pool: &dyn Fn() -> Result<Arg>,
    store_as_json_string: bool,
) -> Result<()> {
    let arr = edge_array(name)?;
    let pool = make_pool()?;
    let cd = ComputationalData::new(args!(Arg::NDArray(std::sync::Arc::new(arr.clone()))))?;
    let recovered = pool_roundtrip(
        &pool,
        &format!("np_edge_{name}"),
        &cd,
        store_as_json_string,
        None,
    )?;
    assert_np_roundtrip(&recovered, &arr);
    safe_close(&pool);
    Ok(())
}

/// The `skip_unless` flag of a backend that is always available.
pub static ALWAYS_AVAILABLE: std::sync::LazyLock<Arg> =
    std::sync::LazyLock::new(|| Arg::Bool(true));

/// `make_tmpdir(prefix)`: a fresh temporary directory for a file-backed pool.
#[pysig("make_tmpdir(prefix)")]
pub fn make_tmpdir(args: impl Into<Args>) -> Result<Arg> {
    Ok(laila::python::stdlib::tempfile
        .mkdtemp(args!(prefix = prefix))?
        .into_arg())
}

/// Close *pool* if it exposes `close`; swallow errors.
pub fn safe_close(pool: &Arg) {
    if pool.__getattr__("close").is_ok() {
        let _ = pool.__call_method__("close", args!());
    }
}

/// `register_dtype_matrix(cls, make_pool, *, store_as_json_string=True)`:
/// the dense grid of test methods (14 numpy dtypes x 2 modes) plus the
/// numpy edge-case methods, as `#[test]`s named like pytest's. The numpy
/// edge cases with no Rust twin (`structured_dtype`, `subarray_dtype`,
/// `datetime64`: structured / datetime dtypes are not in the ndarray compat
/// layer) are not generated.
#[macro_export]
macro_rules! dtype_matrix {
    ($cls:ident, $make:ident, store_as_json_string = $json:expr $(, include_torch = $torch:expr)? $(,)?) => {
        $crate::dtype_matrix!(
            $cls,
            $make,
            store_as_json_string = $json,
            include_torch = true,
            skip_unless = $crate::compdata_roundtrip::ALWAYS_AVAILABLE
        );
    };
    // `if HAVE_X: register_dtype_matrix(..)` where `HAVE_X` is a runtime
    // probe: every generated test skips when the flag is false.
    ($cls:ident, $make:ident, store_as_json_string = $json:expr, include_torch = $torch:expr, skip_unless = $flag:expr $(,)?) => {
        $crate::__dtype_matrix_np! {
            $make, $json, $flag;
            test_pool_np_int8_raw_b64 => ("int8", "raw_b64"),
            test_pool_np_int8_chain_b64_zlib => ("int8", "chain_b64_zlib"),
            test_pool_np_int16_raw_b64 => ("int16", "raw_b64"),
            test_pool_np_int16_chain_b64_zlib => ("int16", "chain_b64_zlib"),
            test_pool_np_int32_raw_b64 => ("int32", "raw_b64"),
            test_pool_np_int32_chain_b64_zlib => ("int32", "chain_b64_zlib"),
            test_pool_np_int64_raw_b64 => ("int64", "raw_b64"),
            test_pool_np_int64_chain_b64_zlib => ("int64", "chain_b64_zlib"),
            test_pool_np_uint8_raw_b64 => ("uint8", "raw_b64"),
            test_pool_np_uint8_chain_b64_zlib => ("uint8", "chain_b64_zlib"),
            test_pool_np_uint16_raw_b64 => ("uint16", "raw_b64"),
            test_pool_np_uint16_chain_b64_zlib => ("uint16", "chain_b64_zlib"),
            test_pool_np_uint32_raw_b64 => ("uint32", "raw_b64"),
            test_pool_np_uint32_chain_b64_zlib => ("uint32", "chain_b64_zlib"),
            test_pool_np_uint64_raw_b64 => ("uint64", "raw_b64"),
            test_pool_np_uint64_chain_b64_zlib => ("uint64", "chain_b64_zlib"),
            test_pool_np_float16_raw_b64 => ("float16", "raw_b64"),
            test_pool_np_float16_chain_b64_zlib => ("float16", "chain_b64_zlib"),
            test_pool_np_float32_raw_b64 => ("float32", "raw_b64"),
            test_pool_np_float32_chain_b64_zlib => ("float32", "chain_b64_zlib"),
            test_pool_np_float64_raw_b64 => ("float64", "raw_b64"),
            test_pool_np_float64_chain_b64_zlib => ("float64", "chain_b64_zlib"),
            test_pool_np_bool__raw_b64 => ("bool_", "raw_b64"),
            test_pool_np_bool__chain_b64_zlib => ("bool_", "chain_b64_zlib"),
            test_pool_np_complex64_raw_b64 => ("complex64", "raw_b64"),
            test_pool_np_complex64_chain_b64_zlib => ("complex64", "chain_b64_zlib"),
            test_pool_np_complex128_raw_b64 => ("complex128", "raw_b64"),
            test_pool_np_complex128_chain_b64_zlib => ("complex128", "chain_b64_zlib"),
        }
        $crate::__dtype_matrix_edges! {
            $make, $json, $flag;
            test_pool_np_edge_noncontiguous_slice => "noncontiguous_slice",
            test_pool_np_edge_noncontiguous_transpose => "noncontiguous_transpose",
            test_pool_np_edge_fortran_order => "fortran_order",
            test_pool_np_edge_big_endian_i4 => "big_endian_i4",
            test_pool_np_edge_little_endian_i4 => "little_endian_i4",
            test_pool_np_edge_big_endian_f8 => "big_endian_f8",
            test_pool_np_edge_nan_inf_float32 => "nan_inf_float32",
            test_pool_np_edge_subnormal_float64 => "subnormal_float64",
            test_pool_np_edge_min_max_int64 => "min_max_int64",
            test_pool_np_edge_zero_d_scalar => "zero_d_scalar",
            test_pool_np_edge_empty_1d => "empty_1d",
            test_pool_np_edge_complex_with_imag => "complex_with_imag",
            test_pool_np_edge_negative_strides_view => "negative_strides_view",
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __dtype_matrix_np {
    ($make:ident, $json:expr, $flag:expr; $($name:ident => ($dt:literal, $mode:literal)),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                if !::laila::python::builtins::truthy(&*$flag) {
                    ::std::println!("{} ... skipped (backend unavailable)", stringify!($name));
                    return;
                }
                let make = || -> ::laila::Result<::laila::Arg> {
                    ::core::result::Result::Ok(::laila::python::IntoArg::into_arg(::laila::py_try!($make())?))
                };
                if let ::core::result::Result::Err(e) = $crate::compdata_roundtrip::run_np_dtype($dt, $mode, &make, $json) {
                    panic!("{}", ::laila::python::builtins::str(&e));
                }
            }
        )*
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __dtype_matrix_edges {
    ($make:ident, $json:expr, $flag:expr; $($name:ident => $edge:literal),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                if !::laila::python::builtins::truthy(&*$flag) {
                    ::std::println!("{} ... skipped (backend unavailable)", stringify!($name));
                    return;
                }
                let make = || -> ::laila::Result<::laila::Arg> {
                    ::core::result::Result::Ok(::laila::python::IntoArg::into_arg(::laila::py_try!($make())?))
                };
                if let ::core::result::Result::Err(e) = $crate::compdata_roundtrip::run_np_edge($edge, &make, $json) {
                    panic!("{}", ::laila::python::builtins::str(&e));
                }
            }
        )*
    };
}
