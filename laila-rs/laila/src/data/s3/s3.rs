//! AWS S3 pool implementation.
//!
//! Thin specialisation of `BotoPool` that points at the AWS S3 endpoint
//! with AWS-style v4 signatures. The bulk of the read/write/delete logic
//! lives in the parent class -- this file just plugs in the right client
//! configuration (what Python's `boto3.client(...)` factory and the
//! matching `aioboto3.Session` carry).

use alloc::boxed::Box;
use alloc::string::String;

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::data::boto::boto::{validate_max_pool_connections, BotoPool};
use crate::data::boto::{BotoBackend, BotoVendor, S3Config};
use crate::data::schema::base::{field_default, pool_class, Pool, PoolObject};
use crate::python::{pysig, Arg, Args, IntoArg, PyArg, Result};

fn opt_str(pool: &PoolObject, name: &str) -> Option<String> {
    match pool.field(name) {
        Some(Arg::Str(s)) => Some(String::from(&*s)),
        _ => None,
    }
}

/// AWS S3: the standard endpoint, credentials and region from the fields
/// or boto3's provider chain.
#[derive(Debug, Default)]
pub struct S3Vendor;

impl BotoVendor for S3Vendor {
    const CLASS_NAME: &'static str = "S3Pool";

    /// `boto3.client("s3", config=Config(signature_version="s3v4",
    /// retries={"max_attempts": 3, "mode": "standard"},
    /// max_pool_connections=..), aws_access_key_id=.., aws_secret_access_key=..,
    /// region_name=..)` -- each credential kwarg only when the field is set,
    /// so the standard provider chain applies otherwise.
    fn client_config(pool: &PoolObject) -> Result<S3Config> {
        Ok(S3Config {
            endpoint_url: None,
            region_name: opt_str(pool, "region_name"),
            access_key_id: opt_str(pool, "access_key_id"),
            secret_access_key: opt_str(pool, "secret_access_key"),
            max_pool_connections: pool
                .field("max_pool_connections")
                .and_then(|v| v.as_i64())
                .unwrap_or(128),
        })
    }
}

/// `S3Pool`'s storage.
pub type S3Backend = BotoBackend<S3Vendor>;

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "S3Pool",
        "laila.data.s3.s3",
        alloc::vec![BotoPool::__class__()],
        alloc::vec![
            (
                String::from("access_key_id"),
                field_default("str | None", Arg::None)
            ),
            (
                String::from("secret_access_key"),
                field_default("str | None", Arg::None)
            ),
            (
                String::from("region_name"),
                field_default("str | None", Arg::None)
            ),
        ],
        alloc::vec![],
        alloc::vec![],
    )
});

crate::laila_pool_handle!(
    /// AWS S3-backed pool.
    ///
    /// Uploads and downloads serialised entries against a single S3
    /// bucket -- one bucket per pool, objects keyed by entry `global_id`.
    ///
    /// Parameters
    /// ----------
    /// `access_key_id : str, optional` -- AWS access key id. Falls back to
    /// the standard boto3 credential provider chain (env vars,
    /// `~/.aws/credentials`, ...) when omitted.
    /// `secret_access_key : str, optional` -- AWS secret access key.
    /// `region_name : str, optional` -- AWS region for the bucket. When
    /// omitted, picked from the standard configuration sources.
    S3Pool,
    S3Backend
);

impl ModelBase for S3Pool {
    fn __class__() -> &'static ModelClass {
        S3Pool::__class__()
    }
}

impl S3Pool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `S3Pool(bucket_name, *, access_key_id=None, secret_access_key=None, region_name=None, max_req_per_second=None, async_default=True, max_pool_connections=128, **kwargs)`
    #[pysig("S3Pool(**kwargs)")]
    pub fn new(args: impl Into<Args>) -> Result<S3Pool> {
        let pool = Pool::construct(Self::__class__(), kwargs, |_inst: &ModelInstance| {
            Ok(Box::new(S3Backend::default()))
        })?;
        validate_max_pool_connections(pool.obj())?;
        Ok(S3Pool(pool))
    }
    /// `pool.bucket_name`
    pub fn bucket_name(&self) -> Arg {
        self.0.field("bucket_name").unwrap_or(Arg::None)
    }
    /// `pool.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
}

impl crate::python::builtins::CallValue for S3Pool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        S3Pool::new(args).map(IntoArg::into_arg)
    }
}

/// `S3Pool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    S3Pool::__class__().as_pyclass().into_arg()
}

impl PyArg for S3Backend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
