//! Backblaze B2 pool implementation using the S3-compatible API.

use alloc::boxed::Box;
use alloc::string::String;

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::data::boto::boto::{validate_max_pool_connections, BotoPool};
use crate::data::boto::{BotoBackend, BotoVendor, S3Config};
use crate::data::schema::base::{field_default, field_required, pool_class, Pool, PoolObject};
use crate::python::{pysig, Arg, Args, IntoArg, PyArg, Result};

fn opt_str(pool: &PoolObject, name: &str) -> Option<String> {
    match pool.field(name) {
        Some(Arg::Str(s)) => Some(String::from(&*s)),
        _ => None,
    }
}

/// Backblaze B2: the application key pair against the B2 S3 endpoint.
#[derive(Debug, Default)]
pub struct BackblazeVendor;

impl BotoVendor for BackblazeVendor {
    const CLASS_NAME: &'static str = "BackblazePool";

    /// `boto3.client("s3", endpoint_url=self.endpoint_url,
    /// aws_access_key_id=self.application_key_id,
    /// aws_secret_access_key=self.application_key, config=Config(
    /// signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"}))`
    fn client_config(pool: &PoolObject) -> Result<S3Config> {
        Ok(S3Config {
            endpoint_url: opt_str(pool, "endpoint_url"),
            region_name: None,
            access_key_id: opt_str(pool, "application_key_id"),
            secret_access_key: opt_str(pool, "application_key"),
            max_pool_connections: pool
                .field("max_pool_connections")
                .and_then(|v| v.as_i64())
                .unwrap_or(128),
        })
    }

    /// `_no_such_key_codes = {"NoSuchKey", "404"}`
    fn no_such_key_codes() -> &'static [&'static str] {
        &["NoSuchKey", "404"]
    }
}

/// `BackblazePool`'s storage.
pub type BackblazeBackend = BotoBackend<BackblazeVendor>;

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "BackblazePool",
        "laila.data.backblaze.backblaze",
        alloc::vec![BotoPool::__class__()],
        alloc::vec![
            (String::from("application_key_id"), field_required("str")),
            (String::from("application_key"), field_required("str")),
            (
                String::from("endpoint_url"),
                field_default("str", "https://s3.us-west-004.backblazeb2.com")
            ),
        ],
        alloc::vec![],
        alloc::vec![],
    )
});

crate::laila_pool_handle!(
    /// Backblaze B2-backed pool using the S3-compatible API.
    BackblazePool,
    BackblazeBackend
);

impl ModelBase for BackblazePool {
    fn __class__() -> &'static ModelClass {
        BackblazePool::__class__()
    }
}

impl BackblazePool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `BackblazePool(bucket_name, application_key_id, application_key, *, endpoint_url="https://s3.us-west-004.backblazeb2.com", **kwargs)`
    #[pysig("BackblazePool(**kwargs)")]
    pub fn new(args: impl Into<Args>) -> Result<BackblazePool> {
        let pool = Pool::construct(Self::__class__(), kwargs, |_inst: &ModelInstance| {
            Ok(Box::new(BackblazeBackend::default()))
        })?;
        validate_max_pool_connections(pool.obj())?;
        Ok(BackblazePool(pool))
    }
    /// `pool.bucket_name`
    pub fn bucket_name(&self) -> Arg {
        self.0.field("bucket_name").unwrap_or(Arg::None)
    }
    /// `pool.endpoint_url`
    pub fn endpoint_url(&self) -> Arg {
        self.0.field("endpoint_url").unwrap_or(Arg::None)
    }
    /// `pool.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
}

impl crate::python::builtins::CallValue for BackblazePool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        BackblazePool::new(args).map(IntoArg::into_arg)
    }
}

/// `BackblazePool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    BackblazePool::__class__().as_pyclass().into_arg()
}

impl PyArg for BackblazeBackend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
