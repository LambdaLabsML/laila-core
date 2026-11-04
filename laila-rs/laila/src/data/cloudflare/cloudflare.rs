//! Cloudflare R2 pool implementation.

use alloc::boxed::Box;
use alloc::string::String;

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::data::boto::boto::{validate_max_pool_connections, BotoPool};
use crate::data::boto::{BotoBackend, BotoVendor, S3Config};
use crate::data::schema::base::{field_required, pool_class, Pool, PoolObject};
use crate::python::{pysig, Arg, Args, IntoArg, PyArg, Result};

fn opt_str(pool: &PoolObject, name: &str) -> Option<String> {
    match pool.field(name) {
        Some(Arg::Str(s)) => Some(String::from(&*s)),
        _ => None,
    }
}

/// The R2 S3-compatible endpoint derived from `account_id`.
fn endpoint_url(pool: &PoolObject) -> String {
    alloc::format!(
        "https://{}.r2.cloudflarestorage.com",
        opt_str(pool, "account_id").unwrap_or_default()
    )
}

/// Cloudflare R2: the account's S3 endpoint with an R2 API token.
#[derive(Debug, Default)]
pub struct CloudflareVendor;

impl BotoVendor for CloudflareVendor {
    const CLASS_NAME: &'static str = "CloudflarePool";

    /// `boto3.client("s3", endpoint_url=self.endpoint_url,
    /// aws_access_key_id=self.access_key_id,
    /// aws_secret_access_key=self.secret_access_key, config=Config(
    /// signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"}))`
    fn client_config(pool: &PoolObject) -> Result<S3Config> {
        Ok(S3Config {
            endpoint_url: Some(endpoint_url(pool)),
            region_name: Some(String::from("auto")),
            access_key_id: opt_str(pool, "access_key_id"),
            secret_access_key: opt_str(pool, "secret_access_key"),
            max_pool_connections: pool
                .field("max_pool_connections")
                .and_then(|v| v.as_i64())
                .unwrap_or(128),
        })
    }

    /// `endpoint_url` -- the property.
    fn getattr(pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        match name {
            "endpoint_url" => Some(Ok(endpoint_url(pool).into_arg())),
            _ => None,
        }
    }
}

/// `CloudflarePool`'s storage.
pub type CloudflareBackend = BotoBackend<CloudflareVendor>;

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "CloudflarePool",
        "laila.data.cloudflare.cloudflare",
        alloc::vec![BotoPool::__class__()],
        alloc::vec![
            (String::from("account_id"), field_required("str")),
            (String::from("access_key_id"), field_required("str")),
            (String::from("secret_access_key"), field_required("str")),
        ],
        alloc::vec![],
        alloc::vec![],
    )
});

crate::laila_pool_handle!(
    /// Cloudflare R2-backed pool. Uploads and downloads key-value data
    /// from R2.
    ///
    /// Uses the S3 API with the R2 endpoint. Objects are keyed by entry
    /// `global_id` directly (no pool directory). Use one bucket per pool.
    CloudflarePool,
    CloudflareBackend
);

impl ModelBase for CloudflarePool {
    fn __class__() -> &'static ModelClass {
        CloudflarePool::__class__()
    }
}

impl CloudflarePool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `CloudflarePool(bucket_name, account_id, access_key_id, secret_access_key, **kwargs)`
    #[pysig("CloudflarePool(**kwargs)")]
    pub fn new(args: impl Into<Args>) -> Result<CloudflarePool> {
        let pool = Pool::construct(Self::__class__(), kwargs, |_inst: &ModelInstance| {
            Ok(Box::new(CloudflareBackend::default()))
        })?;
        validate_max_pool_connections(pool.obj())?;
        Ok(CloudflarePool(pool))
    }
    /// `pool.bucket_name`
    pub fn bucket_name(&self) -> Arg {
        self.0.field("bucket_name").unwrap_or(Arg::None)
    }
    /// `pool.endpoint_url` -- the R2 S3-compatible endpoint derived from
    /// `account_id`.
    pub fn endpoint_url(&self) -> String {
        endpoint_url(self.0.obj())
    }
    /// `pool.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
}

impl crate::python::builtins::CallValue for CloudflarePool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        CloudflarePool::new(args).map(IntoArg::into_arg)
    }
}

/// `CloudflarePool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    CloudflarePool::__class__().as_pyclass().into_arg()
}

impl PyArg for CloudflareBackend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
