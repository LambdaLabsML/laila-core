//! [`AnyFuture`] -- what the top-level memory verbs (`laila.memorize` /
//! `laila.remember` / `laila.forget`) return: a local [`Future`] (or
//! `GroupFuture`), a `RemoteFuture` when the call was routed to a peer in
//! morph mode, or the plain value a 3-party relay answers with (the list
//! of transferred gids). Python returns those as-is; the typed Rust
//! surface needs one handle that `.wait()`s, `.await`s and converts to the
//! dynamic layer either way.

use alloc::boxed::Box;
use alloc::string::String;

use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, Result};

use super::future::Future;

/// A future-shaped handle from the top-level verbs.
#[derive(Clone)]
pub enum AnyFuture {
    /// A local `Future` / `GroupFuture`.
    Local(Future),
    /// A future living in a peer's bank (morph mode).
    #[cfg(feature = "std")]
    Remote(super::remote_future::RemoteFuture),
    /// Not a future: the value a relay / peer call answered with (gids),
    /// or `None`.
    Value(Arg),
}

impl AnyFuture {
    /// Classify a dynamic value.
    pub fn from_arg(x: Arg) -> AnyFuture {
        match x {
            Arg::Future(f) => AnyFuture::Local(f),
            #[cfg(feature = "std")]
            other => match super::remote_future::RemoteFuture::of(&other) {
                Some(rf) => AnyFuture::Remote(rf),
                None => AnyFuture::Value(other),
            },
            #[cfg(not(feature = "std"))]
            other => AnyFuture::Value(other),
        }
    }
    /// The local future, if that is what this is.
    pub fn local(&self) -> Option<Future> {
        match self {
            AnyFuture::Local(f) => Some(f.clone()),
            _ => None,
        }
    }
    /// The local future, else `TypeError`.
    pub fn into_local(self) -> Result<Future> {
        match self {
            AnyFuture::Local(f) => Ok(f),
            other => Err(LailaError::type_error(alloc::format!(
                "expected a local Future, got {}",
                other.to_arg().type_name()
            ))),
        }
    }
    /// `x is None`
    pub fn is_none(&self) -> bool {
        matches!(self, AnyFuture::Value(Arg::None))
    }
    /// `hasattr(x, "wait")`
    pub fn is_future(&self) -> bool {
        !matches!(self, AnyFuture::Value(_))
    }
    /// `fut.global_id`
    pub fn global_id(&self) -> Result<String> {
        match self {
            AnyFuture::Local(f) => Ok(f.global_id()),
            #[cfg(feature = "std")]
            AnyFuture::Remote(f) => Ok(f.global_id()),
            AnyFuture::Value(v) => v.__getattr__("global_id").map(|g| g.__str__()),
        }
    }
    /// `fut.wait(timeout=None)`; a plain value is returned as-is.
    pub fn wait(&self, args: impl Into<Args>) -> Result<Arg> {
        match self {
            AnyFuture::Local(f) => f.wait(args),
            #[cfg(feature = "std")]
            AnyFuture::Remote(f) => f.wait(args),
            AnyFuture::Value(v) => Ok(v.clone()),
        }
    }
    /// `fut.wait(timeout=secs)`
    pub fn wait_timeout(&self, secs: f64) -> Result<Arg> {
        let mut a = Args::new();
        a.push_keyword("timeout", secs);
        self.wait(a)
    }
    /// `fut.result`
    pub fn result(&self) -> Result<Arg> {
        match self {
            AnyFuture::Local(f) => f.result(),
            #[cfg(feature = "std")]
            AnyFuture::Remote(f) => f.result(),
            AnyFuture::Value(v) => Ok(v.clone()),
        }
    }
    /// `fut.data` (the result's payload; an `Entry` answers its data, a
    /// plain value is itself).
    pub fn data(&self) -> Result<Arg> {
        match self {
            AnyFuture::Local(f) => f.data(),
            #[cfg(feature = "std")]
            AnyFuture::Remote(f) => f.data(),
            AnyFuture::Value(v) => match v.__getattr__("data") {
                Ok(d) => Ok(d),
                Err(_) => Ok(v.clone()),
            },
        }
    }
    /// `fut.status`
    pub fn status(&self) -> Result<Arg> {
        match self {
            AnyFuture::Local(f) => Ok(f.status()),
            #[cfg(feature = "std")]
            AnyFuture::Remote(f) => f.status(),
            AnyFuture::Value(v) => v.__getattr__("status"),
        }
    }
    /// `fut.taskforce_id`
    pub fn taskforce_id(&self) -> Result<Arg> {
        match self {
            AnyFuture::Local(f) => Ok(f.taskforce_id()),
            #[cfg(feature = "std")]
            AnyFuture::Remote(f) => Ok(f.taskforce_id()),
            AnyFuture::Value(v) => v.__getattr__("taskforce_id"),
        }
    }
    /// `fut.policy_id`
    pub fn policy_id(&self) -> Result<Arg> {
        match self {
            AnyFuture::Local(f) => Ok(f.policy_id().into_arg()),
            #[cfg(feature = "std")]
            AnyFuture::Remote(f) => Ok(f.policy_id().into_arg()),
            AnyFuture::Value(v) => v.__getattr__("policy_id"),
        }
    }
    /// `fut.exception`
    pub fn exception(&self) -> Result<Arg> {
        match self {
            AnyFuture::Local(f) => Ok(f.exception()),
            #[cfg(feature = "std")]
            AnyFuture::Remote(f) => f.exception(),
            AnyFuture::Value(v) => v.__getattr__("exception"),
        }
    }
    /// `fut.release()`
    pub fn release(&self, args: impl Into<Args>) -> Result<()> {
        match self {
            AnyFuture::Local(f) => f.release(args),
            #[cfg(feature = "std")]
            AnyFuture::Remote(f) => f.release(),
            AnyFuture::Value(_) => Ok(()),
        }
    }
    /// `fut.<name>`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        self.to_arg().__getattr__(name)
    }
    /// `fut.<name>(..)`
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        self.to_arg().__call_method__(name, args)
    }
    /// Python `is`.
    pub fn is(&self, other: &Arg) -> bool {
        self.to_arg().is(other)
    }
}

impl core::fmt::Debug for AnyFuture {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.to_arg().__repr__())
    }
}
impl PartialEq for AnyFuture {
    fn eq(&self, other: &AnyFuture) -> bool {
        self.to_arg() == other.to_arg()
    }
}
impl IntoArg for AnyFuture {
    fn into_arg(self) -> Arg {
        match self {
            AnyFuture::Local(f) => Arg::Future(f),
            #[cfg(feature = "std")]
            AnyFuture::Remote(f) => f.into_arg(),
            AnyFuture::Value(v) => v,
        }
    }
}
impl PyArg for AnyFuture {
    fn to_arg(&self) -> Arg {
        self.clone().into_arg()
    }
}
impl From<Future> for AnyFuture {
    fn from(f: Future) -> AnyFuture {
        AnyFuture::Local(f)
    }
}
/// `await laila.remember(gid)`.
impl core::future::IntoFuture for AnyFuture {
    type Output = Result<Arg>;
    type IntoFuture = crate::python::stdlib::asyncio::BoxFut;
    fn into_future(self) -> Self::IntoFuture {
        match self {
            AnyFuture::Local(f) => f.__await__(),
            // A remote future's wait is a blocking RPC: park it on a thread.
            #[cfg(feature = "std")]
            AnyFuture::Remote(f) => Box::pin(async move {
                crate::python::stdlib::asyncio::to_thread(move || f.wait(Args::new())).await?
            }),
            AnyFuture::Value(v) => Box::pin(async move { Ok(v) }),
        }
    }
}
impl core::future::IntoFuture for &AnyFuture {
    type Output = Result<Arg>;
    type IntoFuture = crate::python::stdlib::asyncio::BoxFut;
    fn into_future(self) -> Self::IntoFuture {
        self.clone().into_future()
    }
}
