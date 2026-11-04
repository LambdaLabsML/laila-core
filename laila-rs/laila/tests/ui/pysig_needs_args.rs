//! `#[pysig]` requires an `args: impl Into<Args>` parameter to bind.
#![allow(unused_imports, dead_code)]
use laila::prelude::*;

#[laila::pysig("f(a, b=1)")]
fn f() -> Result<Arg> {
    Ok(Arg::None)
}

fn main() {}
