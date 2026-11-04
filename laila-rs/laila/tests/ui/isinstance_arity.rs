//! `isinstance` takes exactly two arguments.
#![allow(unused_imports, dead_code)]
use laila::prelude::*;

laila::python! {
    fn f(x: Arg) -> bool {
        return isinstance(x);
    }
}

fn main() {}
