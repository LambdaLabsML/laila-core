//! Nested comprehensions are rejected with a clear message.
#![allow(unused_imports, dead_code)]
use laila::prelude::*;

laila::python! {
    fn f() -> Arg {
        return [y for x in [[1, 2], [3]] for y in x];
    }
}

fn main() {}
