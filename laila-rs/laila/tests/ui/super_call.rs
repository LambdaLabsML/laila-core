//! `super().m(..)` is a py2rs lowering (P12d), not a macro form.
#![allow(unused_imports, dead_code)]
use laila::prelude::*;

laila::python! {
    #![unittest]
    py_class!(TestX);
    impl TestX {
        fn setUp(&self) {
            super().setUp();
        }
    }
}

fn main() {}
