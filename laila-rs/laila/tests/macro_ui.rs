//! Compile-fail cases for `laila::python!` and `#[laila::pysig]`: each
//! `tests/ui/*.rs` must fail with the diagnostic in the `.stderr` beside it
//! (the macro's own messages, so the output is stable across toolchains).
//! Regenerate after a deliberate message change with `TRYBUILD=overwrite`.

#[test]
fn macro_diagnostics() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
