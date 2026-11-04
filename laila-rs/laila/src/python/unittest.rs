//! `unittest.TestCase` assertions with Python's names and failure texts.
//!
//! py2rs turns `class TestX(unittest.TestCase)` into a struct implementing
//! `TestCase`; the `self.assert*` lines stay verbatim.

use alloc::string::String;

use super::args::Args;
use super::builtins::{IsInstance, Truthy};
use super::errors::{ExcMatch, LailaError, Result};
use super::object::{Arg, IntoArg};

fn fail(msg: String) -> LailaError {
    LailaError::new(super::errors::ExcKind::AssertionError, msg)
}

fn with_msg(default: String, msg: Option<Arg>) -> String {
    match msg {
        Some(m) if !m.is_none() => alloc::format!("{default} : {}", m.__str__()),
        _ => default,
    }
}

/// Append the user's `msg` to a failed assertion's standard text.
fn attach_msg(r: Result<()>, msg: impl IntoArg) -> Result<()> {
    match r {
        Err(e) if e.kind == super::errors::ExcKind::AssertionError => {
            Err(fail(with_msg(e.message.clone(), Some(msg.into_arg()))))
        }
        other => other,
    }
}

/// Optional trailing `msg` argument of the `assert*` methods.
pub trait OptMsg {
    fn opt_msg(self) -> Option<Arg>;
}
impl OptMsg for () {
    fn opt_msg(self) -> Option<Arg> {
        None
    }
}
impl<T: IntoArg> OptMsg for (T,) {
    fn opt_msg(self) -> Option<Arg> {
        Some(self.0.into_arg())
    }
}

/// Python's `unittest.TestCase` assertion surface.
#[allow(non_snake_case)]
pub trait TestCase {
    fn assertEqual(&self, a: impl IntoArg, b: impl IntoArg) -> Result<()> {
        let (a, b) = (a.into_arg(), b.into_arg());
        if a == b {
            Ok(())
        } else {
            Err(fail(alloc::format!("{} != {}", a.__repr__(), b.__repr__())))
        }
    }
    fn assertEqual_msg(&self, a: impl IntoArg, b: impl IntoArg, msg: impl IntoArg) -> Result<()> {
        let (a, b) = (a.into_arg(), b.into_arg());
        if a == b {
            Ok(())
        } else {
            Err(fail(with_msg(
                alloc::format!("{} != {}", a.__repr__(), b.__repr__()),
                Some(msg.into_arg()),
            )))
        }
    }
    fn assertNotEqual(&self, a: impl IntoArg, b: impl IntoArg) -> Result<()> {
        let (a, b) = (a.into_arg(), b.into_arg());
        if a != b {
            Ok(())
        } else {
            Err(fail(alloc::format!("{} == {}", a.__repr__(), b.__repr__())))
        }
    }
    fn assertTrue<T: Truthy + ?Sized>(&self, x: &T) -> Result<()> {
        if x.__bool__() {
            Ok(())
        } else {
            Err(fail("expression is not true".into()))
        }
    }
    fn assertFalse<T: Truthy + ?Sized>(&self, x: &T) -> Result<()> {
        if !x.__bool__() {
            Ok(())
        } else {
            Err(fail("expression is not false".into()))
        }
    }
    fn assertIs(&self, a: impl IntoArg, b: impl IntoArg) -> Result<()> {
        let (a, b) = (a.into_arg(), b.into_arg());
        if a.is(&b) {
            Ok(())
        } else {
            Err(fail(alloc::format!(
                "{} is not {}",
                a.__repr__(),
                b.__repr__()
            )))
        }
    }
    fn assertIsNot(&self, a: impl IntoArg, b: impl IntoArg) -> Result<()> {
        let (a, b) = (a.into_arg(), b.into_arg());
        if !a.is(&b) {
            Ok(())
        } else {
            Err(fail(alloc::format!(
                "unexpectedly identical: {}",
                a.__repr__()
            )))
        }
    }
    fn assertIsNone(&self, a: impl IntoArg) -> Result<()> {
        let a = a.into_arg();
        if a.is_none() {
            Ok(())
        } else {
            Err(fail(alloc::format!("{} is not None", a.__repr__())))
        }
    }
    fn assertIsNotNone(&self, a: impl IntoArg) -> Result<()> {
        let a = a.into_arg();
        if !a.is_none() {
            Ok(())
        } else {
            Err(fail("unexpectedly None".into()))
        }
    }
    fn assertIn(&self, member: impl IntoArg, container: impl IntoArg) -> Result<()> {
        let (m, c) = (member.into_arg(), container.into_arg());
        if c.__contains__(m.clone())? {
            Ok(())
        } else {
            Err(fail(alloc::format!(
                "{} not found in {}",
                m.__repr__(),
                c.__repr__()
            )))
        }
    }
    fn assertNotIn(&self, member: impl IntoArg, container: impl IntoArg) -> Result<()> {
        let (m, c) = (member.into_arg(), container.into_arg());
        if !c.__contains__(m.clone())? {
            Ok(())
        } else {
            Err(fail(alloc::format!(
                "{} unexpectedly found in {}",
                m.__repr__(),
                c.__repr__()
            )))
        }
    }
    fn assertIsInstance<T: IsInstance + ?Sized>(&self, x: impl IntoArg) -> Result<()> {
        let x = x.into_arg();
        if T::isinstance(&x) {
            Ok(())
        } else {
            Err(fail(alloc::format!(
                "{} is not an instance of the expected type",
                x.__repr__()
            )))
        }
    }
    fn assertNotIsInstance<T: IsInstance + ?Sized>(&self, x: impl IntoArg) -> Result<()> {
        let x = x.into_arg();
        if !T::isinstance(&x) {
            Ok(())
        } else {
            Err(fail(alloc::format!(
                "{} is an instance of the unexpected type",
                x.__repr__()
            )))
        }
    }
    /// `assertIsInstance(x, cls)` with `cls` a class *value* (`y.__class__`).
    fn assertIsInstance_value(&self, x: impl IntoArg, cls: impl IntoArg) -> Result<()> {
        let (x, cls) = (x.into_arg(), cls.into_arg());
        if super::pyclass::isinstance_value(&x, &cls)? {
            Ok(())
        } else {
            Err(fail(alloc::format!(
                "{} is not an instance of {}",
                x.__repr__(),
                cls.__repr__()
            )))
        }
    }
    /// `assertIsInstance(x, cls, msg)` with `cls` a class *value*.
    fn assertIsInstance_value_msg(
        &self,
        x: impl IntoArg,
        cls: impl IntoArg,
        msg: impl IntoArg,
    ) -> Result<()> {
        let (x, cls) = (x.into_arg(), cls.into_arg());
        if super::pyclass::isinstance_value(&x, &cls)? {
            Ok(())
        } else {
            Err(fail(alloc::format!(
                "{} is not an instance of {} : {}",
                x.__repr__(),
                cls.__repr__(),
                msg.into_arg().__str__()
            )))
        }
    }
    /// `assertNotIsInstance(x, cls, msg)` with `cls` a class *value*.
    fn assertNotIsInstance_value_msg(
        &self,
        x: impl IntoArg,
        cls: impl IntoArg,
        msg: impl IntoArg,
    ) -> Result<()> {
        let (x, cls) = (x.into_arg(), cls.into_arg());
        if !super::pyclass::isinstance_value(&x, &cls)? {
            Ok(())
        } else {
            Err(fail(alloc::format!(
                "{} is an instance of {} : {}",
                x.__repr__(),
                cls.__repr__(),
                msg.into_arg().__str__()
            )))
        }
    }
    /// `assertNotIsInstance(x, cls)` with `cls` a class *value*.
    fn assertNotIsInstance_value(&self, x: impl IntoArg, cls: impl IntoArg) -> Result<()> {
        let (x, cls) = (x.into_arg(), cls.into_arg());
        if !super::pyclass::isinstance_value(&x, &cls)? {
            Ok(())
        } else {
            Err(fail(alloc::format!(
                "{} is an instance of {}",
                x.__repr__(),
                cls.__repr__()
            )))
        }
    }
    fn assertGreater(&self, a: impl IntoArg, b: impl IntoArg) -> Result<()> {
        cmp(
            a,
            b,
            |o| o == core::cmp::Ordering::Greater,
            "not greater than",
        )
    }
    fn assertGreaterEqual(&self, a: impl IntoArg, b: impl IntoArg) -> Result<()> {
        cmp(
            a,
            b,
            |o| o != core::cmp::Ordering::Less,
            "not greater than or equal to",
        )
    }
    fn assertLess(&self, a: impl IntoArg, b: impl IntoArg) -> Result<()> {
        cmp(a, b, |o| o == core::cmp::Ordering::Less, "not less than")
    }
    fn assertLessEqual(&self, a: impl IntoArg, b: impl IntoArg) -> Result<()> {
        cmp(
            a,
            b,
            |o| o != core::cmp::Ordering::Greater,
            "not less than or equal to",
        )
    }
    fn assertAlmostEqual(&self, a: impl IntoArg, b: impl IntoArg) -> Result<()> {
        let (a, b) = (a.into_arg(), b.into_arg());
        match (a.as_f64(), b.as_f64()) {
            (Some(x), Some(y)) if crate::_compat::fmath::round((x - y) * 1e7) == 0.0 => Ok(()),
            _ => Err(fail(alloc::format!(
                "{} != {} within 7 places",
                a.__repr__(),
                b.__repr__()
            ))),
        }
    }
    fn assertCountEqual(&self, a: impl IntoArg, b: impl IntoArg) -> Result<()> {
        let (a, b) = (a.into_arg(), b.into_arg());
        let mut xs = a.__iter__()?;
        let ys = b.__iter__()?;
        let same_len = xs.len() == ys.len();
        let mut ok = same_len;
        if ok {
            for y in &ys {
                match xs.iter().position(|x| x == y) {
                    Some(i) => {
                        xs.remove(i);
                    }
                    None => {
                        ok = false;
                        break;
                    }
                }
            }
        }
        if ok {
            Ok(())
        } else {
            Err(fail(alloc::format!(
                "Element counts were not equal: {} vs {}",
                a.__repr__(),
                b.__repr__()
            )))
        }
    }
    /// `with self.assertRaises(T): body` -- Tier 2 runs `body` as a closure
    /// and hands its `Result` here.
    fn assertRaises<T: ExcMatch + ?Sized>(&self, r: Result<()>) -> Result<LailaError> {
        match r {
            Err(e) if T::matches(&e) => Ok(e),
            Err(e) => Err(e),
            Ok(()) => Err(fail(alloc::format!(
                "{} not raised",
                <T as ExcMatch>::name()
            ))),
        }
    }
    /// `assertRaises(cls)` with *cls* an exception class *value* (a module
    /// constant tuple of classes, a local).
    fn assertRaises_value(&self, cls: impl IntoArg, r: Result<()>) -> Result<LailaError> {
        let cls = cls.into_arg();
        match r {
            Err(e) if super::errors::exception_matches_value(&e, &cls) => Ok(e),
            Err(e) => Err(e),
            Ok(()) => Err(fail(alloc::format!("{} not raised", cls.__repr__()))),
        }
    }
    fn assertRaisesRegex<T: ExcMatch + ?Sized>(
        &self,
        pattern: &str,
        r: Result<()>,
    ) -> Result<LailaError> {
        let e = self.assertRaises::<T>(r)?;
        if e.message.contains(pattern) {
            Ok(e)
        } else {
            Err(fail(alloc::format!(
                "{:?} does not match {:?}",
                pattern,
                e.message
            )))
        }
    }
    /// `self.assertRegex(text, regex)`: `re.search(regex, text)` matches
    /// (`regex` is a pattern string or a compiled `re.Pattern`).
    fn assertRegex(&self, text: impl IntoArg, regex: impl IntoArg) -> Result<()> {
        let (text, regex) = (text.into_arg(), regex.into_arg());
        let p = super::stdlib::re.compile(Args::from(regex.clone()))?;
        if p.search(text.clone())?.is_none() {
            return Err(fail(alloc::format!(
                "Regex didn't match: {} not found in {}",
                p.pattern_repr(),
                text.__repr__()
            )));
        }
        Ok(())
    }
    /// `self.assertNotRegex(text, regex)`
    fn assertNotRegex(&self, text: impl IntoArg, regex: impl IntoArg) -> Result<()> {
        let (text, regex) = (text.into_arg(), regex.into_arg());
        let p = super::stdlib::re.compile(Args::from(regex.clone()))?;
        let m = p.search(text.clone())?;
        if !m.is_none() {
            return Err(fail(alloc::format!(
                "Regex matched: {} matches {} in {}",
                m.__call_method__("group", Args::new())?.__repr__(),
                p.pattern_repr(),
                text.__repr__()
            )));
        }
        Ok(())
    }
    fn fail(&self, msg: impl IntoArg) -> Result<()> {
        Err(fail(msg.into_arg().__str__()))
    }
    // `self.assertX(.., msg)`: the optional trailing message, appended to
    // the standard failure text as `"<standard> : <msg>"`.
    fn assertNotEqual_msg(
        &self,
        a: impl IntoArg,
        b: impl IntoArg,
        msg: impl IntoArg,
    ) -> Result<()> {
        attach_msg(self.assertNotEqual(a, b), msg)
    }
    fn assertIs_msg(&self, a: impl IntoArg, b: impl IntoArg, msg: impl IntoArg) -> Result<()> {
        attach_msg(self.assertIs(a, b), msg)
    }
    fn assertIsNot_msg(&self, a: impl IntoArg, b: impl IntoArg, msg: impl IntoArg) -> Result<()> {
        attach_msg(self.assertIsNot(a, b), msg)
    }
    fn assertIn_msg(&self, a: impl IntoArg, b: impl IntoArg, msg: impl IntoArg) -> Result<()> {
        attach_msg(self.assertIn(a, b), msg)
    }
    fn assertNotIn_msg(&self, a: impl IntoArg, b: impl IntoArg, msg: impl IntoArg) -> Result<()> {
        attach_msg(self.assertNotIn(a, b), msg)
    }
    fn assertGreater_msg(&self, a: impl IntoArg, b: impl IntoArg, msg: impl IntoArg) -> Result<()> {
        attach_msg(self.assertGreater(a, b), msg)
    }
    fn assertGreaterEqual_msg(
        &self,
        a: impl IntoArg,
        b: impl IntoArg,
        msg: impl IntoArg,
    ) -> Result<()> {
        attach_msg(self.assertGreaterEqual(a, b), msg)
    }
    fn assertLess_msg(&self, a: impl IntoArg, b: impl IntoArg, msg: impl IntoArg) -> Result<()> {
        attach_msg(self.assertLess(a, b), msg)
    }
    fn assertLessEqual_msg(
        &self,
        a: impl IntoArg,
        b: impl IntoArg,
        msg: impl IntoArg,
    ) -> Result<()> {
        attach_msg(self.assertLessEqual(a, b), msg)
    }
    fn assertAlmostEqual_msg(
        &self,
        a: impl IntoArg,
        b: impl IntoArg,
        msg: impl IntoArg,
    ) -> Result<()> {
        attach_msg(self.assertAlmostEqual(a, b), msg)
    }
    fn assertCountEqual_msg(
        &self,
        a: impl IntoArg,
        b: impl IntoArg,
        msg: impl IntoArg,
    ) -> Result<()> {
        attach_msg(self.assertCountEqual(a, b), msg)
    }
    fn assertTrue_msg(&self, x: impl IntoArg, msg: impl IntoArg) -> Result<()> {
        attach_msg(self.assertTrue(&x.into_arg()), msg)
    }
    fn assertFalse_msg(&self, x: impl IntoArg, msg: impl IntoArg) -> Result<()> {
        attach_msg(self.assertFalse(&x.into_arg()), msg)
    }
    fn assertIsNone_msg(&self, x: impl IntoArg, msg: impl IntoArg) -> Result<()> {
        attach_msg(self.assertIsNone(x), msg)
    }
    fn assertIsNotNone_msg(&self, x: impl IntoArg, msg: impl IntoArg) -> Result<()> {
        attach_msg(self.assertIsNotNone(x), msg)
    }
    fn skipTest(&self, reason: impl IntoArg) -> Result<()> {
        Err(LailaError::new(
            super::errors::ExcKind::Exception,
            alloc::format!("SKIP: {}", reason.into_arg().__str__()),
        ))
    }
    /// `self.addCleanup(f, *args, **kwargs)`: *f* runs after `tearDown`
    /// (LIFO), whether the test passed or not.
    fn addCleanup(&self, args: impl Into<super::Args>) -> Result<()> {
        let mut a: super::Args = args.into();
        if a.positional.is_empty() {
            return Err(LailaError::type_error(
                "addCleanup() missing 1 required positional argument: 'function'",
            ));
        }
        let f = a.positional.remove(0);
        CLEANUPS.lock().push((f, a));
        Ok(())
    }
    /// `with self.subTest(**params):` -- the block runs inline; the
    /// parameters only label a failure.
    fn subTest(&self, params: impl Into<super::Args>) -> SubTest {
        SubTest {
            params: params.into(),
        }
    }
}

/// The context manager of [`TestCase::subTest`].
pub struct SubTest {
    pub params: super::Args,
}
impl SubTest {
    pub fn __enter__(&self) -> Result<SubTestGuard> {
        Ok(SubTestGuard { _private: () })
    }
}
/// The guard of a `with self.subTest(..):` block (nothing to release).
pub struct SubTestGuard {
    _private: (),
}

/// A test case handed around as a value (`def check(case: unittest.TestCase)`
/// called with `self`): the assertions are the trait's defaults, so any
/// `Arg` can stand in for the case.
impl TestCase for Arg {}

/// `raise unittest.SkipTest(reason)` from a test body (`@unittest.skipUnless`).
pub fn skip_test(reason: impl IntoArg) -> Result<()> {
    Err(LailaError::new(
        super::errors::ExcKind::Exception,
        alloc::format!("SKIP: {}", reason.into_arg().__str__()),
    ))
}

/// Serialises the `#[test]` wrappers of one binary (Python runs tests sequentially).
#[cfg(feature = "std")]
pub fn test_lock() -> parking_lot::MutexGuard<'static, ()> {
    static LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    LOCK.lock()
}

/// `setUpClass` / `tearDownClass` bookkeeping of one `TestCase` class.
///
/// Python runs `setUpClass` once before the class's first test and
/// `tearDownClass` once after its last. Each generated `#[test]` is its
/// own entry point, so the class fixture is a process-wide record per
/// class: the first test to run calls `setUpClass`; the test that brings
/// the remaining count to zero (every test of the class ran) calls
/// `tearDownClass`; a filtered run (`cargo test name`) that never reaches
/// zero tears down at process exit, as the interpreter would.
#[cfg(feature = "std")]
#[derive(Default)]
pub struct ClassFixture {
    set_up: bool,
    remaining: usize,
    torn_down: bool,
    teardown: Option<alloc::boxed::Box<dyn FnOnce() -> Result<()> + Send>>,
}

#[cfg(feature = "std")]
type SharedFixture = alloc::sync::Arc<parking_lot::Mutex<ClassFixture>>;

#[cfg(feature = "std")]
static CLASS_FIXTURES: parking_lot::Mutex<alloc::vec::Vec<(&'static str, SharedFixture)>> =
    parking_lot::Mutex::new(alloc::vec::Vec::new());

#[cfg(feature = "std")]
fn class_fixture(class: &'static str) -> alloc::sync::Arc<parking_lot::Mutex<ClassFixture>> {
    let mut all = CLASS_FIXTURES.lock();
    if let Some((_, f)) = all.iter().find(|(c, _)| *c == class) {
        return f.clone();
    }
    let f = alloc::sync::Arc::new(parking_lot::Mutex::new(ClassFixture::default()));
    all.push((class, f.clone()));
    f
}

/// Before a test of *class* (which has *total* tests): `setUpClass()` on
/// the first call; registers `tearDownClass()` for the end of the run.
#[cfg(feature = "std")]
pub fn class_set_up(
    class: &'static str,
    total: usize,
    set_up: impl FnOnce() -> Result<()>,
    tear_down: impl FnOnce() -> Result<()> + Send + 'static,
) -> Result<()> {
    let fixture = class_fixture(class);
    let mut f = fixture.lock();
    if f.set_up {
        return Ok(());
    }
    f.set_up = true;
    f.remaining = total;
    f.teardown = Some(alloc::boxed::Box::new(tear_down));
    let at_exit = fixture.clone();
    super::stdlib::subprocess::atexit::register(move || {
        let _ = run_class_tear_down(&at_exit);
    });
    drop(f);
    set_up()
}

#[cfg(feature = "std")]
fn run_class_tear_down(fixture: &parking_lot::Mutex<ClassFixture>) -> Result<()> {
    let teardown = {
        let mut f = fixture.lock();
        if f.torn_down {
            return Ok(());
        }
        f.torn_down = true;
        f.teardown.take()
    };
    match teardown {
        Some(t) => t(),
        None => Ok(()),
    }
}

/// After a test of *class*: `tearDownClass()` once every test has run.
#[cfg(feature = "std")]
pub fn class_test_done(class: &'static str) -> Result<()> {
    let fixture = class_fixture(class);
    let last = {
        let mut f = fixture.lock();
        f.remaining = f.remaining.saturating_sub(1);
        f.set_up && f.remaining == 0
    };
    if last {
        run_class_tear_down(&fixture)
    } else {
        Ok(())
    }
}

/// The cleanups registered by `addCleanup` during the running test (tests
/// run one at a time under [`test_lock`]).
static CLEANUPS: super::sync::Mutex<alloc::vec::Vec<(Arg, super::Args)>> =
    super::sync::Mutex::new(alloc::vec::Vec::new());

/// `doCleanups()`: run the registered cleanups, last registered first.
pub fn do_cleanups() -> Result<()> {
    let mut first_err = None;
    loop {
        let next = CLEANUPS.lock().pop();
        let Some((f, a)) = next else { break };
        if let Err(e) = super::builtins::call(&f, a) {
            first_err.get_or_insert(e);
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Runs one test body; `AssertionError`/exceptions fail with Python's text,
/// `skipTest` passes with a notice.
#[cfg(feature = "std")]
pub fn run_test(qualname: &str, body: impl FnOnce() -> Result<()>) {
    let outcome = body();
    let cleanups = do_cleanups();
    match outcome.and(cleanups) {
        Ok(()) => {}
        Err(e)
            if e.kind == super::errors::ExcKind::Exception && e.message.starts_with("SKIP: ") =>
        {
            std::println!("{qualname} ... skipped {:?}", &e.message[6..]);
        }
        Err(e) => panic!("{qualname}\n{e}"),
    }
}

/// `pytest.raises(T)` / `pytest.raises(T, match=...)` as a block check.
pub mod pytest {
    use super::*;
    use crate::python::object::PyObject;
    #[allow(unused_imports)]
    use crate::python::{builtins, object, stdlib};
    pub fn raises<T: ExcMatch + ?Sized>(r: Result<()>) -> Result<LailaError> {
        match r {
            Err(e) if T::matches(&e) => Ok(e),
            Err(e) => Err(e),
            Ok(()) => Err(super::fail(alloc::format!(
                "DID NOT RAISE {}",
                <T as ExcMatch>::name()
            ))),
        }
    }
    pub fn raises_match<T: ExcMatch + ?Sized>(
        pattern: impl IntoArg,
        r: Result<()>,
    ) -> Result<LailaError> {
        let e = raises::<T>(r)?;
        let p = pattern.into_arg().__str__();
        if e.message.contains(&p) {
            Ok(e)
        } else {
            Err(super::fail(alloc::format!(
                "Regex pattern did not match.\n Regex: {p:?}\n Input: {:?}",
                e.message
            )))
        }
    }

    /// `pytest.fail(msg)`: the test fails with *msg* (an `AssertionError`,
    /// as `Failed` derives from it for every handler the ports use).
    pub fn fail(msg: impl IntoArg) -> Result<()> {
        Err(super::fail(msg.into_arg().__str__()))
    }

    /// `pytest.skip(reason)`.
    pub fn skip(reason: impl IntoArg) -> Result<()> {
        skip_test(reason)
    }

    /// `pytest.approx(expected, rel=1e-6, abs=1e-12)`: a value that
    /// compares equal to any number within the tolerance.
    pub fn approx(args: impl Into<Args>) -> Approx {
        let a: Args = args.into();
        let expected = a.positional.first().cloned().unwrap_or(Arg::None);
        let rel = a.get("rel");
        let abs = a.get("abs");
        Approx {
            expected,
            rel: rel.as_f64().unwrap_or(1e-6),
            abs: abs.as_f64().unwrap_or(1e-12),
        }
    }

    /// The `tmp_path` fixture: a fresh, empty directory per call.
    #[cfg(feature = "std")]
    pub fn tmp_path() -> Result<stdlib::Path> {
        let mut a = Args::new();
        a.push_keyword("prefix", "pytest-");
        let dir = stdlib::tempfile.mkdtemp(a)?;
        Ok(stdlib::Path::from_text(&dir))
    }

    /// The `monkeypatch` fixture.
    #[cfg(feature = "std")]
    pub fn monkeypatch() -> MonkeyPatch {
        MonkeyPatch::default()
    }

    /// Runs fixture teardowns last-registered-first; the first error wins
    /// but every teardown still runs.
    pub fn run_teardowns(teardowns: impl IntoArg) -> Result<()> {
        let mut first_err = None;
        let tds: alloc::vec::Vec<Arg> = teardowns.into_arg().__iter__()?.into_iter().collect();
        for td in tds.into_iter().rev() {
            if let Err(e) = builtins::call(&td, Args::default()) {
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// The `pytest` module as a *value* (`pytest.fail(..)`, `pytest.approx(..)`,
    /// the fixture constructors); `with pytest.raises(..)` is lowered by the
    /// macro to the functions above.
    #[allow(non_camel_case_types)]
    #[derive(Clone, Copy, Debug, Default)]
    pub struct pytest_mod;

    impl pytest_mod {
        pub fn fail(&self, msg: impl IntoArg) -> Result<()> {
            fail(msg)
        }
        pub fn skip(&self, reason: impl IntoArg) -> Result<()> {
            skip(reason)
        }
        pub fn approx(&self, args: impl Into<Args>) -> Approx {
            approx(args)
        }
        #[cfg(feature = "std")]
        pub fn tmp_path(&self) -> Result<stdlib::Path> {
            tmp_path()
        }
        #[cfg(feature = "std")]
        pub fn monkeypatch(&self) -> MonkeyPatch {
            monkeypatch()
        }
        pub fn run_teardowns(&self, teardowns: impl IntoArg) -> Result<()> {
            run_teardowns(teardowns)
        }
    }

    /// `pytest.approx(..)`.
    #[derive(Clone, Debug)]
    pub struct Approx {
        pub expected: Arg,
        pub rel: f64,
        pub abs: f64,
    }

    impl Approx {
        fn close(&self, actual: f64, expected: f64) -> bool {
            if actual == expected {
                return true;
            }
            let tol = (self.rel * expected.abs()).max(self.abs);
            (actual - expected).abs() <= tol
        }
    }

    impl PyObject for Approx {
        fn as_any(&self) -> &dyn core::any::Any {
            self
        }
        fn type_name(&self) -> &'static str {
            "ApproxScalar"
        }
        fn __repr__(&self) -> String {
            alloc::format!(
                "{} ± {:.1e}",
                self.expected.__repr__(),
                (self.rel * self.expected.as_f64().unwrap_or(0.0).abs()).max(self.abs)
            )
        }
        fn __eq__(&self, other: &dyn PyObject) -> bool {
            other
                .as_any()
                .downcast_ref::<Approx>()
                .map(|o| o.expected == self.expected)
                .unwrap_or(false)
        }
        fn __eq_arg__(&self, other: &Arg) -> bool {
            match (self.expected.as_f64(), other.as_f64()) {
                (Some(e), Some(a)) => self.close(a, e),
                _ => match other {
                    Arg::Object(o) => self.__eq__(&**o),
                    _ => false,
                },
            }
        }
        fn __hash__(&self) -> Result<i64> {
            Err(LailaError::type_error("unhashable type: 'ApproxScalar'"))
        }
    }

    impl IntoArg for Approx {
        fn into_arg(self) -> Arg {
            Arg::Object(alloc::sync::Arc::new(self))
        }
    }
    impl PartialEq<Approx> for Arg {
        fn eq(&self, other: &Approx) -> bool {
            other.__eq_arg__(self)
        }
    }
    impl PartialEq<Approx> for f64 {
        fn eq(&self, other: &Approx) -> bool {
            other.__eq_arg__(&Arg::Float(*self))
        }
    }
    impl PartialEq<Approx> for i64 {
        fn eq(&self, other: &Approx) -> bool {
            other.__eq_arg__(&Arg::Int(*self))
        }
    }

    /// The `monkeypatch` fixture: `setitem`, `setenv`, `delenv`,
    /// `setattr` (`"sys.argv"` and `(obj, name, value)`), undone by
    /// One recorded undo step of a [`MonkeyPatch`].
    #[cfg(feature = "std")]
    type UndoStep = alloc::boxed::Box<dyn FnOnce() -> Result<()> + Send>;

    /// `undo()` in reverse order.
    #[cfg(feature = "std")]
    #[derive(Clone, Default)]
    pub struct MonkeyPatch {
        undo: alloc::sync::Arc<parking_lot::Mutex<alloc::vec::Vec<UndoStep>>>,
    }

    #[cfg(feature = "std")]
    impl core::fmt::Debug for MonkeyPatch {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.write_str("MonkeyPatch")
        }
    }

    #[cfg(feature = "std")]
    impl MonkeyPatch {
        fn push(&self, f: impl FnOnce() -> Result<()> + Send + 'static) {
            self.undo.lock().push(alloc::boxed::Box::new(f));
        }

        /// `monkeypatch.setitem(mapping, name, value)`.
        pub fn setitem(&self, args: impl Into<Args>) -> Result<()> {
            let a: Args = args.into();
            let mut it = a.positional.into_iter();
            let (mapping, key, value) = (
                it.next().unwrap_or(Arg::None),
                it.next().unwrap_or(Arg::None),
                it.next().unwrap_or(Arg::None),
            );
            let old = if mapping.__contains__(key.clone())? {
                Some(mapping.__getitem__(key.clone())?)
            } else {
                None
            };
            mapping.__setitem__(key.clone(), value)?;
            self.push(move || match old {
                Some(v) => mapping.__setitem__(key, v),
                None => mapping.__delitem__(key),
            });
            Ok(())
        }

        /// `monkeypatch.setenv(name, value)`.
        pub fn setenv(&self, args: impl Into<Args>) -> Result<()> {
            let a: Args = args.into();
            let name = a.positional.first().cloned().unwrap_or(Arg::None).__str__();
            let value = a.positional.get(1).cloned().unwrap_or(Arg::None).__str__();
            let old = std::env::var_os(&name);
            std::env::set_var(&name, value);
            self.push(move || {
                match old {
                    Some(v) => std::env::set_var(&name, v),
                    None => std::env::remove_var(&name),
                }
                Ok(())
            });
            Ok(())
        }

        /// `monkeypatch.delenv(name, raising=True)`.
        pub fn delenv(&self, args: impl Into<Args>) -> Result<()> {
            let a: Args = args.into();
            let name = a.positional.first().cloned().unwrap_or(Arg::None).__str__();
            let raising = a.get("raising");
            let old = std::env::var_os(&name);
            if old.is_none() {
                if raising.is_none() || raising.__bool__() {
                    return Err(LailaError::key_error(alloc::format!("{name:?}")));
                }
                return Ok(());
            }
            std::env::remove_var(&name);
            self.push(move || {
                if let Some(v) = old {
                    std::env::set_var(&name, v);
                }
                Ok(())
            });
            Ok(())
        }

        /// `monkeypatch.setattr("sys.argv", value)` or
        /// `monkeypatch.setattr(obj, name, value)`.
        pub fn setattr(&self, args: impl Into<Args>) -> Result<()> {
            let a: Args = args.into();
            let first = a.positional.first().cloned().unwrap_or(Arg::None);
            if let Arg::Str(target) = &first {
                let value = a.positional.get(1).cloned().unwrap_or(Arg::None);
                if &**target == "sys.argv" {
                    let argv: alloc::vec::Vec<String> =
                        value.__iter__()?.into_iter().map(|v| v.__str__()).collect();
                    let old = stdlib::set_sys_argv(Some(argv));
                    self.push(move || {
                        stdlib::set_sys_argv(old);
                        Ok(())
                    });
                    return Ok(());
                }
                return Err(LailaError::not_implemented(alloc::format!(
                    "monkeypatch.setattr({target:?}, ..): only \"sys.argv\" is supported by the Rust port"
                )));
            }
            let name = a.positional.get(1).cloned().unwrap_or(Arg::None).__str__();
            let value = a.positional.get(2).cloned().unwrap_or(Arg::None);
            let old = first.__getattr__(&name)?;
            first.__setattr__(&name, value)?;
            self.push(move || first.__setattr__(&name, old));
            Ok(())
        }

        /// `monkeypatch.undo()`: revert every change, last first.
        pub fn undo(&self) -> Result<()> {
            let mut first_err = None;
            loop {
                let next = self.undo.lock().pop();
                let Some(f) = next else { break };
                if let Err(e) = f() {
                    first_err.get_or_insert(e);
                }
            }
            match first_err {
                Some(e) => Err(e),
                None => Ok(()),
            }
        }
    }

    #[cfg(feature = "std")]
    impl PyObject for MonkeyPatch {
        fn as_any(&self) -> &dyn core::any::Any {
            self
        }
        fn type_name(&self) -> &'static str {
            "MonkeyPatch"
        }
        fn __repr__(&self) -> String {
            String::from("<MonkeyPatch>")
        }
        fn __eq__(&self, other: &dyn PyObject) -> bool {
            other
                .as_any()
                .downcast_ref::<MonkeyPatch>()
                .map(|o| alloc::sync::Arc::ptr_eq(&o.undo, &self.undo))
                .unwrap_or(false)
        }
        fn __hash__(&self) -> Result<i64> {
            Ok(alloc::sync::Arc::as_ptr(&self.undo) as usize as i64)
        }
        fn __getattr__(&self, name: &str) -> Result<Arg> {
            let me = self.clone();
            Ok(match name {
                "undo" => object::callable(move |_a: Args| me.undo()),
                "setitem" => object::callable(move |a: Args| me.setitem(a)),
                "setenv" => object::callable(move |a: Args| me.setenv(a)),
                "delenv" => object::callable(move |a: Args| me.delenv(a)),
                "setattr" => object::callable(move |a: Args| me.setattr(a)),
                _ => {
                    return Err(LailaError::attribute_error(alloc::format!(
                        "'MonkeyPatch' object has no attribute '{name}'"
                    )))
                }
            })
        }
        fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
            match name {
                "undo" => self.undo().map(|_| Arg::None),
                "setitem" => self.setitem(args).map(|_| Arg::None),
                "setenv" => self.setenv(args).map(|_| Arg::None),
                "delenv" => self.delenv(args).map(|_| Arg::None),
                "setattr" => self.setattr(args).map(|_| Arg::None),
                _ => Err(LailaError::attribute_error(alloc::format!(
                    "'MonkeyPatch' object has no attribute '{name}'"
                ))),
            }
        }
    }

    #[cfg(feature = "std")]
    impl IntoArg for MonkeyPatch {
        fn into_arg(self) -> Arg {
            Arg::Object(alloc::sync::Arc::new(self))
        }
    }
}

/// `self.assertX(.., msg=m)`: unittest's `longMessage` formatting --
/// `"<standard message> : <msg>"` -- applied to a failed assertion.
pub fn assert_with_msg<T>(r: Result<T>, msg: impl IntoArg) -> Result<T> {
    match r {
        Err(e) if e.kind == super::errors::ExcKind::AssertionError => {
            let m = msg.into_arg();
            if m.is_none() {
                return Err(e);
            }
            Err(LailaError::new(
                super::errors::ExcKind::AssertionError,
                alloc::format!("{} : {}", e.message, m.__str__()),
            ))
        }
        other => other,
    }
}

/// `unittest.mock.patch.object(..)` (`patch` the name is also the module
/// holding `object`, so `patch.object(Cls, "attr", ..)` reads as in Python).
pub mod patch {
    pub use crate::python::mock::object;
}

/// `unittest.mock.patch(target, new=DEFAULT, **kwargs)` as a context
/// manager. `"laila.args"` swaps the process-wide args object; any other
/// dotted target (`"os.path.exists"`, `"laila.data.mongo.mongo.MongoClient"`)
/// installs a mock in the [`mock`](super::mock) registry under
/// `(module path, attribute)`, which the port's twin of that module
/// attribute consults on each use.
pub fn patch(args: impl Into<super::args::Args>) -> Result<Patch> {
    let a: super::args::Args = args.into();
    let target = a
        .positional
        .first()
        .cloned()
        .unwrap_or_else(|| a.get("target"));
    let target = match target {
        Arg::Str(s) => s,
        other => {
            return Err(LailaError::type_error(alloc::format!(
                "Need a valid target to patch. You supplied: {}",
                other.__repr__()
            )))
        }
    };
    if &*target == "laila.args" {
        let new = a.positional.get(1).cloned().unwrap_or_else(|| a.get("new"));
        return Ok(Patch::Args { target, new });
    }
    let Some((module, attribute)) = target.rsplit_once('.') else {
        return Err(LailaError::type_error(alloc::format!(
            "Need a valid target to patch. You supplied: {}",
            Arg::Str(target.clone()).__repr__()
        )));
    };
    let mut object_args = super::args::Args::new();
    object_args.push_positional(super::mock::module_target(module));
    object_args.push_positional(attribute);
    for v in a.positional.iter().skip(1) {
        object_args.push_positional(v.clone());
    }
    for (k, v) in &a.keywords {
        if &**k != "target" {
            object_args.push_keyword(k, v.clone());
        }
    }
    Ok(Patch::Attribute(super::mock::object(object_args)?))
}

/// The object `patch(..)` returns; `__enter__` swaps the target in and the
/// guard restores it on drop (`__exit__`).
pub enum Patch {
    /// `patch("laila.args", new)`
    Args {
        target: alloc::sync::Arc<str>,
        new: Arg,
    },
    /// `patch("module.attr", ..)`: a registry patch (see [`mock::object`](super::mock::object)).
    Attribute(super::mock::ObjectPatch),
}

impl Patch {
    pub fn __enter__(&self) -> Result<PatchGuard> {
        match self {
            Patch::Args { target, new } => {
                let old = crate::basics::definitions::cli_capable::_replace_args(new.clone());
                Ok(PatchGuard::Args {
                    target: target.clone(),
                    old,
                })
            }
            Patch::Attribute(p) => Ok(PatchGuard::Attribute(p.__enter__()?)),
        }
    }
    /// `patch.start()` -- apply without a guard (`stop()` restores).
    pub fn start(&self) -> Result<Arg> {
        match self {
            Patch::Args { new, .. } => {
                let _ = crate::basics::definitions::cli_capable::_replace_args(new.clone());
                Ok(new.clone())
            }
            Patch::Attribute(p) => p.start(super::args::Args::new()),
        }
    }
    /// `patch.stop()`
    pub fn stop(&self) -> Result<()> {
        match self {
            Patch::Args { .. } => Ok(()),
            Patch::Attribute(p) => p.stop(super::args::Args::new()),
        }
    }
}

/// Restores the patched attribute when dropped.
pub enum PatchGuard {
    Args {
        #[allow(dead_code)]
        target: alloc::sync::Arc<str>,
        old: Arg,
    },
    Attribute(super::mock::ObjectPatchGuard),
}

impl Drop for PatchGuard {
    fn drop(&mut self) {
        if let PatchGuard::Args { old, .. } = self {
            let _ = crate::basics::definitions::cli_capable::_replace_args(old.clone());
        }
    }
}

impl PatchGuard {
    /// What `with patch(..) as m:` binds: the mock, or the new args object.
    pub fn mock(&self) -> Arg {
        match self {
            PatchGuard::Attribute(g) => g.mock(),
            PatchGuard::Args { .. } => crate::basics::definitions::cli_capable::args(),
        }
    }
}

impl LailaError {
    /// `cm.exception` after `with self.assertRaises(T) as cm`.
    pub fn exception(&self) -> Arg {
        Arg::Exception(alloc::sync::Arc::new(self.clone()))
    }
    /// `cm.value` after `with pytest.raises(T) as cm`.
    pub fn value(&self) -> Arg {
        self.exception()
    }
}

fn cmp(
    a: impl IntoArg,
    b: impl IntoArg,
    ok: impl Fn(core::cmp::Ordering) -> bool,
    what: &str,
) -> Result<()> {
    let (a, b) = (a.into_arg(), b.into_arg());
    let ord = match (&a, &b) {
        (Arg::Str(x), Arg::Str(y)) => Some(x.cmp(y)),
        _ => match (a.as_f64(), b.as_f64()) {
            (Some(x), Some(y)) => x.partial_cmp(&y),
            _ => None,
        },
    };
    match ord {
        Some(o) if ok(o) => Ok(()),
        Some(_) => Err(fail(alloc::format!(
            "{} {} {}",
            a.__repr__(),
            what,
            b.__repr__()
        ))),
        None => Err(LailaError::type_error(alloc::format!(
            "'<' not supported between instances of '{}' and '{}'",
            a.type_name(),
            b.type_name()
        ))),
    }
}
