//! `GroupFuture` -- aggregate future tracking a fixed set of children.
//!
//! A group future is the natural return type of any "submit N tasks
//! together" operation. It owns no execution itself; it references the
//! children by `global_id` and delegates `status`, `result`, `wait` and
//! `__await__` to them. Children are looked up through the future banks on
//! every access -- the owning policy's bank first (`policy_id`), then every
//! other local policy's -- so the group keeps working regardless of which
//! policy is currently active.

use alloc::string::String;
use alloc::vec::Vec;

use crate::python::{Arg, Args, IntoArg, LailaError, Result};

use super::future::{Future, FutureKind};
use super::future_status::FutureStatus;

future_class_marker!(
    GroupFuture,
    "GroupFuture",
    "laila.policy.central.command.schema.future.future.group_future",
    FutureKind::Group,
    |k| k == FutureKind::Group
);

fn gid_str(x: &Arg) -> Option<String> {
    match x {
        Arg::Str(s) => Some(String::from(&**s)),
        Arg::None => None,
        other => other
            .__getattr__("global_id")
            .ok()
            .and_then(|g| g.as_str().map(String::from)),
    }
}

/// Candidate future banks: the owning policy's first, then every other
/// local one; the (lazily created) default policy when none is registered.
pub(crate) fn _iter_future_banks(policy_id: &Arg) -> Vec<Arg> {
    let mut seen: Vec<crate::policy::Policy> = Vec::new();
    let mut banks = Vec::new();
    if let Some(pid) = gid_str(policy_id) {
        if let Some(owner) = crate::policy::_local_policy_or_live(&pid) {
            banks.push(owner.future_bank());
            seen.push(owner);
        }
    }
    for policy in crate::policy::_future_bank_policies() {
        if seen.iter().any(|p| p.is(&policy)) {
            continue;
        }
        banks.push(policy.future_bank());
        seen.push(policy);
    }
    if seen.is_empty() {
        banks.push(crate::policy::_get_active_local_policy().future_bank());
    }
    banks
}

impl Future {
    /// `gf.future_ids` -- the child ids (the live list).
    pub fn future_ids(&self) -> Arg {
        self.0.fields.lock().future_ids.clone()
    }
    fn future_id_strings(&self) -> Vec<String> {
        self.future_ids()
            .__iter__()
            .unwrap_or_default()
            .iter()
            .map(|x| x.__str__())
            .collect()
    }
    /// `_resolve_children(strict=...)`: the child futures from the banks.
    /// With `strict=false` released children are skipped instead of
    /// raising `KeyError`.
    pub(crate) fn _resolve_children(&self, strict: bool) -> Result<Vec<Future>> {
        let banks = _iter_future_banks(&self.policy_id());
        let mut children = Vec::new();
        for fid in self.future_id_strings() {
            let mut found = None;
            for bank in &banks {
                if let Arg::Future(f) = bank.get(fid.as_str())? {
                    found = Some(f);
                    break;
                }
            }
            match found {
                Some(f) => children.push(f),
                None if strict => {
                    return Err(LailaError::key_error(crate::python::pyrepr::str_repr(&fid)))
                }
                None => {}
            }
        }
        Ok(children)
    }
    /// `gf.release(children=True)`.
    pub(crate) fn group_release(&self, children: bool) -> Result<()> {
        if children {
            for fid in self.future_id_strings() {
                for policy in crate::policy::_future_bank_policies() {
                    let bank = policy.future_bank();
                    if let Arg::Future(child) = bank.get(fid.as_str())? {
                        child.release(Args::new())?;
                        break;
                    }
                }
            }
        }
        let gid = self.global_id();
        for policy in crate::policy::_future_bank_policies() {
            let bank = policy.future_bank();
            if let Arg::Future(f) = bank.get(gid.as_str())? {
                if f.is(self) {
                    let mut a = Args::from(Arg::Str(gid.as_str().into()));
                    a.push_positional(Arg::None);
                    bank.pop(a)?;
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    // ---------- computed status ----------

    /// `gf.status`: the percentage breakdown of the children's statuses.
    pub(crate) fn group_status(&self) -> Arg {
        let ids = self.future_id_strings();
        let children = if ids.is_empty() {
            Vec::new()
        } else {
            self._resolve_children(false).unwrap_or_default()
        };
        let total = ids.len() as f64;
        let pct = |finished: f64, running: f64, not_started: f64, error: f64, cancelled: f64| {
            let mut p = crate::python::PyDictBuilder::new();
            p.insert("finished", finished);
            p.insert("running", running);
            p.insert("not_started", not_started);
            p.insert("error", error);
            p.insert("cancelled", cancelled);
            let mut d = crate::python::PyDictBuilder::new();
            d.insert("total", total);
            d.insert("percentages", p.finish());
            d.finish()
        };
        if children.is_empty() {
            return pct(0.0, 0.0, 100.0, 0.0, 0.0);
        }
        let live = children.len() as f64;
        let (mut running, mut not_started, mut cancelled, mut finished, mut error) =
            (0.0, 0.0, 0.0, 0.0, 0.0);
        for c in &children {
            if c.kind() == FutureKind::Group {
                continue;
            }
            match c._status() {
                FutureStatus::RUNNING | FutureStatus::POLL_TIMEOUT | FutureStatus::UNKNOWN => {
                    running += 1.0
                }
                FutureStatus::NOT_STARTED => not_started += 1.0,
                FutureStatus::CANCELLED => cancelled += 1.0,
                FutureStatus::FINISHED => finished += 1.0,
                FutureStatus::ERROR => error += 1.0,
            }
        }
        pct(
            finished / live * 100.0,
            running / live * 100.0,
            not_started / live * 100.0,
            error / live * 100.0,
            cancelled / live * 100.0,
        )
    }
    /// `gf.status["percentages"]` summed over the terminal states.
    #[cfg(feature = "std")]
    pub(crate) fn group_terminal_pct(&self) -> f64 {
        let st = self.group_status();
        let p = st.get("percentages").unwrap_or(Arg::None);
        ["finished", "error", "cancelled"]
            .iter()
            .map(|k| p.get(*k).ok().and_then(|v| v.as_f64()).unwrap_or(0.0))
            .sum()
    }

    // ---------- read-only interface ----------

    /// `gf.append(future_ids)`: merge additional child ids.
    pub fn append(&self, future_ids: impl IntoArg) -> Result<()> {
        let ids = future_ids.into_arg();
        self.0.fields.lock().future_ids.extend(ids)
    }
    /// `gf + other`: merge *other*'s children in place; `NotImplemented`
    /// for anything but a `GroupFuture`.
    pub fn __add__(&self, other: impl IntoArg) -> Result<Arg> {
        match other.into_arg() {
            Arg::Future(o) if o.kind() == FutureKind::Group => {
                let ids = o.future_ids();
                self.0.fields.lock().future_ids.extend(ids)?;
                Ok(Arg::Future(self.clone()))
            }
            _ => Ok(crate::python::builtins::not_implemented()),
        }
    }
    /// `gf.wait(timeout)`: wait on every child sequentially; their results.
    pub(crate) fn group_wait(&self, timeout: Option<f64>) -> Result<Arg> {
        super::super::super::exceptions::_check_not_loop_thread()?;
        let me = self.clone();
        super::super::super::parking::park_sync(move || -> Result<Arg> {
            let children = me._resolve_children(true)?;
            let mut out = Vec::with_capacity(children.len());
            for f in children {
                let mut a = Args::new();
                a.push_keyword("timeout", timeout);
                out.push(f.wait(a)?);
            }
            Ok(out.into_arg())
        })
    }
    /// `gf.result`: the children's results (assumes they completed).
    pub(crate) fn group_result(&self) -> Result<Arg> {
        let children = self._resolve_children(true)?;
        let mut out = Vec::with_capacity(children.len());
        for f in children {
            out.push(f.result()?);
        }
        Ok(out.into_arg())
    }
    /// `gf.data`: every child's unwrapped payload.
    pub(crate) fn group_data(&self) -> Result<Arg> {
        let children = self._resolve_children(true)?;
        let mut out = Vec::with_capacity(children.len());
        for f in children {
            out.push(f.data()?);
        }
        Ok(out.into_arg())
    }
    /// `await gf`: gather the children concurrently, parking the slot.
    pub(crate) async fn group_await(self) -> Result<Arg> {
        let me = self.clone();
        super::super::super::parking::park_async(async move {
            let children = me._resolve_children(true)?;
            let aws: Vec<crate::python::stdlib::asyncio::BoxFut> = children
                .into_iter()
                .map(|c| {
                    alloc::boxed::Box::pin(c.__await__()) as crate::python::stdlib::asyncio::BoxFut
                })
                .collect();
            crate::python::stdlib::asyncio::gather(aws).await
        })
        .await
    }
    /// `gf.exception`: the first child exception, else `None`.
    pub(crate) fn group_exception(&self) -> Arg {
        for f in self._resolve_children(false).unwrap_or_default() {
            let e = f.exception();
            if !e.is_none() {
                return e;
            }
        }
        Arg::None
    }

    // ---------- introspection ----------

    /// `gf.what`: the nested summary keyed by the group id.
    pub fn what(&self) -> Result<Arg> {
        let mut child_details = crate::python::PyDictBuilder::new();
        let mut cancelled_ids: Vec<Arg> = Vec::new();
        let mut not_cancelled_ids: Vec<Arg> = Vec::new();
        let mut errors = crate::python::PyDictBuilder::new();
        for f in self._resolve_children(false)? {
            let fid = f.global_id();
            let det = if f.kind() == FutureKind::Group {
                f.what()?
            } else {
                let mut d = crate::python::PyDictBuilder::new();
                d.insert("status", f._status().value());
                d.insert(
                    "error",
                    if f._status() == FutureStatus::ERROR {
                        Arg::Str(f.exception().__repr__().into())
                    } else {
                        Arg::None
                    },
                );
                let mut outer = crate::python::PyDictBuilder::new();
                outer.insert(fid.as_str(), d.finish());
                outer.finish()
            };
            for kv in det.items()?.__iter__()? {
                let pair = kv.__iter__()?;
                if let [k, v] = pair.as_slice() {
                    child_details.insert(k.clone(), v.clone());
                }
            }
            if f.kind() != FutureKind::Group && f.cancelled() {
                cancelled_ids.push(Arg::Str(fid.as_str().into()));
            } else {
                not_cancelled_ids.push(Arg::Str(fid.as_str().into()));
            }
            if f.kind() != FutureKind::Group && f._status() == FutureStatus::ERROR {
                errors.insert(fid.as_str(), f.exception().__repr__());
            }
        }
        let mut summary = crate::python::PyDictBuilder::new();
        summary.insert("cancelled", cancelled_ids);
        summary.insert("not_cancelled", not_cancelled_ids);
        summary.insert("errors", errors.finish());
        let mut body = crate::python::PyDictBuilder::new();
        body.insert("status", self.group_status());
        body.insert("taskforce_id", self.taskforce_id());
        body.insert("policy_id", self.policy_id());
        body.insert("futures", child_details.finish());
        body.insert("summary", summary.finish());
        let mut d = crate::python::PyDictBuilder::new();
        d.insert(self.global_id().as_str(), body.finish());
        Ok(d.finish())
    }
    /// `str(gf)` / `repr(gf)`: `json.dumps(gf.what)`.
    pub(crate) fn group_str(&self) -> String {
        match self.what() {
            Ok(w) => crate::python::stdlib::json
                .dumps(w)
                .unwrap_or_else(|e| e.__str__()),
            Err(e) => e.__str__(),
        }
    }
    /// `iter(gf)`: the child ids.
    pub fn __iter__(&self) -> Result<Vec<Arg>> {
        self.future_ids().__iter__()
    }
    /// `len(gf)`: the number of children.
    pub fn __len__(&self) -> usize {
        self.future_ids().__len__().unwrap_or(0)
    }
}
