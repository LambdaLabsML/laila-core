//! `_LAILA_IDENTIFIABLE_FUTURE` -- a future's identity without its state.
//!
//! An identity handle carries only the metadata needed to *find* the live
//! future in some policy's `future_bank` (its `global_id`, `taskforce_id`,
//! `policy_id`, optional group / precedence / purpose); `status`,
//! `result`, `data`, `exception`, `wait` and `release` resolve through the
//! bank on demand (see the `FutureKind::Identity` arms in `future.rs`).
//! Every concrete future *is* an identity (Python's subclassing), so
//! `isinstance(fut, _LAILA_IDENTIFIABLE_FUTURE)` holds for all but
//! `GroupFuture`.

use super::future::{Future, FutureKind};

future_class_marker!(
    _LAILA_IDENTIFIABLE_FUTURE,
    "_LAILA_IDENTIFIABLE_FUTURE",
    "laila.policy.central.command.schema.future.future.future_identity",
    FutureKind::Identity,
    |k| k != FutureKind::Group
);

impl _LAILA_IDENTIFIABLE_FUTURE {
    /// `fut.future_identity` for any concrete future.
    pub fn of(fut: &Future) -> Future {
        fut.future_identity()
    }
}
