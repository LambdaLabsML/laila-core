//! `laila.policy.central.memory` -- central memory: `memorize` / `remember`
//! / `forget` / `extend` over the pool router.
//!
//! Phase 3 carries the slice of `_LAILA_IDENTIFIABLE_CENTRAL_MEMORY`
//! (`schema/base.py`) the data layer is wired through: the router
//! ([`router`]), the alpha pool, `extend`, `_resolve_pool_ref` /
//! `_route_pool`, and the per-entry write / read / delete bodies
//! (`_memorize_one`, `_remember_one`, `_delete_one`) run inline on the
//! calling thread -- every write is a serialized [`Record`](record::Record)
//! envelope, every read goes through the pool's `_read_through_async`
//! and `Record._build_async`, and `_check_record_identity` /
//! `_stamp_source_pool` apply.
//!
//! Phase 4 adds the command taskforce submission (`cmd.submit`,
//! `GroupFuture`s), `_resolve_entry_key_async` (nickname /
//! `@evolution=-k` / `@creation_timestamp` references through
//! `_search_keys` / `_candidate_keys` across the proxy chain) and
//! `_duplicate_pool`.
//!
//! `_remember_with_persist` (the alpha write-back of `persist=True`)
//! runs the fetch + alpha memorize on a daemon thread, one
//! `ConcurrentPackageFuture` per id.
//!
//! Phase 5 adds the remote / relay paths, the logger hooks and
//! `borrow` (planned in both twins: `__enter__` raises).

pub mod hint;
pub mod record;
pub mod router;
pub mod schema;

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::basics::definitions::identifiable_object::{
    split_global_id_attributes, to_global_id, EVOLUTION_ATTRIBUTE,
};
use crate::data::Pool;
use crate::policy::central::command::Future;
use crate::policy::Policy;
use crate::python::object::PyObject;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

use record::Record;
use router::PoolRouter;

/// `laila.memory` / `policy.central.memory`: a central-memory handle.
/// `Memory::default()` resolves the *active* policy on every call (the
/// facade's `laila.memory`); [`Memory::of`] binds one policy.
#[derive(Clone, Default)]
pub struct Memory {
    policy: Option<Policy>,
    /// A detached `_LAILA_IDENTIFIABLE_CENTRAL_MEMORY()`'s own state (router
    /// and alpha pool) until a policy adopts it (`central.memory = m`), after
    /// which the two share it.
    state: Option<Arc<crate::policy::CentralMemoryState>>,
}

/// The context manager returned by [`Memory::borrow`]; `__enter__` raises
/// `NotImplementedError` (the planned semantics pin `keys` in the alpha
/// pool for the block, announced to peers with `global_borrow=True`).
#[derive(Clone, Debug)]
pub struct BorrowContext {
    /// The keys to pin (`[]` when `None` was given).
    pub keys: Arg,
    /// Whether the borrow is announced to peer policies.
    pub global_borrow: bool,
}

impl BorrowContext {
    /// Raises `NotImplementedError`.
    pub fn __enter__(&self) -> Result<Arg> {
        Err(LailaError::not_implemented(""))
    }
    /// Nothing to release.
    pub fn __exit__(&self) -> Result<bool> {
        Ok(false)
    }
}

/// `laila.policy.central.memory.schema.base._LAILA_IDENTIFIABLE_CENTRAL_MEMORY`
/// -- the central-memory class: `_LAILA_IDENTIFIABLE_CENTRAL_MEMORY()` is
/// a detached [`Memory`] with its own router (see [`Memory::new`]).
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _LAILA_IDENTIFIABLE_CENTRAL_MEMORY;

impl _LAILA_IDENTIFIABLE_CENTRAL_MEMORY {
    /// `_LAILA_IDENTIFIABLE_CENTRAL_MEMORY(**kwargs)`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Memory> {
        Memory::new(args)
    }
    /// `_LAILA_IDENTIFIABLE_CENTRAL_MEMORY._key_evolution(key)` -- the
    /// staticmethod through the class: [`Memory::_key_evolution`].
    pub fn _key_evolution(&self, key: impl IntoArg) -> Result<i64> {
        let key = key.into_arg();
        match key.as_str() {
            Some(k) => Ok(Memory::_key_evolution(k)),
            None => Err(LailaError::type_error(alloc::format!(
                "expected str, got {}",
                key.type_name()
            ))),
        }
    }
    /// `_LAILA_IDENTIFIABLE_CENTRAL_MEMORY._pool_chain(pool)` -- the
    /// staticmethod through the class: [`Memory::_pool_chain`].
    pub fn _pool_chain(&self, pool: impl IntoArg) -> Result<Arg> {
        let pool = pool.into_arg();
        match &pool {
            Arg::Pool(p) => Ok(Memory::_pool_chain(p).into_arg()),
            other => Err(LailaError::type_error(alloc::format!(
                "expected a pool, got {}",
                other.type_name()
            ))),
        }
    }
    /// The class as a Python value.
    pub fn class_object() -> crate::python::PyClass {
        crate::python::PyClass::with_ctor(
            "_LAILA_IDENTIFIABLE_CENTRAL_MEMORY",
            "laila.policy.central.memory.schema.base",
            Arc::new(|a: Args| Memory::new(a).map(IntoArg::into_arg)),
        )
    }
}
impl PyArg for _LAILA_IDENTIFIABLE_CENTRAL_MEMORY {
    fn to_arg(&self) -> Arg {
        Self::class_object().into_arg()
    }
}
impl IntoArg for _LAILA_IDENTIFIABLE_CENTRAL_MEMORY {
    fn into_arg(self) -> Arg {
        Self::class_object().into_arg()
    }
}
impl crate::python::builtins::CallValue for _LAILA_IDENTIFIABLE_CENTRAL_MEMORY {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Memory::new(args).map(IntoArg::into_arg)
    }
}
impl crate::python::builtins::IsInstance for _LAILA_IDENTIFIABLE_CENTRAL_MEMORY {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<Memory>())
    }
}

/// `@ensure_list(name)`: a single value becomes `[value]`; lists, sets
/// and frozensets pass through.
use crate::utils::decorators::typecheck::ensure_list_value as ensure_list;

impl Memory {
    /// `_LAILA_IDENTIFIABLE_CENTRAL_MEMORY()`: a detached memory with its
    /// own default router / alpha pool (`model_post_init`); record
    /// provenance and the internal taskforce come from the active policy.
    pub fn new(_args: impl Into<Args>) -> Result<Memory> {
        Ok(Memory {
            policy: None,
            state: Some(Arc::new(crate::policy::CentralMemoryState::new()?)),
        })
    }
    /// The memory of *policy*.
    pub fn of(policy: Policy) -> Memory {
        Memory {
            policy: Some(policy),
            state: None,
        }
    }
    /// The state this handle reads: its own when detached, else the
    /// policy's.
    pub fn state_arc(&self) -> Arc<crate::policy::CentralMemoryState> {
        match &self.state {
            Some(s) => s.clone(),
            None => self._policy().memory_state(),
        }
    }
    /// The policy that owns this memory (the active one for the facade's).
    pub fn _policy(&self) -> Policy {
        self.policy
            .clone()
            .unwrap_or_else(crate::policy::get_active_policy)
    }
    /// `memory.pool_router`
    pub fn pool_router(&self) -> PoolRouter {
        self.state_arc().pool_router.clone()
    }
    /// `memory.alpha_pool` -- the alpha pool's global id.
    pub fn alpha_pool(&self) -> Arg {
        Arg::Str(self.state_arc().alpha_pool.read().clone().into())
    }
    /// `memory.global_id` -- the owning policy's id as record provenance.
    pub fn _owner_gid(&self) -> String {
        self._policy().global_id()
    }

    /// Forward pool registration to `PoolRouter.extend`.
    #[pysig("extend(pool, *, affinity=None, pool_nickname=None)")]
    pub fn extend(&self, args: impl Into<Args>) -> Result<()> {
        let mut a = Args::new();
        a.push_positional(pool);
        a.push_keyword("affinity", affinity);
        a.push_keyword("pool_nickname", pool_nickname);
        self.pool_router().extend(a)
    }

    /// Resolve a pool object, gid string, or nickname to a live pool
    /// instance (`KeyError` when the string matches neither a gid nor a
    /// nickname; `TypeError` for anything else).
    pub fn _resolve_pool_ref(&self, pool_ref: &Arg) -> Result<Pool> {
        if let Arg::Pool(p) = pool_ref {
            return Ok(p.clone());
        }
        if let Arg::Str(name) = pool_ref {
            let router = self.pool_router();
            let pools = router.pools();
            if pools.__contains__(name.as_ref())? {
                if let Arg::Pool(p) = pools.__getitem__(name.as_ref())? {
                    return Ok(p);
                }
            }
            let nicknames = router.pools_nicknames();
            if nicknames.__contains__(name.as_ref())? {
                if let Arg::Pool(p) = pools.__getitem__(nicknames.__getitem__(name.as_ref())?)? {
                    return Ok(p);
                }
            }
            return Err(LailaError::key_error(crate::python::pyrepr::str_repr(
                &alloc::format!("Pool '{name}' was not found."),
            )));
        }
        Err(LailaError::type_error(
            "pool_ref must be a pool object, pool id, or pool nickname.",
        ))
    }

    /// Resolve the target pool for a memory op: `pool` (a live pool, a
    /// gid or a nickname) wins when supplied; otherwise the router decides
    /// (`pool_id` > `pool_nickname` > default alpha).
    pub fn _route_pool(&self, pool: &Arg, pool_id: &Arg, pool_nickname: &Arg) -> Result<Pool> {
        if !pool.is_none() {
            return self._resolve_pool_ref(pool);
        }
        let mut a = Args::new();
        a.push_positional(Arg::None);
        a.push_keyword("pool_id", pool_id.clone());
        a.push_keyword("pool_nickname", pool_nickname.clone());
        self.pool_router().route(a)
    }

    /// Persist *entries* to the routed pool.
    ///
    /// Routes through the `PoolRouter` (`pool_id` > `pool_nickname` >
    /// default) and runs the per-entry write body (`_memorize_one`) for
    /// each entry. Returns an already-settled future: its `data` is the
    /// entry's global id (one entry) or the list of ids (many), and a
    /// failed write raises from `.wait()` / `.result`.
    #[pysig("memorize(entries, *, pool=None, pool_id=None, pool_nickname=None, affinity=None)")]
    pub fn memorize(&self, args: impl Into<Args>) -> Result<Future> {
        let _ = affinity;
        // A *bare* Manifest dispatches to ``Manifest.memorize`` (its
        // pending children first, then itself); a list stores the manifest
        // as a plain entry.
        if let Arg::Entry(e) = &entries {
            if let Some(m) = schema::manifest::Manifest::from_entry(e) {
                let mut fwd = Args::new();
                fwd.push_keyword("pool", pool);
                fwd.push_keyword("pool_id", pool_id);
                fwd.push_keyword("pool_nickname", pool_nickname);
                return m.memorize(fwd);
            }
        }
        let entries = ensure_list(entries);
        let routed = self._route_pool(&pool, &pool_id, &pool_nickname)?;
        crate::logger::hook_memorize(
            entries.clone(),
            routed.clone().into_arg(),
            self._policy().into_arg(),
        );
        self._record(entries, routed)
    }

    /// `self._record(entries, pool)`: the write primitive `memorize` ends
    /// in -- batch-accelerated pools go through `_batch_accelerated_record`,
    /// others use the per-entry path. A `patch.object(memory, "_record")`
    /// replaces it (the mock's return is handed back as a settled future).
    pub fn _record(&self, entries: Arg, pool: Pool) -> Result<Future> {
        if let Some(out) = self._patched("_record", {
            let mut a = Args::from(entries.clone());
            a.push_positional(pool.clone());
            a
        }) {
            return out;
        }
        if pool.batch_accelerated() {
            return Err(LailaError::not_implemented(""));
        }
        self._parallel_individual_record(entries, pool)
    }

    /// `self._fetch(entry_ids, pool=pool)`: the read primitive `remember`
    /// ends in (patchable like [`Memory::_record`]).
    pub fn _fetch(&self, entry_ids: Arg, pool: Pool) -> Result<Future> {
        if let Some(out) = self._patched("_fetch", {
            let mut a = Args::from(entry_ids.clone());
            a.push_keyword("pool", pool.clone());
            a
        }) {
            return out;
        }
        if pool.batch_accelerated() {
            return Err(LailaError::not_implemented(""));
        }
        self._parallel_individual_fetch(entry_ids, pool)
    }

    /// `self._delete(entry_ids, pool=pool)`: the delete primitive `forget`
    /// ends in (patchable like [`Memory::_record`]).
    pub fn _delete(&self, entry_ids: Arg, pool: Pool) -> Result<Future> {
        if let Some(out) = self._patched("_delete", {
            let mut a = Args::from(entry_ids.clone());
            a.push_keyword("pool", pool.clone());
            a
        }) {
            return out;
        }
        if pool.batch_accelerated() {
            return Err(LailaError::not_implemented(""));
        }
        self._parallel_individual_delete(entry_ids, pool)
    }

    /// Copy every entry from *pool_src* into *pool_dest* asynchronously
    /// (the `cache <= origin` operator): one `ConcurrentPackageFuture` per
    /// entry under a `GroupFuture`, each flipping to `FINISHED` once that
    /// entry has landed in *pool_dest*. Both legs run on a daemon thread,
    /// *inflight_max_entries* (read, write) pairs at a time.
    ///
    /// `ValueError` for `inflight_max_entries < 1`; `KeyError` when either
    /// pool is not registered with this memory.
    /// `with memory.borrow(keys=None, global_borrow=False): ...` -- context
    /// manager that lends entries to the caller for the scope of a `with`
    /// block. Not yet implemented: `__enter__` raises `NotImplementedError`
    /// (the Python `@contextmanager` generator raises on entry, not on call).
    // TODO: need to make sure cross-borrowing does not lead to stall
    #[pysig("borrow(keys=None, global_borrow=False)")]
    pub fn borrow(&self, args: impl Into<Args>) -> Result<BorrowContext> {
        let keys = if keys.is_none() {
            crate::python::builtins::list(Arg::Tuple(alloc::vec![].into()))?
        } else {
            keys
        };
        Ok(BorrowContext {
            keys,
            global_borrow: global_borrow.__bool__(),
        })
    }

    #[pysig("_duplicate_pool(pool_src, pool_dest, *, inflight_max_entries=4)")]
    pub fn _duplicate_pool(&self, args: impl Into<Args>) -> Result<Future> {
        let inflight = if inflight_max_entries.is_none() {
            4
        } else {
            inflight_max_entries.as_i64().unwrap_or(0)
        };
        if inflight < 1 {
            return Err(LailaError::value_error(
                "duplicate_pool requires inflight_max_entries >= 1.",
            ));
        }
        let src_pool = self._resolve_pool_ref(&pool_src)?;
        let dest_pool = self._resolve_pool_ref(&pool_dest)?;
        // Both legs are routed through central memory by pool gid, so an
        // unregistered pool would otherwise fail per entry with an opaque
        // KeyError on the gid. Check up front and say what to do instead.
        let pools = self.pool_router().pools();
        for (role, pool) in [("source", &src_pool), ("destination", &dest_pool)] {
            if !pools.__contains__(pool.global_id().as_str())? {
                return Err(LailaError::key_error(crate::python::pyrepr::str_repr(
                    &alloc::format!(
                        "{role} pool {} is not registered with central memory; \
                         call laila.memory.extend(pool) first",
                        pool.global_id()
                    ),
                )));
            }
        }
        let entry_ids: Vec<String> = src_pool
            .keys(Args::new())?
            .__iter__()?
            .iter()
            .map(|k| k.__str__())
            .collect();
        self._duplicate_entries(
            entry_ids,
            src_pool,
            dest_pool,
            inflight as usize,
            "duplicate_pool",
            "DuplicatePool",
        )
    }

    /// The copy runner shared by [`_duplicate_pool`](Self::_duplicate_pool)
    /// (`pool_dest <= pool_src`) and `PoolWrapper.__le__`
    /// (`pool_dest[manifest] <= pool_src[manifest]`): one
    /// `ConcurrentPackageFuture` per entry under a `GroupFuture`; a daemon
    /// thread `remember`s each entry from *src_pool* and `memorize`s it
    /// into *dest_pool*, at most *inflight* copies at a time.
    pub(crate) fn _duplicate_entries(
        &self,
        entry_ids: Vec<String>,
        src_pool: Pool,
        dest_pool: Pool,
        inflight: usize,
        purpose: &str,
        thread_name: &str,
    ) -> Result<Future> {
        use crate::policy::central::command::{FutureKind, FutureStatus};

        let cmd = self._policy().central().command();
        let owner_gid = self._owner_gid();
        let mut children: Vec<(String, Future)> = Vec::with_capacity(entry_ids.len());
        for entry_id in &entry_ids {
            let mut a = Args::new();
            a.push_keyword("taskforce_id", cmd.internal_taskforce());
            a.push_keyword("policy_id", owner_gid.as_str());
            a.push_keyword("purpose", alloc::format!("{purpose}:{entry_id}"));
            children.push((
                entry_id.clone(),
                Future::construct(FutureKind::Concurrent, a)?,
            ));
        }
        let mut a = Args::new();
        a.push_keyword("taskforce_id", cmd.internal_taskforce());
        a.push_keyword("policy_id", owner_gid.as_str());
        a.push_keyword(
            "future_ids",
            children
                .iter()
                .map(|(_, f)| Arg::Str(f.global_id().into()))
                .collect::<Vec<_>>(),
        );
        let group_future = Future::construct(FutureKind::Group, a)?;
        for (_, child) in &children {
            child.__setattr__("future_group_id", group_future.global_id().as_str())?;
        }

        let memory = self.clone();
        let (src_gid, dest_gid) = (src_pool.global_id(), dest_pool.global_id());
        let duplicate_one = move |entry_id: &str, child: &Future| -> Result<()> {
            child.set_status(FutureStatus::RUNNING);
            let mut ra = Args::new();
            ra.push_keyword("entry_ids", entry_id);
            ra.push_keyword("pool_id", src_gid.as_str());
            let entry = memory.remember(ra)?.wait(Args::new())?;
            let mut ma = Args::new();
            ma.push_keyword("entries", entry);
            ma.push_keyword("pool_id", dest_gid.as_str());
            memory.memorize(ma)?.wait(Args::new())?;
            child.set_exception(None);
            child.set_result(Arg::Str(entry_id.into()));
            child.set_status(FutureStatus::FINISHED);
            Ok(())
        };
        let run_all = move || {
            // ``asyncio.Semaphore(inflight)``: at most *inflight* (read,
            // write) pairs at a time.
            for chunk in children.chunks(inflight.max(1)) {
                crate::python::sync::for_each_parallel(chunk, |(entry_id, child)| {
                    if let Err(exc) = duplicate_one(entry_id, child) {
                        child.set_exception(Some(Arc::new(exc)));
                        child.set_result(Arg::None);
                        child.set_status(FutureStatus::ERROR);
                    }
                });
            }
        };
        crate::python::sync::spawn_named(
            alloc::format!("{thread_name}-{}", group_future.global_id()),
            run_all,
        )?;
        Ok(group_future)
    }

    /// A `patch.object(memory, name, ..)` in effect: call the mock with
    /// *args*. The typed return is a `Future`, so a non-future mock result
    /// is handed back as an already-settled one.
    fn _patched(&self, name: &str, args: Args) -> Option<Result<Future>> {
        let out = crate::python::mock::call_patched(
            crate::python::object::PyObject::type_name(self),
            name,
            self.clone(),
            args,
        )?;
        Some(out.map(|v| match v {
            Arg::Future(f) => f,
            other => Future::done(other),
        }))
    }

    /// Memorize each entry as one per-entry coroutine on the internal
    /// taskforce: the single future (one entry) or a `GroupFuture`.
    pub fn _parallel_individual_record(&self, entries: Arg, pool: Pool) -> Result<Future> {
        let policy_gid = self._owner_gid();
        let transformations = pool.transformations();
        let factories: Vec<Arg> = entries
            .__iter__()?
            .into_iter()
            .map(|e| {
                let (p, t, pgid) = (pool.clone(), transformations.clone(), policy_gid.clone());
                crate::python::stdlib::asyncio::coroutine_function(
                    "_memorize_one",
                    move |_a: Args| {
                        let (e, p, t, pgid) = (e.clone(), p.clone(), t.clone(), pgid.clone());
                        async move { Self::_memorize_one(&e, &p, &t, &pgid).await }
                    },
                )
            })
            .collect();
        self._submit_internal(factories)
    }

    /// `cmd.submit(tasks=factories, taskforce_id=cmd.internal_taskforce)`.
    fn _submit_internal(&self, factories: Vec<Arg>) -> Result<Future> {
        let cmd = self._policy().central().command();
        let mut a = Args::new();
        a.push_keyword("tasks", factories.into_arg());
        a.push_keyword("taskforce_id", cmd.internal_taskforce());
        match cmd.submit(a)? {
            Arg::Future(f) => Ok(f),
            other => Err(LailaError::type_error(alloc::format!(
                "command.submit returned {}",
                other.type_name()
            ))),
        }
    }

    /// The per-entry write coroutine of `_parallel_individual_record`.
    async fn _memorize_one(e: &Arg, p: &Pool, t: &Arg, pgid: &str) -> Result<Arg> {
        // A variable entry whose payload was re-assigned since its last
        // memorize becomes a new evolution (in place); an untouched one
        // is re-written under the same key.
        if let Arg::Entry(entry) = e {
            entry.bump_evolution_if_locally_modified();
        }
        let mut ra = Args::new();
        ra.push_keyword("entry", e.clone());
        ra.push_keyword("recorder", pgid);
        let record = Record::new(ra)?;
        let mut blob = Record::serialize_record(&record, t.clone())?;
        if let Ok(data) = blob.__getattr__("data") {
            blob = data;
        }
        let gid = e.__getattr__("global_id")?.__str__();
        p.write_async(&gid, blob).await?;
        if let Arg::Entry(entry) = e {
            entry.mark_memorized();
            entry.set_source_pool_gid(Some(p.global_id()));
        }
        Ok(Arg::Str(gid.into()))
    }

    /// Fetch entries from the routed pool.
    ///
    /// Each id is read through the pool's proxy chain
    /// (`_read_through_async`), rebuilt from its `Record` envelope and
    /// identity-checked against the key. Returns an already-settled
    /// future whose `data` is the entry (one id) or the list of entries.
    /// `KeyError` if an id is missing from the routed pool.
    #[pysig("remember(entry_ids, *, pool=None, pool_id=None, pool_nickname=None, affinity=None, persist=True)")]
    pub fn remember(&self, args: impl Into<Args>) -> Result<Future> {
        let _ = affinity;
        let entry_ids = ensure_list(entry_ids);
        let routed = self._route_pool(&pool, &pool_id, &pool_nickname)?;
        crate::logger::hook_remember(
            entry_ids.clone(),
            routed.clone().into_arg(),
            self._policy().into_arg(),
        );
        if !persist.__bool__() || routed.global_id() == self.alpha_pool().__str__() {
            return self._fetch(entry_ids, routed);
        }
        self._remember_with_persist(entry_ids, routed)
    }

    /// Fetch entries from *pool* and then memorize them into the alpha
    /// pool (`remember(..., persist=True)` from a non-alpha pool).
    ///
    /// Returns a single future (one id) or a `GroupFuture` (many) whose
    /// children only reach `FINISHED` once both the fetch and the
    /// alpha-pool write have completed. Modeled after
    /// [`_duplicate_entries`](Self::_duplicate_entries): the pipeline runs
    /// on a daemon thread so the caller's slot is never blocked.
    pub fn _remember_with_persist(&self, entry_ids: Arg, pool: Pool) -> Result<Future> {
        use crate::policy::central::command::{FutureKind, FutureStatus};

        let cmd = self._policy().central().command();
        let owner_gid = self._owner_gid();
        let ids: Vec<String> = entry_ids
            .__iter__()?
            .iter()
            .map(|eid| match eid {
                Arg::Entry(e) => e.global_id(),
                other => other.__str__(),
            })
            .collect();
        let mut children: Vec<Future> = Vec::with_capacity(ids.len());
        for entry_id in &ids {
            let mut a = Args::new();
            a.push_keyword("taskforce_id", cmd.internal_taskforce());
            a.push_keyword("policy_id", owner_gid.as_str());
            a.push_keyword(
                "purpose",
                alloc::format!("remember_with_persist:{entry_id}"),
            );
            children.push(Future::construct(FutureKind::Concurrent, a)?);
        }
        let group_future = if children.len() > 1 {
            let mut a = Args::new();
            a.push_keyword("taskforce_id", cmd.internal_taskforce());
            a.push_keyword("policy_id", owner_gid.as_str());
            a.push_keyword(
                "future_ids",
                children
                    .iter()
                    .map(|f| Arg::Str(f.global_id().into()))
                    .collect::<Vec<_>>(),
            );
            let gf = Future::construct(FutureKind::Group, a)?;
            for child in &children {
                child.__setattr__("future_group_id", gf.global_id().as_str())?;
            }
            Some(gf)
        } else {
            None
        };
        let alpha_pool_id = self.alpha_pool();
        let memory = self.clone();
        let run_children = children.clone();
        let run = move || -> Result<()> {
            for child in &run_children {
                child.set_status(FutureStatus::RUNNING);
            }
            // The fetch / memorize futures below are consumed right here
            // and never handed out, so this runner owns their release.
            let fetch_ref = memory._fetch(entry_ids, pool)?;
            let fetched = fetch_ref.wait(Args::new());
            let _ = fetch_ref.release(Args::new());
            let fetched = fetched?;
            let entries: Vec<Arg> = match &fetched {
                Arg::List(_) => fetched.__iter__()?,
                _ => alloc::vec![fetched],
            };
            let ready: Vec<Arg> = entries
                .iter()
                .filter(|e| {
                    e.as_entry()
                        .is_some_and(|e| e.state() == crate::entry::EntryState::READY)
                })
                .cloned()
                .collect();
            if !ready.is_empty() {
                let mut ma = Args::new();
                ma.push_keyword("entries", ready);
                ma.push_keyword("pool_id", alpha_pool_id);
                let memorize_ref = memory.memorize(ma)?;
                let out = memorize_ref.wait(Args::new());
                let _ = memorize_ref.release(Args::new());
                out?;
            }
            for (entry, child) in entries.into_iter().zip(run_children.iter()) {
                child.set_exception(None);
                child.set_result(entry);
                child.set_status(FutureStatus::FINISHED);
            }
            Ok(())
        };
        // Carry only the resolve chain (not the caller's slot context) onto
        // the helper thread so cycle detection keeps working across it.
        let resolve_chain = crate::policy::central::command::schema::parking::_RESOLVE_CHAIN.get();
        let fail_children = children.clone();
        let thread_body = move || {
            crate::policy::central::command::schema::parking::_RESOLVE_CHAIN.set(resolve_chain);
            if let Err(exc) = run() {
                let exc = Arc::new(exc);
                for child in &fail_children {
                    if child._status().is_terminal() {
                        continue;
                    }
                    child.set_exception(Some(exc.clone()));
                    child.set_result(Arg::None);
                    child.set_status(FutureStatus::ERROR);
                }
            }
        };
        let handle_gid = group_future
            .as_ref()
            .map(|g| g.global_id())
            .unwrap_or_else(|| children[0].global_id());
        crate::python::sync::spawn_named(
            alloc::format!("RememberWithPersist-{handle_gid}"),
            thread_body,
        )?;
        Ok(match group_future {
            Some(gf) => gf,
            None => children.into_iter().next().expect("one child future"),
        })
    }

    /// Fetch each entry as one per-entry coroutine on the internal
    /// taskforce: the single future (one id) or a `GroupFuture`.
    pub fn _parallel_individual_fetch(&self, entry_ids: Arg, pool: Pool) -> Result<Future> {
        let ids = entry_ids.__iter__()?;
        let keys: Vec<String> = ids
            .iter()
            .map(|eid| match eid {
                Arg::Entry(e) => e.global_id(),
                other => other.__str__(),
            })
            .collect();
        crate::policy::central::command::schema::parking::check_resolve_cycle(
            &keys.iter().map(String::as_str).collect::<Vec<_>>(),
        )?;
        let factories: Vec<Arg> = ids
            .into_iter()
            .map(|eid| {
                let p = pool.clone();
                crate::python::stdlib::asyncio::coroutine_function(
                    "_remember_one",
                    move |_a: Args| {
                        let (eid, p) = (eid.clone(), p.clone());
                        async move { Self::_remember_one(&eid, &p).await }
                    },
                )
            })
            .collect();
        self._submit_internal(factories)
    }

    /// Evolution encoded in a storage key; `-1` for a constant (no `@`).
    pub fn _key_evolution(key: &str) -> i64 {
        crate::data::schema::pool_index::_key_evolution(key).unwrap_or(-1)
    }

    /// *pool* followed by its `_proxy_to` upstream tiers, front to back.
    pub fn _pool_chain(pool: &Pool) -> Vec<Pool> {
        let mut chain = alloc::vec![pool.clone()];
        chain.extend(pool._upstream_tiers());
        chain
    }

    /// Turn an entry *reference* into the storage key to read:
    /// `(storage_key, raw_record_or_None)`.
    ///
    /// - `LAILA:ENTRY:<uuid>@evolution=N` (`N >= 0`) -- exact key;
    /// - `LAILA:ENTRY:<uuid>` -- the exact key when it exists locally (a
    ///   constant), else the highest stored evolution across *pool* and
    ///   its proxy chain;
    /// - `@evolution=-k` -- the k-th evolution from the end;
    /// - `@creation_timestamp=<iso>` (optionally with `evolution`) -- the
    ///   stored evolution whose record carries that stamp exactly (its
    ///   raw record is returned so the caller does not read it twice).
    ///
    /// Every tier contributes candidates (its index shard via
    /// `_search_keys`, or a prefix scan via `_candidate_keys`); the union
    /// is ranked once and only the chosen key is validated with
    /// `_exists_async` -- a stale indexed key is un-indexed and the next
    /// best tried; when the indexes are entirely stale the chain is
    /// re-scanned from its keys.
    pub async fn _resolve_entry_key_async(pool: &Pool, eid: &str) -> Result<(String, Arg)> {
        use crate::basics::definitions::identifiable_object::{
            resolve_global_id_for, ResolveOptions,
        };
        use crate::data::schema::pool_index::{
            _record_creation_timestamp, CREATION_TIMESTAMP_ATTRIBUTE,
        };
        // Shorthands ("ENTRY:nick@evolution=3") are normally expanded by
        // `laila.remember`; expand here too for direct callers.
        let eid =
            resolve_global_id_for(crate::entry::_DEFAULT_SCOPES, eid, &ResolveOptions::new())?;
        let (base, attrs) = split_global_id_attributes(&eid)?;
        let mut unknown: Vec<&str> = attrs
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|k| *k != EVOLUTION_ATTRIBUTE && *k != CREATION_TIMESTAMP_ATTRIBUTE)
            .collect();
        if !unknown.is_empty() {
            unknown.sort_unstable();
            unknown.dedup();
            let mut supported = [CREATION_TIMESTAMP_ATTRIBUTE, EVOLUTION_ATTRIBUTE];
            supported.sort_unstable();
            let list = |items: &[&str]| {
                let parts: Vec<String> = items
                    .iter()
                    .map(|s| crate::python::pyrepr::str_repr(s))
                    .collect();
                alloc::format!("[{}]", parts.join(", "))
            };
            return Err(LailaError::value_error(alloc::format!(
                "Unsupported search attribute(s) {} in {}; supported: {}",
                list(&unknown),
                crate::python::pyrepr::str_repr(&eid),
                list(&supported),
            )));
        }
        let find = |name: &str| {
            attrs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        let evolution: Option<i64> = match find(EVOLUTION_ATTRIBUTE) {
            Some(raw) => Some(raw.trim().parse::<i64>().map_err(|_| {
                LailaError::value_error(alloc::format!(
                    "invalid literal for int() with base 10: {}",
                    crate::python::pyrepr::str_repr(&raw)
                ))
            })?),
            None => None,
        };
        let creation_timestamp = find(CREATION_TIMESTAMP_ATTRIBUTE);
        let chain = Self::_pool_chain(pool);

        // Fast paths: a non-negative explicit evolution is an exact key; a
        // bare id that exists as-is (a constant) needs no lookup.
        if creation_timestamp.is_none() {
            if let Some(ev) = evolution {
                if ev >= 0 {
                    return Ok((eid, Arg::None));
                }
            }
            if evolution.is_none() && pool._exists_async(&base).await? {
                return Ok((base, Arg::None));
            }
        }

        let mut candidates = Self::_gather_candidates(&chain, &base, &attrs, evolution, true)?;

        let Some(creation_timestamp) = creation_timestamp else {
            let mut rescanned = false;
            loop {
                let Some(pos) = Self::_pick_candidate(&candidates, &base, evolution) else {
                    if !rescanned {
                        rescanned = true;
                        candidates =
                            Self::_gather_candidates(&chain, &base, &attrs, evolution, false)?;
                        if !candidates.is_empty() {
                            continue;
                        }
                    }
                    return Err(LailaError::key_error(crate::python::pyrepr::str_repr(
                        &alloc::format!("Entry {eid} not found in pool {}", pool.global_id()),
                    )));
                };
                let (tier, key, indexed) = candidates[pos].clone();
                if tier._exists_async(&key).await? {
                    return Ok((key, Arg::None));
                }
                candidates.remove(pos);
                if indexed {
                    tier.index().remove(key.as_str())?;
                }
            }
        };

        // Time-based search: inspect each candidate record's creation_timestamp.
        if let Some(ev) = evolution {
            if ev < 0 {
                // The stamp must agree with the k-th evolution from the end.
                let mut ranked: Vec<i64> = candidates
                    .iter()
                    .map(|c| Self::_key_evolution(&c.1))
                    .collect();
                ranked.sort_unstable();
                ranked.dedup();
                let idx = ranked.len() as i64 + ev;
                let wanted = if idx >= 0 {
                    Some(ranked[idx as usize])
                } else {
                    None
                };
                candidates.retain(|c| Some(Self::_key_evolution(&c.1)) == wanted);
            }
        }
        let mut best: Option<(i64, String, Arg)> = None;
        for (tier, key, _indexed) in &candidates {
            let raw = tier._read_async(key).await?;
            if raw.is_none() {
                continue;
            }
            if _record_creation_timestamp(&raw).__str__() == creation_timestamp {
                let ev = Self::_key_evolution(key);
                if best.as_ref().is_none_or(|b| ev > b.0) {
                    best = Some((ev, key.clone(), raw));
                }
            }
        }
        match best {
            Some((_, key, raw)) => Ok((key, raw)),
            None => Err(LailaError::key_error(crate::python::pyrepr::str_repr(
                &alloc::format!(
                    "No evolution of {base} with creation_timestamp={} in pool {}",
                    crate::python::pyrepr::str_repr(&creation_timestamp),
                    pool.global_id()
                ),
            ))),
        }
    }

    /// Collect `(tier, key, from_index)` candidates for *base* across
    /// *chain*: each tier's indexed keys (`_search_keys`) or, without an
    /// index answer / with `use_index=false`, a prefix scan
    /// (`_candidate_keys`). A key found on several tiers is attributed
    /// to the nearest one.
    fn _gather_candidates(
        chain: &[Pool],
        base: &str,
        attrs: &[(String, String)],
        evolution: Option<i64>,
        use_index: bool,
    ) -> Result<Vec<(Pool, String, bool)>> {
        let attrs_arg: Arg = attrs
            .iter()
            .map(|(k, v)| (Arg::Str(k.as_str().into()), Arg::Str(v.as_str().into())))
            .collect::<Vec<_>>()
            .into_arg();
        let prefix = alloc::format!("{base}@");
        let mut seen: Vec<String> = Vec::new();
        let mut out = Vec::new();
        for tier in chain {
            let mut keys: Arg = Arg::None;
            if use_index {
                keys = tier._search_keys(base, &attrs_arg)?;
            }
            let from_index = !keys.is_none();
            let keys: Vec<String> = if keys.is_none() {
                tier.0._candidate_keys(base)?
            } else {
                keys.__iter__()?.into_iter().map(|k| k.__str__()).collect()
            };
            for key in keys {
                if seen.contains(&key) {
                    continue;
                }
                if key != base && !key.starts_with(&prefix) {
                    continue;
                }
                if let Some(ev) = evolution {
                    if ev >= 0 && Self::_key_evolution(&key) != ev {
                        continue;
                    }
                }
                seen.push(key.clone());
                out.push((tier.clone(), key, from_index));
            }
        }
        Ok(out)
    }

    /// The index of the candidate a lineage query refers to, or `None`:
    /// `evolution=None` -- the constant key when present, else the
    /// highest evolution; `evolution < 0` -- the k-th from the end over
    /// the sorted, de-duplicated evolutions (constants rank lowest);
    /// `evolution >= 0` -- the (already filtered) exact match.
    fn _pick_candidate(
        candidates: &[(Pool, String, bool)],
        base: &str,
        evolution: Option<i64>,
    ) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }
        match evolution {
            None => {
                if let Some(i) = candidates.iter().position(|c| c.1 == base) {
                    return Some(i);
                }
                // `max` keeps the first of equal keys, like Python's.
                let mut best = 0usize;
                for (i, c) in candidates.iter().enumerate().skip(1) {
                    if Self::_key_evolution(&c.1) > Self::_key_evolution(&candidates[best].1) {
                        best = i;
                    }
                }
                Some(best)
            }
            Some(ev) if ev < 0 => {
                let mut ranked: Vec<i64> = candidates
                    .iter()
                    .map(|c| Self::_key_evolution(&c.1))
                    .collect();
                ranked.sort_unstable();
                ranked.dedup();
                let idx = ranked.len() as i64 + ev;
                if idx < 0 {
                    return None;
                }
                let wanted = ranked[idx as usize];
                candidates
                    .iter()
                    .position(|c| Self::_key_evolution(&c.1) == wanted)
            }
            Some(_) => Some(0),
        }
    }

    /// The per-entry read coroutine of `_parallel_individual_fetch`.
    async fn _remember_one(eid: &Arg, p: &Pool) -> Result<Arg> {
        let reference = match eid {
            Arg::Entry(e) => e.global_id(),
            other => other.__str__(),
        };
        let (key, raw) = Self::_resolve_entry_key_async(p, &reference).await?;
        // Extend the resolve chain with the *concrete* key so a floating
        // reference (`@evolution=-1`) cannot hide a cycle through a
        // nested manifest / constitution.
        use crate::policy::central::command::schema::parking::{
            resolve_chain_with, _RESOLVE_CHAIN,
        };
        let token = _RESOLVE_CHAIN.set(resolve_chain_with(&[key.as_str()]));
        let out = Self::_remember_one_on_chain(&key, raw, eid, p).await;
        _RESOLVE_CHAIN.reset(token);
        out
    }

    async fn _remember_one_on_chain(key: &str, raw: Arg, eid: &Arg, p: &Pool) -> Result<Arg> {
        let key = String::from(key);
        let raw = if raw.is_none() {
            p._read_through_async(&key).await?
        } else {
            raw
        };
        if raw.is_none() {
            return Err(LailaError::key_error(crate::python::pyrepr::str_repr(
                &alloc::format!(
                    "Entry {} not found in pool {}",
                    eid.__str__(),
                    p.global_id()
                ),
            )));
        }
        let record = Record::_build_async(raw).await?;
        let entry = record.__getitem__("entry")?;
        Ok(Self::_stamp_source_pool(
            Self::_check_record_identity(&key, entry)?,
            p,
        ))
    }

    /// Remember which pool *entry* came from (`Entry._source_pool_gid`).
    pub fn _stamp_source_pool(entry: Arg, pool: &Pool) -> Arg {
        if let Arg::Entry(e) = &entry {
            e.set_source_pool_gid(Some(pool.global_id()));
        }
        entry
    }

    /// Require the rebuilt *entry* to be the one *key* names: compares the
    /// `<scopes>:<uuid>` head, and the evolution when the key carries one
    /// (constant keys have none). `KeyError` on a mismatch.
    pub fn _check_record_identity(key: &str, entry: Arg) -> Result<Arg> {
        let Ok((key_head, key_attrs)) = split_global_id_attributes(key) else {
            return Ok(entry);
        };
        let Arg::Entry(e) = &entry else {
            return Ok(entry);
        };
        let head = to_global_id(Some(&e.uuid()), Some(&e.scopes_vec()), None, None);
        let mut mismatch = head != key_head;
        let key_evolution = key_attrs
            .iter()
            .find(|(k, _)| k == EVOLUTION_ATTRIBUTE)
            .map(|(_, v)| v.clone());
        if let Some(kev) = key_evolution {
            if !mismatch && !kev.starts_with('-') {
                mismatch = e.evolution().__str__() != kev;
            }
        }
        if mismatch {
            return Err(LailaError::key_error(crate::python::pyrepr::str_repr(
                &alloc::format!(
                    "record identity mismatch: key {} holds entry {}",
                    crate::python::pyrepr::str_repr(key),
                    crate::python::pyrepr::str_repr(&e.global_id())
                ),
            )));
        }
        Ok(entry)
    }

    /// Fetch *entry_ids* directly (no per-entry futures): the list of
    /// entries in the order of *entry_ids*, as one settled future
    /// (`Manifest.realized` / `async_realized` consume it).
    #[pysig("_read_entries_direct(entry_ids, *, pool=None, pool_id=None, pool_nickname=None, persist=True)")]
    pub fn _read_entries_direct(&self, args: impl Into<Args>) -> Result<Future> {
        let entry_ids = ensure_list(entry_ids).__iter__()?;
        let routed = self._route_pool(&pool, &pool_id, &pool_nickname)?;
        let memory = self.clone();
        let body = crate::python::stdlib::asyncio::coroutine_function(
            "_read_entries_async",
            move |_a: Args| {
                let (ids, p, persist, memory) = (
                    entry_ids.clone(),
                    routed.clone(),
                    persist.__bool__(),
                    memory.clone(),
                );
                async move { memory._read_entries_on(ids, p, persist).await }
            },
        );
        self._submit_internal(alloc::vec![body])
    }

    /// Fetch *entry_ids* by awaiting the pool directly -- no per-entry
    /// futures: a coroutine (await it) yielding the entries in the order
    /// of *entry_ids*. Semantics match `remember`: same pool routing,
    /// every id is checked against the resolve chain (cyclic constitutions
    /// raise `CyclicDependencyError`), and with `persist=True` on a
    /// non-alpha pool the READY entries are cached into the alpha pool
    /// before returning. `KeyError` when an id is missing.
    #[pysig("_read_entries_async(entry_ids, *, pool=None, pool_id=None, pool_nickname=None, persist=True, max_concurrency=None)")]
    pub fn _read_entries_async(&self, args: impl Into<Args>) -> Result<Arg> {
        let _ = max_concurrency;
        let entry_ids = ensure_list(entry_ids).__iter__()?;
        let routed = self._route_pool(&pool, &pool_id, &pool_nickname)?;
        crate::logger::hook_remember(
            entry_ids.clone().into_arg(),
            routed.clone().into_arg(),
            self._policy().into_arg(),
        );
        let (memory, persist) = (self.clone(), persist.__bool__());
        Ok(crate::python::stdlib::asyncio::coroutine(async move {
            memory._read_entries_on(entry_ids, routed, persist).await
        }))
    }

    /// The body of `_read_entries_async` on an already routed pool.
    async fn _read_entries_on(
        &self,
        entry_ids: Vec<Arg>,
        routed: Pool,
        persist: bool,
    ) -> Result<Arg> {
        use crate::policy::central::command::schema::parking::check_resolve_cycle;
        if entry_ids.is_empty() {
            return Ok(Vec::<Arg>::new().into_arg());
        }
        if routed.batch_accelerated() {
            return Err(LailaError::not_implemented(
                "batch-accelerated pools are not supported by _read_entries_async",
            ));
        }
        let ids: Vec<String> = entry_ids
            .iter()
            .map(|e| match e {
                Arg::Entry(e) => e.global_id(),
                other => other.__str__(),
            })
            .collect();
        check_resolve_cycle(&ids.iter().map(String::as_str).collect::<Vec<_>>())?;
        let mut out: Vec<Arg> = Vec::with_capacity(ids.len());
        for eid in &entry_ids {
            out.push(Self::_remember_one(eid, &routed).await?);
        }
        if persist && routed.global_id() != self.alpha_pool().__str__() {
            let ready: Vec<Arg> = out
                .iter()
                .filter(
                    |e| matches!(e, Arg::Entry(e) if e.state() == crate::entry::EntryState::READY),
                )
                .cloned()
                .collect();
            if !ready.is_empty() {
                let mut a = Args::new();
                a.push_keyword("entries", ready);
                a.push_keyword("pool_id", self.alpha_pool());
                let r#ref = self.memorize(a)?;
                let waited = r#ref.__await__().await;
                r#ref.release(Args::new())?;
                waited?;
            }
        }
        Ok(out.into_arg())
    }

    // ------------------------------------------------------------------
    // Cross-peer (over-the-wire) memorize / remember / forget
    // ------------------------------------------------------------------

    /// `[x.global_id if hasattr(x, "global_id") else str(x) for x in xs]`
    fn _gids_of(xs: &Arg) -> Result<Vec<Arg>> {
        let mut out = Vec::new();
        for x in ensure_list(xs.clone()).__iter__()? {
            out.push(match x.__getattr__("global_id") {
                Ok(g) => g,
                Err(_) => Arg::Str(x.__str__().into()),
            });
        }
        Ok(out)
    }
    fn _wait_60(fut: &Future) -> Result<Arg> {
        let mut a = Args::new();
        a.push_keyword("timeout", 60.0);
        fut.wait(a)
    }
    /// [`Memory::_wait_60`] for a dynamically produced value: a future is
    /// awaited, anything else (a stand-in's plain answer) is used as is.
    fn _wait_60_arg(v: &Arg) -> Result<Arg> {
        match v {
            Arg::Future(f) => Self::_wait_60(f),
            other if crate::python::builtins::hasattr(other, "wait") => {
                let mut a = Args::new();
                a.push_keyword("timeout", 60.0);
                other.__call_method__("wait", a)
            }
            other => Ok(other.clone()),
        }
    }

    /// Reconstruct wire-serialized entries and memorize them locally.
    ///
    /// Invoked over a transport by a peer's `laila.memorize(dst_policy=..)`
    /// call. Each item is the JSON-safe form produced by
    /// `Entry.serialize(transformation_base64)` on the sender; here it is
    /// rebuilt into a live `Entry`, written through the normal
    /// [`Memory::memorize`] path, and the call blocks until the write
    /// completes so the caller gets a definite list of stored gids back.
    /// Only string pool selectors are accepted (a pool object cannot
    /// cross the wire).
    #[pysig("_remote_memorize(serialized_entries, *, pool=None, pool_id=None, pool_nickname=None)")]
    pub fn _remote_memorize(&self, args: impl Into<Args>) -> Result<Arg> {
        let mut entries: Vec<Arg> = Vec::new();
        for s in serialized_entries.__iter__()? {
            let mut a = Args::new();
            a.push_positional(s);
            a.push_keyword("asynchronous", false);
            entries.push(crate::entry::constitution::build_maps::build_by_scope(a)?);
        }
        let mut a = Args::new();
        a.push_positional(entries.clone().into_arg());
        a.push_keyword("pool", pool);
        a.push_keyword("pool_id", pool_id);
        a.push_keyword("pool_nickname", pool_nickname);
        let fut = self.memorize(a)?;
        Self::_wait_60(&fut)?;
        let mut gids = Vec::new();
        for e in &entries {
            gids.push(e.__getattr__("global_id")?);
        }
        Ok(gids.into_arg())
    }

    /// Fetch entries locally and return them wire-serialized -- the
    /// inverse of [`Memory::_remote_memorize`]. `persist` is the caller's
    /// `laila.remember(..., persist=)` flag: with `True` the entries are
    /// also cached back into *this* (the peer's) alpha pool.
    #[pysig("_remote_remember(entry_ids, *, pool=None, pool_id=None, pool_nickname=None, persist=False)")]
    pub fn _remote_remember(&self, args: impl Into<Args>) -> Result<Arg> {
        let mut a = Args::new();
        a.push_positional(entry_ids);
        a.push_keyword("pool", pool);
        a.push_keyword("pool_id", pool_id);
        a.push_keyword("pool_nickname", pool_nickname);
        a.push_keyword("persist", persist.__bool__());
        // Through the dynamic layer: an instance-bound `remember` stand-in
        // (`object.__setattr__(mem, "remember", spy)`) sees the call.
        let fut = self.__call_method__("remember", a)?;
        let result = Self::_wait_60_arg(&fut)?;
        let entries = match result {
            Arg::List(_) => result.__iter__()?,
            other => alloc::vec![other],
        };
        let mut out = Vec::new();
        for e in entries {
            let mut a = Args::new();
            a.push_keyword(
                "transformations",
                crate::entry::compdata::transformation::transformation_base64(),
            );
            out.push(e.__call_method__("serialize", a)?);
        }
        Ok(out.into_arg())
    }

    /// Delete *entry_ids* from a local pool on behalf of a peer; blocks
    /// until the delete completes and returns the gids it asked to delete.
    #[pysig("_remote_forget(entry_ids, *, pool=None, pool_id=None, pool_nickname=None)")]
    pub fn _remote_forget(&self, args: impl Into<Args>) -> Result<Arg> {
        let ids = Self::_gids_of(&entry_ids)?;
        let mut a = Args::new();
        a.push_positional(ids.clone().into_arg());
        a.push_keyword("pool", pool);
        a.push_keyword("pool_id", pool_id);
        a.push_keyword("pool_nickname", pool_nickname);
        let fut = self.forget(a)?;
        Self::_wait_60(&fut)?;
        Ok(ids.into_arg())
    }

    // ------------------------------------------------------------------
    // 3-party relays (source-side orchestration)
    //
    // These run on the *source* policy B when an orchestrator A issues a
    // ``src_policy=B, dst_policy=C`` transfer. A reaches B over the A<->B
    // link and asks B to move data over B's own B<->C link -- A never
    // brokers the B<->C connection.
    // ------------------------------------------------------------------

    /// Push entries this policy holds to *dst_policy* (push relay): read
    /// *entry_ids* from this policy's `src_pool` and memorize them into
    /// `dst_policy`'s `dst_pool` over this policy's own peer link.
    #[pysig(
        "_relay_memorize(entry_ids, *, src_pool=None, dst_policy=None, dst_pool=None, comm=None)"
    )]
    pub fn _relay_memorize(&self, args: impl Into<Args>) -> Result<Arg> {
        let ids = Self::_gids_of(&entry_ids)?;
        let mut a = Args::new();
        a.push_positional(ids.into_arg());
        a.push_keyword("pool", src_pool);
        a.push_keyword("persist", false);
        let fetched = self.remember(a)?;
        let result = Self::_wait_60(&fetched)?;
        let entries = match result {
            Arg::List(_) => result.__iter__()?,
            other => alloc::vec![other],
        };
        let mut a = Args::new();
        a.push_positional(entries.clone().into_arg());
        a.push_keyword("dst_policy", dst_policy);
        a.push_keyword("dst_pool", dst_pool);
        a.push_keyword("comm", comm);
        let pushed = crate::laila.memorize(a)?;
        pushed.wait_timeout(60.0)?;
        let mut gids = Vec::new();
        for e in &entries {
            gids.push(e.__getattr__("global_id")?);
        }
        Ok(gids.into_arg())
    }

    /// Pull entries from *dst_policy* into this policy (pull relay): fetch
    /// *entry_ids* from `dst_policy`'s `dst_pool` over this policy's own
    /// link, store them into `src_pool` (or the alpha pool) and return the
    /// stored gids.
    #[pysig("_relay_remember(entry_ids, *, dst_policy=None, dst_pool=None, src_pool=None, comm=None, persist=True)")]
    pub fn _relay_remember(&self, args: impl Into<Args>) -> Result<Arg> {
        let ids = Self::_gids_of(&entry_ids)?;
        let mut a = Args::new();
        a.push_positional(ids.into_arg());
        a.push_keyword("dst_policy", dst_policy);
        a.push_keyword("dst_pool", dst_pool);
        a.push_keyword("comm", comm);
        a.push_keyword("persist", false);
        let fetched = crate::laila.remember(a)?;
        let result = fetched.wait_timeout(60.0)?;
        let entries = match result {
            Arg::List(_) => result.__iter__()?,
            other => alloc::vec![other],
        };
        if persist.__bool__() {
            let mut a = Args::new();
            a.push_positional(entries.clone().into_arg());
            a.push_keyword("pool", src_pool);
            let stored = self.memorize(a)?;
            Self::_wait_60(&stored)?;
        }
        let mut gids = Vec::new();
        for e in &entries {
            gids.push(e.__getattr__("global_id")?);
        }
        Ok(gids.into_arg())
    }

    /// Delete *entry_ids* from the routed pool (pool-local).
    #[pysig("forget(entry_ids, *, pool=None, pool_id=None, pool_nickname=None, affinity=None)")]
    pub fn forget(&self, args: impl Into<Args>) -> Result<Future> {
        let _ = affinity;
        let entry_ids = ensure_list(entry_ids);
        let routed = self._route_pool(&pool, &pool_id, &pool_nickname)?;
        crate::logger::hook_forget(
            entry_ids.clone(),
            routed.clone().into_arg(),
            self._policy().into_arg(),
        );
        self._delete(entry_ids, routed)
    }

    /// Delete each entry as one per-entry coroutine on the internal
    /// taskforce: the single future (one id) or a `GroupFuture`.
    pub fn _parallel_individual_delete(&self, entry_ids: Arg, pool: Pool) -> Result<Future> {
        let factories: Vec<Arg> = entry_ids
            .__iter__()?
            .into_iter()
            .map(|eid| {
                let p = pool.clone();
                crate::python::stdlib::asyncio::coroutine_function(
                    "_delete_one",
                    move |_a: Args| {
                        let (eid, p) = (eid.clone(), p.clone());
                        async move {
                            let mut key = match &eid {
                                Arg::Entry(e) => e.global_id(),
                                other => other.__str__(),
                            };
                            // A negative evolution ("@evolution=-1" = latest)
                            // must be resolved to the concrete key; an
                            // evolution-less reference stays exact.
                            let floating = split_global_id_attributes(&key)
                                .ok()
                                .and_then(|(_, attrs)| {
                                    attrs
                                        .into_iter()
                                        .find(|(k, _)| k == EVOLUTION_ATTRIBUTE)
                                        .map(|(_, v)| v.starts_with('-'))
                                })
                                .unwrap_or(false);
                            if floating {
                                key = Self::_resolve_entry_key_async(&p, &key).await?.0;
                            }
                            p.delete_async(&key).await?;
                            Ok(Arg::Str(key.into()))
                        }
                    },
                )
            })
            .collect();
        self._submit_internal(factories)
    }

    /// `memory.<name>` for the dynamic layer (an instance attribute first,
    /// then the model's; a method name reads as a bound method).
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        if let Some(v) = self.state_arc().attrs.get(name) {
            return Ok(v);
        }
        match name {
            "pool_router" => Ok(self.pool_router().into_arg()),
            "alpha_pool" => Ok(self.alpha_pool()),
            "global_id" => Ok(Arg::Str(self._owner_gid().into())),
            "scopes" => Ok(alloc::vec![Arg::Str(
                crate::macros::strings::_CENTRAL_MEMORY_SCOPE.into()
            )]
            .into_arg()),
            other if MEMORY_METHODS.contains(&other) => {
                let this = self.clone();
                let n = String::from(other);
                Ok(crate::python::callable(move |a: Args| {
                    this.__class_method__(&n, a)
                }))
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'_LAILA_IDENTIFIABLE_CENTRAL_MEMORY' object has no attribute '{other}'"
            ))),
        }
    }
    /// `object.__setattr__(memory, name, value)`: an instance attribute
    /// (a patched verb shadows the model's method).
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        self.state_arc().attrs.__setattr__(name, value)
    }
    /// `object.__delattr__(memory, name)`.
    pub fn __delattr__(&self, name: &str) -> Result<()> {
        self.state_arc().attrs.__delattr__(name)
    }
    /// `memory.<name>(*args, **kwargs)` for the dynamic layer.
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        if let Some(v) = self.state_arc().attrs.get(name) {
            return v.__call__(args);
        }
        self.__class_method__(name, args)
    }
    /// The model's own `<name>` (instance attributes not consulted): what a
    /// bound method read before patching keeps calling.
    fn __class_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "_policy" => Ok(self._policy().into_arg()),
            "_command" => Ok(self._policy().command().into_arg()),
            "_owner_gid" => Ok(Arg::Str(self._owner_gid().into())),
            "extend" => self.extend(args).map(|()| Arg::None),
            "memorize" => self.memorize(args).map(Arg::Future),
            "remember" => self.remember(args).map(Arg::Future),
            "forget" => self.forget(args).map(Arg::Future),
            "_remote_memorize" => self._remote_memorize(args),
            "_remote_remember" => self._remote_remember(args),
            "_remote_forget" => self._remote_forget(args),
            "_relay_memorize" => self._relay_memorize(args),
            "_relay_remember" => self._relay_remember(args),
            "_read_entries_direct" => self._read_entries_direct(args).map(Arg::Future),
            "_read_entries_async" => self._read_entries_async(args),
            "_duplicate_pool" => self._duplicate_pool(args).map(Arg::Future),
            "_record" | "_fetch" | "_delete" => {
                let first = args.positional.first().cloned().unwrap_or_else(|| {
                    args.get(if name == "_record" {
                        "entries"
                    } else {
                        "entry_ids"
                    })
                });
                let pool = args
                    .positional
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| args.get("pool"));
                if name == "_fetch" && args.get("borrow").__bool__() {
                    return Err(LailaError::not_implemented(
                        "borrow=True is not implemented for _fetch.",
                    ));
                }
                let pool = self._resolve_pool_ref(&pool)?;
                match name {
                    "_record" => self._record(first, pool),
                    "_fetch" => self._fetch(first, pool),
                    _ => self._delete(first, pool),
                }
                .map(Arg::Future)
            }
            "_resolve_pool_ref" => self
                ._resolve_pool_ref(&args.positional.first().cloned().unwrap_or(Arg::None))
                .map(Arg::Pool),
            // `with memory.borrow(): ..` -- planned, not implemented (the
            // Python twin raises on entry; see [`Memory::borrow`]).
            "borrow" => self.borrow(args)?.__enter__(),
            // `await memory._resolve_entry_key_async(pool, eid)` -> `(key, raw)`.
            "_resolve_entry_key_async" => {
                let pool = args
                    .positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| args.get("pool"));
                let eid = args
                    .positional
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| args.get("eid"))
                    .__str__();
                let pool = self._resolve_pool_ref(&pool)?;
                Ok(crate::python::stdlib::asyncio::coroutine(async move {
                    let (key, raw) = Self::_resolve_entry_key_async(&pool, &eid).await?;
                    Ok(Arg::Tuple(alloc::sync::Arc::from(alloc::vec![
                        Arg::Str(key.into()),
                        raw
                    ])))
                }))
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'_LAILA_IDENTIFIABLE_CENTRAL_MEMORY' object has no attribute '{other}'"
            ))),
        }
    }
}

impl core::fmt::Debug for Memory {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "<central memory of {}>", self._owner_gid())
    }
}
impl PyObject for Memory {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_LAILA_IDENTIFIABLE_CENTRAL_MEMORY"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Memory>()
            .is_some_and(|o| o._policy().is(&self._policy()))
    }
    /// One central memory per policy: two handles over the same policy
    /// are the same object (`laila.memory is policy.central.memory`).
    fn __is__(&self, other: &dyn PyObject) -> bool {
        self.__eq__(other)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self._policy().0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Memory::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        Memory::__setattr__(self, name, value)
    }
    fn __delattr__(&self, name: &str) -> Result<()> {
        Memory::__delattr__(self, name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Memory::__call_method__(self, name, args)
    }
    /// The `laila.args.environment` mirror fields (`pool_router` is walked
    /// by `build_environment` itself; the memory has no identity of its
    /// own in the port, `global_id` is the owning policy's).
    fn __cli_fields__(&self) -> Option<Vec<(String, Arg)>> {
        Some(alloc::vec![
            (String::from("alpha_pool"), self.alpha_pool()),
            (
                String::from("global_id"),
                Arg::Str(self._owner_gid().into())
            ),
        ])
    }
}
impl IntoArg for Memory {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Memory {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

/// The verbs `Memory::__call_method__` answers (a read of one is a bound
/// method).
const MEMORY_METHODS: &[&str] = &[
    "_policy",
    "_command",
    "_owner_gid",
    "extend",
    "memorize",
    "remember",
    "forget",
    "_remote_memorize",
    "_remote_remember",
    "_remote_forget",
    "_relay_memorize",
    "_relay_remember",
    "_read_entries_direct",
    "_read_entries_async",
    "_duplicate_pool",
    "_record",
    "_fetch",
    "_delete",
    "_resolve_pool_ref",
    "borrow",
    "_resolve_entry_key_async",
];
