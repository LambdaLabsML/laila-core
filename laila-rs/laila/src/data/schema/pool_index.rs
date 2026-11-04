//! Per-pool evolution / creation-timestamp index.
//!
//! A [`PoolIndex`] answers, for one *base* global id
//! (`LAILA:ENTRY:<uuid>`, no `@` suffix), the questions `laila.remember`
//! asks when a reference carries search attributes:
//!
//! - which evolutions are stored (and whether a constant key exists), so
//!   `@evolution=-1` (latest) / `-k` resolve without listing keys;
//! - which evolution was created at a given `creation_timestamp`, so
//!   `@creation_timestamp=<iso>` resolves without reading every record.
//!
//! Layout
//! ------
//! One **shard per base**. A shard is a *constant* `Entry` with scope
//! `POOL_INDEX` and the deterministic, stable storage key
//! `LAILA:POOL_INDEX:<uuid5("pool_index:<index identity>:<base>")>` whose
//! payload is:
//!
//! ```text
//! {"base": base, "evolutions": [0, 1, 2], "constant": False,
//!  "creation_timestamps": {"2026-...": 2, ...}}
//! ```
//!
//! Shards live in `owner.index_pool` (default: the owner pool itself;
//! point it at an in-memory pool for cheap index writes). Because the key
//! is constant, a shard is rewritten in place: no listing, no straggler
//! cleanup, so maintaining a shard is O(size of that base's history) and
//! independent of the size of the pool. Shards are loaded on demand.
//!
//! Index identity
//! --------------
//! Shard ids are derived from the pool's **index identity**, not from the
//! transient `uuid` of the object: the identity is persisted in the owner
//! pool under the reserved key [`INDEX_IDENTITY_KEY`] the first time the
//! index is used, and adopted by every later pool instance that opens the
//! same backing store. Reopening a file therefore finds its shards again
//! (no orphaned shards, no silent fall back to key scans). In-memory pools
//! never persist anything and keep using their `uuid`.
//!
//! Legacy layout
//! -------------
//! Earlier releases stored shards as *evolvable* entries
//! (`<shard id>@evolution=N`). The first time an index touches a store it
//! migrates such shards to the constant layout (one key listing per pool
//! instance, performed lazily and only when a legacy shard may exist).
//!
//! Consistency model
//! -----------------
//! The index is a **validated cache, never an authority**. Every hit is
//! confirmed by the read that follows it in central memory; a miss
//! invalidates the base and falls back to the key scan. A failed shard
//! write logs, invalidates the base, and never fails the user's
//! `memorize`. [`PoolIndex::rebuild`] repopulates every shard from the
//! owner's keys.
//!
//! Maintenance is **write-through**: the pool's `write` / `delete`
//! wrappers call [`record`](PoolIndex::record) / [`remove`](PoolIndex::remove),
//! which update the shard in memory and persist it immediately, all under
//! the owner's atomic lock. Shard keys are never indexed themselves and
//! are hidden from the pool's public `keys()`.

use alloc::string::{String, ToString};
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};

use crate::basics::definitions::identifiable_object::{
    split_global_id_attributes, EVOLUTION_ATTRIBUTE,
};
use crate::entry::{Entry, EntryState};
use crate::macros::strings::{_POOL_INDEX_SCOPE, _TOPMOST_SCOPE};
use crate::policy::central::memory::record::record::Record;
use crate::python::object::PyObject;
use crate::python::sync::RwLock;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, PyDictBuilder, Result};

use super::base::{Pool, PoolObject};

/// `f"{_TOPMOST_SCOPE}:{_POOL_INDEX_SCOPE}:"` -- `"LAILA:POOL_INDEX:"`.
pub const _INDEX_KEY_PREFIX: &str = "LAILA:POOL_INDEX:";
/// `f"{_TOPMOST_SCOPE}:"` -- `"LAILA:"`.
pub const _LAILA_KEY_PREFIX: &str = "LAILA:";

/// Name of the search attribute keyed by an entry's creation stamp.
pub const CREATION_TIMESTAMP_ATTRIBUTE: &str = "creation_timestamp";

/// Reserved (hidden) key under which a pool persists its index identity:
/// `f"{_INDEX_KEY_PREFIX}{uuid.uuid5(uuid.NAMESPACE_URL, 'laila:pool_index:identity')}"`.
/// Derived from a fixed namespace so it does not depend on the active
/// laila namespace: the same file must find its identity whoever reopens it.
pub const INDEX_IDENTITY_KEY: &str = "LAILA:POOL_INDEX:906aeac9-9c7c-5136-81d0-d208d867d122";

/// `uuid.NAMESPACE_URL`
pub const NAMESPACE_URL: [u8; 16] = [
    0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
];

const _: () = {
    // The two prefixes are spelled out above so they are `const`; this
    // pins them to the scope constants they are built from.
    assert!(_TOPMOST_SCOPE.len() + 1 + _POOL_INDEX_SCOPE.len() + 1 == _INDEX_KEY_PREFIX.len());
    assert!(_TOPMOST_SCOPE.len() + 1 == _LAILA_KEY_PREFIX.len());
};

/// `True` for storage keys that belong to an index shard (or the identity record).
pub fn index_key(key: &str) -> bool {
    key.starts_with(_INDEX_KEY_PREFIX)
}

/// `True` for keys central memory manages (`LAILA:...`); raw user keys are not indexed.
pub fn laila_key(key: &str) -> bool {
    key.starts_with(_LAILA_KEY_PREFIX)
}

/// `is_index_key(key)` -- Python-shaped (`False` for a non-string key).
#[pysig("is_index_key(key)")]
pub fn is_index_key(args: impl Into<Args>) -> Result<bool> {
    Ok(matches!(&key, Arg::Str(s) if index_key(s)))
}

/// `is_laila_key(key)` -- Python-shaped (`False` for a non-string key).
#[pysig("is_laila_key(key)")]
pub fn is_laila_key(args: impl Into<Args>) -> Result<bool> {
    Ok(matches!(&key, Arg::Str(s) if laila_key(s)))
}

/// Pull the entry `creation_timestamp` out of a stored record without
/// rebuilding it.
///
/// *raw* is whatever the pool holds: a JSON string / bytes, a record dict
/// whose `entry` is a serialized dict, or (in-memory pools with no
/// transformations) a record dict whose `entry` is a live `Entry`.
pub fn _record_creation_timestamp(raw: &Arg) -> Arg {
    let mut raw = raw.clone();
    if let Arg::Bytes(b) = &raw {
        raw = Arg::Str(String::from_utf8_lossy(b).into_owned().into());
    } else if let Arg::ByteArray(b) = &raw {
        let text = String::from_utf8_lossy(&b.read()).into_owned();
        raw = Arg::Str(text.into());
    }
    if let Arg::Str(s) = &raw {
        raw = match crate::_compat::pyjson::loads(s) {
            Ok(v) => v,
            Err(_) => return Arg::None,
        };
    }
    if !matches!(raw, Arg::Dict(_)) {
        return raw.__getattr__("creation_timestamp").unwrap_or(Arg::None);
    }
    let entry = raw
        .get(Args::from(Arg::Str("entry".into())))
        .ok()
        .filter(|e| !e.is_none())
        .unwrap_or(raw);
    if let Arg::Dict(_) = &entry {
        return entry
            .get(Args::from(Arg::Str("_creation_timestamp".into())))
            .unwrap_or(Arg::None);
    }
    entry.__getattr__("creation_timestamp").unwrap_or(Arg::None)
}

/// Evolution encoded in a storage key; `None` for a constant (no `@`).
pub fn _key_evolution(key: &str) -> Option<i64> {
    let (_, attrs) = split_global_id_attributes(key).ok()?;
    let raw = attrs
        .iter()
        .find(|(k, _)| k == EVOLUTION_ATTRIBUTE)
        .map(|(_, v)| v.as_str())?;
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    raw.parse().ok()
}

pub fn _evolution_key(base: &str, evolution: Option<i64>) -> String {
    match evolution {
        None => base.to_string(),
        Some(e) => alloc::format!("{base}@{EVOLUTION_ATTRIBUTE}={e}"),
    }
}

/// Sort rank: constants (no evolution) sort below every evolution.
pub fn _rank(evolution: Option<i64>) -> i64 {
    evolution.unwrap_or(-1)
}

fn opt_evo(a: &Arg) -> Option<i64> {
    match a {
        Arg::None => None,
        other => other.as_i64(),
    }
}

fn evo_arg(e: Option<i64>) -> Arg {
    match e {
        None => Arg::None,
        Some(e) => Arg::Int(e),
    }
}

fn s(text: &str) -> Arg {
    Arg::Str(text.into())
}

struct PoolIndexInner {
    _owner: Weak<PoolObject>,
    /// `base -> shard dict` (live dicts: `_shards[base]["evolutions"]` is the list).
    _shards: Arg,
    /// `base -> live shard Entry`
    _entries: Arg,
    /// bases known to have no shard
    _missing: RwLock<Vec<String>>,
    _identity: RwLock<Option<String>>,
    _migrated: AtomicBool,
}

/// Shard-per-base evolution / creation-timestamp index of one pool.
///
/// Created lazily by `pool.index`. All methods take the owner's atomic
/// lock (re-entrant), so callers may hold it already. A handle: clones
/// alias one index.
#[derive(Clone)]
pub struct PoolIndex(Arc<PoolIndexInner>);

impl PoolIndex {
    /// `PoolIndex(owner)`: the pool whose keys are indexed
    /// (`owner.index_pool`, or the owner itself, stores the shards).
    #[pysig("PoolIndex(owner)")]
    pub fn new(args: impl Into<Args>) -> Result<PoolIndex> {
        match &owner {
            Arg::Pool(p) => Ok(PoolIndex::of(p)),
            other => Err(LailaError::type_error(alloc::format!(
                "PoolIndex owner must be a pool, got {}",
                other.type_name()
            ))),
        }
    }

    /// The index of *owner* (typed).
    pub fn of(owner: &Pool) -> PoolIndex {
        PoolIndex(Arc::new(PoolIndexInner {
            _owner: Arc::downgrade(&owner.0),
            _shards: crate::python::builtins::dict(Args::new()).expect("dict"),
            _entries: crate::python::builtins::dict(Args::new()).expect("dict"),
            _missing: RwLock::new(Vec::new()),
            _identity: RwLock::new(None),
            _migrated: AtomicBool::new(false),
        }))
    }

    /// `self._owner`
    pub fn _owner(&self) -> Result<Pool> {
        self.0
            ._owner
            .upgrade()
            .map(Pool)
            .ok_or_else(|| LailaError::runtime_error("PoolIndex: the owner pool was dropped"))
    }
    /// `self._shards`
    pub fn _shards(&self) -> Arg {
        self.0._shards.clone()
    }
    /// `self._entries`
    pub fn _entries(&self) -> Arg {
        self.0._entries.clone()
    }
    /// `self._missing` (a set snapshot).
    pub fn _missing(&self) -> Arg {
        let items: Vec<Arg> = self.0._missing.read().iter().map(|b| s(b)).collect();
        crate::python::builtins::set(Args::from(items.into_arg())).expect("set")
    }
    fn missing_add(&self, base: &str) {
        let mut g = self.0._missing.write();
        if !g.iter().any(|b| b == base) {
            g.push(base.to_string());
        }
    }
    fn missing_discard(&self, base: &str) {
        self.0._missing.write().retain(|b| b != base);
    }
    fn missing_contains(&self, base: &str) -> bool {
        self.0._missing.read().iter().any(|b| b == base)
    }
    /// `self._identity`
    pub fn _identity(&self) -> Arg {
        match &*self.0._identity.read() {
            Some(i) => s(i),
            None => Arg::None,
        }
    }
    /// `self._migrated`
    pub fn _migrated(&self) -> bool {
        self.0._migrated.load(Ordering::SeqCst)
    }
    /// Python `is`.
    pub fn is(&self, other: &PoolIndex) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    // ------------------------------------------------------------------
    // Identity helpers
    // ------------------------------------------------------------------

    /// Pool that stores the shards (`owner.index_pool` or the owner).
    pub fn index_pool(&self) -> Result<Pool> {
        let owner = self._owner()?;
        Ok(match owner.index_pool() {
            Arg::Pool(p) => p,
            _ => owner,
        })
    }

    /// Stable identity shard ids are derived from.
    ///
    /// Persistent pools (`_persistent_storage = True`) store it in the
    /// owner under [`INDEX_IDENTITY_KEY`] on first use and adopt the
    /// stored value on reopen, so a fresh pool instance finds the shards
    /// an earlier instance wrote. In-process pools use their `uuid` and
    /// persist nothing.
    pub fn identity(&self) -> Result<String> {
        if let Some(i) = self.0._identity.read().as_ref() {
            return Ok(i.clone());
        }
        let owner = self._owner()?;
        let _held = owner.held();
        if let Some(i) = self.0._identity.read().as_ref() {
            return Ok(i.clone());
        }
        let mut identity = owner.uuid();
        if owner._persistent_index_identity() {
            // the index is a cache: never break the caller
            let r: Result<()> = (|| {
                let mut stored = owner._read(INDEX_IDENTITY_KEY)?;
                if let Arg::Bytes(b) = &stored {
                    stored = Arg::Str(String::from_utf8_lossy(b).into_owned().into());
                }
                if let Arg::Str(text) = &stored {
                    stored = crate::_compat::pyjson::loads(text)?;
                }
                let found = match &stored {
                    Arg::Dict(_) => stored.get(Args::from(s("identity")))?,
                    _ => Arg::None,
                };
                if let Arg::Str(found) = found {
                    identity = found.to_string();
                } else {
                    let mut d = PyDictBuilder::new();
                    d.insert("identity", identity.as_str());
                    owner._write(INDEX_IDENTITY_KEY, d.finish())?;
                }
                Ok(())
            })();
            let _ = r; // ``_LOG.debug("pool index: could not persist identity: %s", exc)``
        }
        *self.0._identity.write() = Some(identity.clone());
        Ok(identity)
    }

    /// Deterministic, constant storage key of the shard for *base*.
    pub fn shard_id(&self, base: impl IntoArg) -> Result<String> {
        Ok(alloc::format!(
            "{_INDEX_KEY_PREFIX}{}",
            self._shard_uuid(&base.into_arg().__str__())?
        ))
    }

    /// `uuid5(identity, base)`: stable across processes and laila namespaces.
    pub fn _shard_uuid(&self, base: &str) -> Result<String> {
        let identity = self.identity()?;
        let namespace = match crate::_compat::uuid::parse(&identity) {
            Some(ns) => ns,
            // non-UUID identity (custom pools): hash it first
            None => crate::_compat::uuid::parse(&crate::_compat::uuid::uuid5(
                &NAMESPACE_URL,
                &alloc::format!("laila:pool_index:{identity}"),
            ))
            .expect("uuid5 is a valid uuid"),
        };
        Ok(crate::_compat::uuid::uuid5(&namespace, base))
    }

    // ------------------------------------------------------------------
    // Queries
    // ------------------------------------------------------------------

    /// Every stored key of *base*, or `None` when no shard exists.
    pub fn candidates(&self, base: impl IntoArg) -> Result<Arg> {
        let base = base.into_arg().__str__();
        let shard = self.shard_opt(&base, false)?;
        let Some(shard) = shard else {
            return Ok(Arg::None);
        };
        let mut keys: Vec<Arg> = Vec::new();
        if shard_constant(&shard)? {
            keys.push(s(&base));
        }
        for e in shard_evolutions(&shard)? {
            keys.push(s(&_evolution_key(&base, Some(e))));
        }
        Ok(keys.into_arg())
    }

    /// Key of the highest evolution (or the constant key), or `None`.
    pub fn latest(&self, base: impl IntoArg) -> Result<Arg> {
        self.nth(base, -1)
    }

    /// Key of the *n*-th evolution of *base*.
    ///
    /// `n >= 0` is an exact evolution; `n < 0` counts from the end of the
    /// sorted evolutions (`-1` = latest). A constant key is preferred for
    /// `n < 0` when it is the only thing stored, and counts as the lowest
    /// rank otherwise. `None` when out of range or unindexed.
    pub fn nth(&self, base: impl IntoArg, n: impl IntoArg) -> Result<Arg> {
        let base = base.into_arg().__str__();
        let n = n.into_arg();
        let n = n.as_i64().ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "'{}' object cannot be interpreted as an integer",
                n.type_name()
            ))
        })?;
        let Some(shard) = self.shard_opt(&base, false)? else {
            return Ok(Arg::None);
        };
        let evolutions = shard_evolutions(&shard)?;
        if n >= 0 {
            return Ok(if evolutions.contains(&n) {
                s(&_evolution_key(&base, Some(n)))
            } else {
                Arg::None
            });
        }
        let mut ranked: Vec<Option<i64>> = Vec::new();
        if shard_constant(&shard)? {
            ranked.push(None);
        }
        ranked.extend(evolutions.into_iter().map(Some));
        let idx = ranked.len() as i64 + n;
        if idx < 0 || idx >= ranked.len() as i64 {
            return Ok(Arg::None);
        }
        Ok(s(&_evolution_key(&base, ranked[idx as usize])))
    }

    /// Key of the evolution created at *timestamp* (exact match).
    ///
    /// With *evolution* given, the match must also be that evolution
    /// (negative values count from the end, as in [`nth`](Self::nth)).
    /// `None` when unindexed or no such stamp.
    #[pysig("by_creation_timestamp(base, timestamp, evolution=None)")]
    pub fn by_creation_timestamp(&self, args: impl Into<Args>) -> Result<Arg> {
        let base = base.__str__();
        let Some(shard) = self.shard_opt(&base, false)? else {
            return Ok(Arg::None);
        };
        let stamps = shard.__getitem__("creation_timestamps")?;
        if !stamps.__contains__(timestamp.clone())? {
            return Ok(Arg::None);
        }
        let found = opt_evo(&stamps.__getitem__(timestamp)?);
        let key = s(&_evolution_key(&base, found));
        if !evolution.is_none() && self.nth(base.as_str(), evolution)? != key {
            return Ok(Arg::None);
        }
        Ok(key)
    }

    // ------------------------------------------------------------------
    // Maintenance (write-through)
    // ------------------------------------------------------------------

    /// Index a key that was just written with record *value*, then
    /// persist the shard.
    ///
    /// Only `LAILA:` keys are indexed: raw user keys (`pool["k0"]`) carry
    /// no lineage and skipping them keeps a plain write at one backend
    /// write instead of two.
    pub fn record(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        let key = key.into_arg().__str__();
        let value = value.into_arg();
        if index_key(&key) || !laila_key(&key) {
            return Ok(());
        }
        let (base, _) = split_global_id_attributes(&key)?;
        let evolution = _key_evolution(&key);
        let stamp = _record_creation_timestamp(&value);
        let owner = self._owner()?;
        let _held = owner.held();
        let shard = self
            .shard_opt(&base, true)?
            .expect("create=True yields a shard");
        if let Some(e) = evolution {
            let evolutions = shard.__getitem__("evolutions")?;
            if !evolutions.__contains__(Arg::Int(e))? {
                evolutions.append(Arg::Int(e))?;
                evolutions.sort_in_place()?;
            }
        } else {
            shard.__setitem__("constant", Arg::Bool(true))?;
        }
        if !stamp.is_none() {
            let stamps = shard.__getitem__("creation_timestamps")?;
            // An evolution re-written with a different stamp: drop the stale one.
            for pair in stamps.items()?.__iter__()? {
                let (old, evo) = crate::python::builtins::unpack2(pair)?;
                if evo == evo_arg(evolution) && old != stamp {
                    stamps.__delitem__(old)?;
                }
            }
            // Same-millisecond collision: keep the highest evolution.
            let absent = s("__absent__");
            let mut g = Args::new();
            g.push_positional(stamp.clone());
            g.push_positional(absent.clone());
            let prev = stamps.get(g)?;
            if prev == absent || _rank(evolution) > _rank(opt_evo(&prev)) {
                stamps.__setitem__(stamp, evo_arg(evolution))?;
            }
        }
        self._flush(&base)
    }

    /// Un-index a key that was just deleted, then persist (or drop) the shard.
    pub fn remove(&self, key: impl IntoArg) -> Result<()> {
        let key = key.into_arg().__str__();
        if index_key(&key) || !laila_key(&key) {
            return Ok(());
        }
        let (base, _) = split_global_id_attributes(&key)?;
        let evolution = _key_evolution(&key);
        let owner = self._owner()?;
        let _held = owner.held();
        let Some(shard) = self.shard_opt(&base, false)? else {
            return Ok(());
        };
        match evolution {
            None => shard.__setitem__("constant", Arg::Bool(false))?,
            Some(e) => {
                let evolutions = shard.__getitem__("evolutions")?;
                if evolutions.__contains__(Arg::Int(e))? {
                    evolutions.remove(Arg::Int(e))?;
                }
            }
        }
        let stamps = shard.__getitem__("creation_timestamps")?;
        let mut kept = PyDictBuilder::new();
        for pair in stamps.items()?.__iter__()? {
            let (ts, evo) = crate::python::builtins::unpack2(pair)?;
            if evo != evo_arg(evolution) {
                kept.insert(ts, evo);
            }
        }
        shard.__setitem__("creation_timestamps", kept.finish())?;
        if !shard_constant(&shard)? && shard_evolutions(&shard)?.is_empty() {
            self._drop_shard(&base)
        } else {
            self._flush(&base)
        }
    }

    /// Forget the in-memory state of *base* (or of every base).
    ///
    /// The next query reloads the shard from the index pool; central
    /// memory calls this when an index hit fails validation.
    #[pysig("invalidate(base=None)")]
    pub fn invalidate(&self, args: impl Into<Args>) -> Result<()> {
        let owner = self._owner()?;
        let _held = owner.held();
        if base.is_none() {
            self.0._shards.clear(Args::new())?;
            self.0._entries.clear(Args::new())?;
            self.0._missing.write().clear();
        } else {
            self.forget(&base.__str__())?;
        }
        Ok(())
    }

    fn forget(&self, base: &str) -> Result<()> {
        let mut a = Args::new();
        a.push_positional(s(base));
        a.push_positional(Arg::None);
        self.0._shards.pop(a.clone())?;
        self.0._entries.pop(a)?;
        self.missing_discard(base);
        Ok(())
    }

    /// Delete every shard of the owner's current keys from the index pool.
    ///
    /// Used by `pool.empty()`: shards are addressed per base, so the
    /// owner's keys tell us which shards exist.
    pub fn clear(&self) -> Result<()> {
        let owner = self._owner()?;
        let _held = owner.held();
        let mut bases: Vec<String> = Vec::new();
        for k in owner.0._keys()? {
            if laila_key(&k) && !index_key(&k) {
                let (base, _) = split_global_id_attributes(&k)?;
                if !bases.contains(&base) {
                    bases.push(base);
                }
            }
        }
        for base in bases {
            self._drop_shard(&base)?;
        }
        self.invalidate(Args::new())
    }

    /// Rebuild every shard from the owner's keys and records.
    ///
    /// Reads each record once (for its creation stamp). Replaces the
    /// shard contents outright, so evolutions that no longer exist are
    /// dropped too.
    pub fn rebuild(&self) -> Result<()> {
        let owner = self._owner()?;
        let _held = owner.held();
        let mut by_base: Vec<(String, Vec<String>)> = Vec::new();
        for key in owner.0._keys()? {
            if index_key(&key) || !laila_key(&key) {
                continue;
            }
            let (base, _) = split_global_id_attributes(&key)?;
            match by_base.iter_mut().find(|(b, _)| *b == base) {
                Some((_, keys)) => keys.push(key),
                None => by_base.push((base, alloc::vec![key])),
            }
        }
        for (base, keys) in by_base {
            // load (to keep the shard entry's counter)
            self.shard_opt(&base, true)?;
            let shard = self.0._shards.__getitem__(s(&base))?;
            shard.__setitem__("evolutions", Vec::<Arg>::new().into_arg())?;
            shard.__setitem__("constant", Arg::Bool(false))?;
            shard.__setitem__("creation_timestamps", PyDictBuilder::new().finish())?;
            let evolutions = shard.__getitem__("evolutions")?;
            let stamps = shard.__getitem__("creation_timestamps")?;
            for key in keys {
                let evolution = _key_evolution(&key);
                match evolution {
                    None => shard.__setitem__("constant", Arg::Bool(true))?,
                    Some(e) => evolutions.append(Arg::Int(e))?,
                }
                let stamp = _record_creation_timestamp(&owner._read(&key)?);
                if !stamp.is_none() {
                    let absent = s("__absent__");
                    let mut g = Args::new();
                    g.push_positional(stamp.clone());
                    g.push_positional(absent.clone());
                    let prev = stamps.get(g)?;
                    if prev == absent || _rank(evolution) > _rank(opt_evo(&prev)) {
                        stamps.__setitem__(stamp, evo_arg(evolution))?;
                    }
                }
            }
            evolutions.sort_in_place()?;
            self._flush(&base)?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Shard storage
    // ------------------------------------------------------------------

    fn _new_shard(&self, base: &str) -> Arg {
        let mut d = PyDictBuilder::new();
        d.insert("base", base);
        d.insert("evolutions", Vec::<Arg>::new());
        d.insert("constant", false);
        d.insert("creation_timestamps", PyDictBuilder::new().finish());
        d.finish()
    }

    /// The in-memory shard for *base*, loading it from the index pool on
    /// first use (`None` unless *create*).
    #[pysig("_shard(base, create=False)")]
    pub fn _shard(&self, args: impl Into<Args>) -> Result<Arg> {
        Ok(self
            .shard_opt(&base.__str__(), create.__bool__())?
            .unwrap_or(Arg::None))
    }

    fn shard_opt(&self, base: &str, create: bool) -> Result<Option<Arg>> {
        let owner = self._owner()?;
        let _held = owner.held();
        if self.0._shards.__contains__(s(base))? {
            return Ok(Some(self.0._shards.__getitem__(s(base))?));
        }
        if !self.missing_contains(base) {
            if let Some(loaded) = self._load(base)? {
                return Ok(Some(loaded));
            }
            self.missing_add(base);
        }
        if !create {
            return Ok(None);
        }
        self.missing_discard(base);
        let shard = self._new_shard(base);
        self.0._shards.__setitem__(s(base), shard.clone())?;
        Ok(Some(shard))
    }

    /// Rebuild the in-memory shard (and cache its live Entry) from a stored record.
    fn _shard_from_raw(&self, base: &str, raw: &Arg) -> Result<Option<Arg>> {
        let entry = Record::_build_sync(raw.clone())?.__getitem__("entry")?;
        let Arg::Entry(entry) = entry else {
            return Ok(None);
        };
        let data = entry.data()?;
        if !matches!(data, Arg::Dict(_)) {
            return Ok(None);
        }
        let shard = self._new_shard(base);
        let mut evolutions: Vec<i64> = Vec::new();
        let mut a = Args::new();
        a.push_positional(s("evolutions"));
        a.push_positional(Vec::<Arg>::new());
        for e in data.get(a)?.__iter__()? {
            evolutions.push(crate::python::builtins::int(e)?);
        }
        evolutions.sort_unstable();
        shard.__setitem__(
            "evolutions",
            evolutions
                .into_iter()
                .map(Arg::Int)
                .collect::<Vec<_>>()
                .into_arg(),
        )?;
        let mut c = Args::new();
        c.push_positional(s("constant"));
        c.push_positional(false);
        shard.__setitem__("constant", Arg::Bool(data.get(c)?.__bool__()))?;
        let mut t = Args::new();
        t.push_positional(s("creation_timestamps"));
        t.push_positional(PyDictBuilder::new().finish());
        let raw_stamps = crate::python::builtins::dict(Args::from(data.get(t)?))?;
        let mut stamps = PyDictBuilder::new();
        for pair in raw_stamps.items()?.__iter__()? {
            let (ts, evo) = crate::python::builtins::unpack2(pair)?;
            let evo = if evo.is_none() {
                Arg::None
            } else {
                Arg::Int(crate::python::builtins::int(evo)?)
            };
            stamps.insert(ts.__str__(), evo);
        }
        shard.__setitem__("creation_timestamps", stamps.finish())?;
        self.0._shards.__setitem__(s(base), shard.clone())?;
        self.0._entries.__setitem__(s(base), Arg::Entry(entry))?;
        Ok(Some(shard))
    }

    /// Load the persisted shard of *base* from its constant key (one read, no listing).
    fn _load(&self, base: &str) -> Result<Option<Arg>> {
        let sid = self.shard_id(base)?;
        let r: Result<Option<Arg>> = (|| {
            let pool = self.index_pool()?;
            let raw = pool._read(sid.as_str())?;
            if raw.is_none() {
                // A persistent store written by an earlier release may still
                // hold legacy (evolvable) shards: clean them up once.
                self._ensure_migrated()?;
                return Ok(None);
            }
            self._shard_from_raw(base, &raw)
        })();
        // ``_LOG.warning("pool index: could not load shard for %s: %s", base, exc)``
        // -- the index is a cache: never let it break the caller.
        Ok(r.unwrap_or(None))
    }

    /// Run [`_migrate_legacy_shards`](Self::_migrate_legacy_shards) once,
    /// and only for persistent index pools.
    pub fn _ensure_migrated(&self) -> Result<()> {
        if self._migrated() {
            return Ok(());
        }
        if !self.index_pool()?._persistent_storage() {
            // in-process storage cannot hold legacy shards
            self.0._migrated.store(true, Ordering::SeqCst);
            return Ok(());
        }
        self._migrate_legacy_shards()
    }

    /// One-time purge of legacy `<shard>@evolution=N` shards.
    ///
    /// Earlier releases derived shard ids from the transient uuid of the
    /// pool *instance* that wrote them (and the active laila namespace),
    /// so shards found in a reopened store can never be attributed to the
    /// identity now in use: they are dead weight and are deleted. The
    /// index pool is listed once per [`PoolIndex`] instance and only when
    /// a shard read misses. Lineage information is rebuilt lazily by the
    /// write-through maintenance, or eagerly with [`rebuild`](Self::rebuild).
    pub fn _migrate_legacy_shards(&self) -> Result<()> {
        self.0._migrated.store(true, Ordering::SeqCst);
        let pool = self.index_pool()?;
        let r: Result<()> = (|| {
            let legacy: Vec<String> = pool
                .0
                ._keys()?
                .into_iter()
                .filter(|k| index_key(k) && k.contains('@'))
                .collect();
            for key in &legacy {
                let _ = pool._delete(key.as_str()); // best effort cleanup
            }
            // ``_LOG.info("pool index: removed %d legacy index shard(s)", len(legacy))``
            Ok(())
        })();
        let _ = r; // ``_LOG.warning("pool index: legacy shard cleanup failed: %s", exc)``
        Ok(())
    }

    /// Persist the in-memory shard of *base* in place under its constant key.
    pub fn _flush(&self, base: &str) -> Result<()> {
        if !self.0._shards.__contains__(s(base))? {
            return Ok(());
        }
        let shard = self.0._shards.__getitem__(s(base))?;
        let r: Result<()> = (|| {
            let pool = self.index_pool()?;
            // Fresh container objects: the in-memory pool stores a snapshot,
            // and the live shard dict must never alias what is stored.
            let mut payload = PyDictBuilder::new();
            payload.insert("base", shard.__getitem__("base")?);
            payload.insert(
                "evolutions",
                crate::python::builtins::list(shard.__getitem__("evolutions")?)?,
            );
            payload.insert("constant", shard.__getitem__("constant")?.__bool__());
            payload.insert(
                "creation_timestamps",
                crate::python::builtins::dict(Args::from(
                    shard.__getitem__("creation_timestamps")?,
                ))?,
            );
            let payload = payload.finish();
            let entry = if self.0._entries.__contains__(s(base))? {
                let Arg::Entry(e) = self.0._entries.__getitem__(s(base))? else {
                    return Err(LailaError::runtime_error("pool index: shard entry"));
                };
                e.set_data(payload)?;
                e
            } else {
                let mut a = Args::new();
                a.push_keyword("uuid", self._shard_uuid(base)?);
                a.push_keyword("scopes", alloc::vec![s(_POOL_INDEX_SCOPE)]);
                a.push_keyword("evolution", Arg::None); // constant: one stable key per base
                a.push_keyword("data", payload);
                a.push_keyword("state", EntryState::READY);
                let e = Entry::contingent(a)?;
                self.0
                    ._entries
                    .__setitem__(s(base), Arg::Entry(e.clone()))?;
                e
            };
            let mut ra = Args::new();
            ra.push_keyword("entry", Arg::Entry(entry.clone()));
            let record = Record::new(ra)?;
            let blob = Record::serialize_record(&record, pool.transformations())?;
            pool._write(self.shard_id(base)?.as_str(), blob)?;
            entry.mark_memorized();
            Ok(())
        })();
        if r.is_err() {
            // ``_LOG.warning("pool index: could not persist shard for %s: %s", base, exc)``
            self.invalidate(Args::from(s(base)))?;
        }
        Ok(())
    }

    /// Delete the persisted shard for *base* and forget it.
    pub fn _drop_shard(&self, base: &str) -> Result<()> {
        let r: Result<()> = (|| {
            let pool = self.index_pool()?;
            self._ensure_migrated()?;
            pool._delete(self.shard_id(base)?.as_str())
        })();
        let _ = r; // ``_LOG.warning("pool index: could not drop shard for %s: %s", base, exc)``
        let mut a = Args::new();
        a.push_positional(s(base));
        a.push_positional(Arg::None);
        self.0._shards.pop(a.clone())?;
        self.0._entries.pop(a)?;
        self.missing_add(base);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Python protocol
    // ------------------------------------------------------------------

    /// `index.<name>` for the dynamic layer.
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "_shards" => self._shards(),
            "_entries" => self._entries(),
            "_missing" => self._missing(),
            "_identity" => self._identity(),
            "_migrated" => Arg::Bool(self._migrated()),
            "_owner" => Arg::Pool(self._owner()?),
            "index_pool" => Arg::Pool(self.index_pool()?),
            "identity" => s(&self.identity()?),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'PoolIndex' object has no attribute '{name}'"
                )))
            }
        })
    }

    /// `index.<name>(*args, **kwargs)` for the dynamic layer.
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = || args.positional.first().cloned().unwrap_or(Arg::None);
        let second = || args.positional.get(1).cloned().unwrap_or(Arg::None);
        Ok(match name {
            "identity" => s(&self.identity()?),
            "index_pool" => Arg::Pool(self.index_pool()?),
            "shard_id" => s(&self.shard_id(first())?),
            "_shard_uuid" => s(&self._shard_uuid(&first().__str__())?),
            "candidates" => self.candidates(first())?,
            "latest" => self.latest(first())?,
            "nth" => self.nth(first(), second())?,
            "by_creation_timestamp" => self.by_creation_timestamp(args)?,
            "record" => {
                self.record(first(), second())?;
                Arg::None
            }
            "remove" => {
                self.remove(first())?;
                Arg::None
            }
            "invalidate" => {
                self.invalidate(args)?;
                Arg::None
            }
            "clear" => {
                self.clear()?;
                Arg::None
            }
            "rebuild" => {
                self.rebuild()?;
                Arg::None
            }
            "_shard" => self._shard(args)?,
            "_flush" => {
                self._flush(&first().__str__())?;
                Arg::None
            }
            "_drop_shard" => {
                self._drop_shard(&first().__str__())?;
                Arg::None
            }
            "_ensure_migrated" => {
                self._ensure_migrated()?;
                Arg::None
            }
            "_migrate_legacy_shards" => {
                self._migrate_legacy_shards()?;
                Arg::None
            }
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'PoolIndex' object has no attribute '{name}'"
                )))
            }
        })
    }
}

fn shard_constant(shard: &Arg) -> Result<bool> {
    Ok(shard.__getitem__("constant")?.__bool__())
}

fn shard_evolutions(shard: &Arg) -> Result<Vec<i64>> {
    Ok(shard
        .__getitem__("evolutions")?
        .__iter__()?
        .iter()
        .filter_map(Arg::as_i64)
        .collect())
}

impl core::fmt::Debug for PoolIndex {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&PyObject::__repr__(self))
    }
}

impl PyObject for PoolIndex {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PoolIndex>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        "PoolIndex"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<laila.data.schema.pool_index.PoolIndex object at {:#x}>",
            Arc::as_ptr(&self.0) as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PoolIndex>()
            .is_some_and(|o| self.is(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        PoolIndex::__getattr__(self, name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        PoolIndex::__call_method__(self, name, args)
    }
    fn __bool__(&self) -> bool {
        true
    }
}
impl IntoArg for PoolIndex {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for PoolIndex {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::IsInstance for PoolIndex {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<PoolIndex>())
    }
}
