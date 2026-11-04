// Package laila is the Go port of the laila Python package -- Lambda's
// Interdisciplinary Large Atlas. This file is ``laila/__init__.py``: the
// single entry point user code reaches for. Every other capability is a
// public identifier of this flat package; the Python import paths are
// mirrored by the facade packages (laila-go/entry, laila-go/data, ...).
//
// Python module properties (``laila.memory``, ``laila.active_policy``, ...)
// are functions here (``laila.Memory()``, ``laila.Active_policy()`` /
// ``laila.Set_active_policy(p)``).
package laila

import (
	"path/filepath"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// Version is ``laila.__version__``.
var Version = "0.0.0+local"

// Is_env_load_trigger_ decides whether assigning value to
// ``laila.args.environment`` should trigger a full Load_environment_ cycle:
// a non-empty plain dict always does; a DotMap only when it carries a
// non-empty ``policies`` mapping or an ``active_gid``.
func Is_env_load_trigger_(value any) bool {
	switch v := value.(type) {
	case *py.DotMap:
		policies, _ := v.Lookup("policies")
		active, _ := v.Lookup("active_gid")
		if !py.IsNone(active) {
			return true
		}
		if py.IsNone(policies) {
			return false
		}
		n, err := py.Len(policies)
		return err == nil && n > 0
	case *py.Dict:
		return v.Len() > 0
	case py.Kw:
		return len(v) > 0
	case map[string]any:
		return len(v) > 0
	}
	return false
}

// New_LailaArgs_ builds the ``_LailaArgs`` DotMap: a DotMap that watches
// assignments to ``environment`` (full process-wide reload) and flushes
// the lazy environment mirror on reads of ``environment``.
func New_LailaArgs_() *py.DotMap {
	m := py.NewDotMap()
	Install_args_hooks_(m)
	return m
}

// Install_args_hooks_ wires the ``_LailaArgs`` __setattr__ / __getattr__
// overrides onto m (split from New_LailaArgs_ so the package-level Args
// can be allocated before the environment loader it calls into exists).
func Install_args_hooks_(m *py.DotMap) {
	m.Intercept = func(key string, value any) bool {
		if key == "environment" && Is_env_load_trigger_(value) {
			if err := Load_environment_(value); err != nil {
				panic(err)
			}
			return true
		}
		return false
	}
	m.OnRead = func(key string) {
		if key == "environment" || key == "" {
			func() {
				defer func() { _ = recover() }()
				Flush_environment_mirror_()
			}()
		}
	}
}

// Args is ``laila.args``: the hooks are installed in init() because the
// loader they call (Load_environment_ -> Terminate) reads Args itself.
var Args = py.NewDotMap()

// Arg_reader is the ``laila.arg_reader`` singleton bound to Args.
var Arg_reader = py.Must(New_ArgReader(py.Kw{"target": Args}))

func init() { Install_args_hooks_(Args) }

// Local_policies_ is ``laila._local_policies`` (gid -> policy).
var Local_policies_ = py.NewDict()

// Remote_policies_ is ``laila._remote_policies`` (gid -> RemotePolicyProxy).
var Remote_policies_ = py.NewDict()

// Live_policies_ is ``laila._live_policies``: every local policy constructed
// in this process, by gid, held weakly.
var Live_policies_ = py.NewWeakValueDictionary()

// Live_policies_get_ is ``laila._live_policies.get(gid)``.
func Live_policies_get_(gid string) any {
	v, _ := Live_policies_.Get(gid)
	return v
}

// Live_taskforces_snapshot_hook_ is set by the taskforce module
// (``_live_taskforces_snapshot``); nil until that module is linked.
var Live_taskforces_snapshot_hook_ func() []any

// Terminate gracefully tears down everything laila has spawned in this
// process (``laila.terminate(wait=True, cancel_pending=False)``). Returns
// one short error string per failed step; never raises.
func Terminate(kw ...py.Kw) []string {
	opts := py.MergeKw(kw...)
	wait := py.Truthy(opts.Get("wait", true))
	cancel_pending := py.Truthy(opts.Get("cancel_pending", false))
	errors := []string{}

	for _, it := range Local_policies_.Items() {
		gid := py.Str(it.Key)
		pol := it.Value
		if err := attempt(func() error {
			if comm := getattr(getattr(pol, "central"), "communication"); !py.IsNone(comm) {
				_, err := callm(comm, "stop")
				return err
			}
			return nil
		}); err != nil {
			errors = append(errors, "communication.stop["+gid+"]: "+py.Repr(err))
		}
		if err := attempt(func() error {
			if cmd := getattr(getattr(pol, "central"), "command"); !py.IsNone(cmd) {
				_, err := callm(cmd, "shutdown", py.Kw{"wait": wait, "cancel_pending": cancel_pending})
				return err
			}
			return nil
		}); err != nil {
			errors = append(errors, "command.shutdown["+gid+"]: "+py.Repr(err))
		}
		if err := attempt(func() error {
			mem := getattr(getattr(pol, "central"), "memory")
			router := getattr(mem, "pool_router")
			if !py.IsNone(router) {
				for _, pit := range mappingItems(getattr(router, "pools")) {
					pool_id := py.Str(pit.Key)
					if err := attempt(func() error {
						if py.Hasattr(pit.Value, "close") {
							_, err := callm(pit.Value, "close")
							return err
						}
						return nil
					}); err != nil {
						errors = append(errors, "pool.close["+pool_id+"]: "+py.Repr(err))
					}
				}
			}
			return nil
		}); err != nil {
			errors = append(errors, "memory.pools["+gid+"]: "+py.Repr(err))
		}
	}

	Local_policies_.Clear()
	Remote_policies_.Clear()
	// Drop the construction-time registry too.
	Live_policies_.Clear()
	activeMu.Lock_()
	active_policy_gid_ = ""
	activeMu.Unlock()

	if err := attempt(func() error {
		if Live_taskforces_snapshot_hook_ == nil {
			return nil
		}
		for _, tf := range Live_taskforces_snapshot_hook_() {
			if err := attempt(func() error {
				_, err := callm(tf, "shutdown", py.Kw{"wait": wait, "cancel_pending": cancel_pending})
				return err
			}); err != nil {
				errors = append(errors, "orphan_taskforce.shutdown["+py.Str(getattr(tf, "global_id"))+"]: "+py.Repr(err))
			}
		}
		return nil
	}); err != nil {
		errors = append(errors, "orphan_taskforce sweep: "+py.Repr(err))
	}

	if err := attempt(func() error { Logger_reset_singleton(); return nil }); err != nil {
		errors = append(errors, "logger.reset_singleton: "+py.Repr(err))
	}

	if err := attempt(func() error {
		Clear_environment_dirty_()
		env, _ := Args.Lookup("environment")
		if dm, ok := env.(*py.DotMap); ok {
			if policies, ok := dm.Lookup("policies"); ok {
				if pdm, ok := policies.(*py.DotMap); ok {
					pdm.Clear()
				}
			}
			dm.Pop("logger", nil)
		}
		return nil
	}); err != nil {
		errors = append(errors, "clear environment mirror: "+py.Repr(err))
	}
	return errors
}

// attempt runs fn converting panics into errors (a ``try/except Exception``).
func attempt(fn func() error) (err error) {
	defer py.Recover(&err)
	return fn()
}

// ---------------------------------------------------------------------------
// build / memorize / remember / forget
// ---------------------------------------------------------------------------

// Build materializes *entry* by running its constitution on a taskforce
// (``build(entry, *, taskforce_id=None)``).
//
// An Entry may be created with a Constitution rather than a concrete
// payload; in that case its ``state`` is STAGED and ``entry.data`` raises
// EntryNotBuiltError until you call Build.
//
// Always submits the entry's Build_async_ coroutine to the chosen
// taskforce (alpha by default). SimpleConstitution entries run their pure
// CPU chain inline on the loop thread; ComplexConstitution entries first
// await ``Manifest.async_realized`` to recursively materialize every
// referenced entry, then run the user's sync builder body on the
// taskforce's sync executor so nested blocking waits park the slot.
//
// The returned future identity is awaitable both ways:
// ``Build(entry).Wait()`` from sync callers, ``await`` from async ones.
// On completion the entry is mutated in place (``_payload`` populated,
// ``_constitution`` cleared, ``_state`` READY); the future also resolves
// to the same entry instance.
//
// Idempotent and coalescing: building a READY entry is a no-op (the
// future resolves to the entry); concurrent Build calls for the *same*
// entry object while a build is in flight return that in-flight future, so
// the constitution body runs exactly once.
//
//   - taskforce_id: target taskforce ``global_id``; defaults to
//     ``policy.central.command.alpha_taskforce``. For a process pool the
//     manifest is resolved and realized on the alpha taskforce and only
//     the constitution *body* runs in a worker.
//
// RuntimeError (inside the future) when *entry* is not built and has no
// constitution; CyclicDependencyError *synchronously* when this call is
// made from inside a resolution (build / remember) of the same entry.
func Build(entry any, kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	e := As_entry_(entry)
	if e == nil {
		return nil, py.AttributeError.Newf("'%s' object has no attribute 'global_id'", py.Pytypename(entry))
	}
	gid := e.Global_id()

	// A build that (transitively, through its constitution body) triggers
	// a build of the same entry can never finish; fail fast instead.
	if err := Check_resolve_cycle(gid); err != nil {
		return nil, err
	}

	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	command, ok := getattr(getattr(policy, "central"), "command").(interface {
		Central_command_base_() *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_
	})
	if !ok {
		return nil, py.AttributeError.New("'NoneType' object has no attribute 'command'")
	}
	cmd := command.Central_command_base_()
	key := Inflight_build_key_{Gid: gid, Id: py.Id(e.Self())}

	// A process pool cannot run the (local, unpicklable) build coroutine.
	// The orchestration -- manifest resolution and realization -- stays on
	// the alpha taskforce; only the constitution body is shipped to the
	// process pool as a picklable job (see Run_body_in_process_).
	taskforce_id := opts.Get("taskforce_id", nil)
	var process_tf_id any
	if !py.IsNone(taskforce_id) {
		tf, err := cmd.Taskforces.Getitem(taskforce_id)
		if err != nil {
			return nil, err
		}
		if !wraps_sync_(tf) {
			process_tf_id = taskforce_id
			taskforce_id = nil
		}
	}

	fut_box := []any{nil}
	var build_on_chain py.CoroutineFunc = func(args ...any) (any, error) {
		token := RESOLVE_CHAIN_.Set(append(append([]string(nil), RESOLVE_CHAIN_.Value()...), gid))
		defer func() {
			_ = RESOLVE_CHAIN_.Reset(token)
			cmd.Inflight_lock_.Lock()
			if cur, ok := cmd.Inflight_builds_[key]; ok && py.Is(cur, fut_box[0]) {
				delete(cmd.Inflight_builds_, key)
			}
			cmd.Inflight_lock_.Unlock()
		}()
		built, err := e.Build_async_(py.Kw{"_process_taskforce_id": process_tf_id})
		if err != nil {
			return nil, err
		}
		return built.Self(), nil
	}

	cmd.Inflight_lock_.Lock()
	defer cmd.Inflight_lock_.Unlock()
	if inflight, ok := cmd.Inflight_builds_[key]; ok && !py.IsNone(inflight) {
		if fl, ok := inflight.(interface{ Status() FutureStatus }); ok {
			switch fl.Status() {
			case FutureStatus_FINISHED, FutureStatus_ERROR, FutureStatus_CANCELLED:
			default:
				return inflight, nil
			}
		} else {
			return inflight, nil
		}
	}
	fut, err := cmd.Submit(py.NewList(build_on_chain), py.Kw{"taskforce_id": taskforce_id})
	if err != nil {
		return nil, err
	}
	fut_box[0] = fut
	cmd.Inflight_builds_[key] = fut
	return fut, nil
}

// remote_policy_proxy_class_ is the RemotePolicyProxy class descriptor
// (nil until the communication module is linked).
func remote_policy_proxy_class_() *py.Class {
	cls, _ := py.Class_by_name("RemotePolicyProxy")
	return cls
}

// is_remote_policy_proxy_ is ``isinstance(target, RemotePolicyProxy)``.
func is_remote_policy_proxy_(target any) bool {
	cls := remote_policy_proxy_class_()
	return cls != nil && py.Isinstance(target, cls)
}

// Route_memory_to_peer_ routes a memory op to a remote peer as a LOCAL
// (A-owned) Future (``laila._route_memory_to_peer``).
//
// With the *current* active policy A (not morphed), asking peer B to
// memorize/remember produces a normal local Future / GroupFuture owned by
// A -- not a RemoteFuture. The over-the-wire transfer runs inside one
// A-side task per entry; each task offloads the blocking wire RPC with
// ``asyncio.to_thread`` so it never blocks the taskforce event loop.
//
// Entries cross the wire as their canonical, self-describing
// ``Entry.serialize(transformation_base64)`` blob and are rebuilt via
// Build_by_scope -- real entry data moves, no shared pool needed.
// (RemoteFuture is reserved for *morph* mode, when A's active policy has
// been switched to B.)
func Route_memory_to_peer_(proxy any, op string, args []any, kwargs py.Kw) (any, error) {
	pool := kwargs.Get("pool", nil)
	if !py.IsNone(pool) {
		if _, ok := pool.(string); !ok {
			return nil, py.TypeError.New(
				"Remote memory ops need a pool gid or nickname *string* for the " +
					"peer-side pool; a standalone pool object cannot be shipped to a peer.")
		}
	}
	mem_kwargs := py.Kw{}
	if !py.IsNone(pool) {
		mem_kwargs["pool"] = pool
	}
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	command, ok := getattr(getattr(policy, "central"), "command").(interface {
		Central_command_base_() *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_
	})
	if !ok {
		return nil, py.AttributeError.New("'NoneType' object has no attribute 'command'")
	}
	cmd := command.Central_command_base_()

	// Same calling convention as the local verbs: one leading positional
	// (an entry / gid or a list of them). Extra positionals are a caller
	// error there (TypeError) and must not be silently dropped here.
	if (op == "memorize" || op == "remember" || op == "forget") && len(args) > 1 {
		return nil, py.TypeError.Newf(
			"laila.%s() takes one positional argument (an entry/gid or a list of "+
				"them) but %d were given; pass a list to %s several at once.", op, len(args), op)
	}

	remote_memory := getattr(getattr(proxy, "central"), "memory")
	// The wire wait is pure I/O: park the taskforce slot for its duration
	// so a fan-out larger than the slot cap cannot wedge.
	wire := func(name string, fargs ...any) (any, error) {
		return Park_async(py.Coroutine(func() (any, error) {
			return py.To_thread(func() (any, error) { return callm(remote_memory, name, fargs...) })
		}))
	}
	first_or_kw := func(name string) any {
		if len(args) > 0 {
			return args[0]
		}
		return kwargs.Get(name, nil)
	}
	as_list := func(v any) []any {
		if lst, ok := v.(*py.List); ok {
			return lst.Items()
		}
		if is_list_(v) {
			return py.Iter(v)
		}
		return []any{v}
	}

	switch op {
	case "memorize":
		entries_list := as_list(first_or_kw("entries"))
		tasks := py.NewList()
		for _, entry := range entries_list {
			e := As_entry_(entry)
			if e == nil {
				return nil, py.AttributeError.Newf("'%s' object has no attribute 'serialize'", py.Pytypename(entry))
			}
			blob, err := e.Serialize(Transformation_base64)
			if err != nil {
				return nil, err
			}
			var store py.CoroutineFunc = func(_ ...any) (any, error) {
				gids, err := wire("_remote_memorize", py.NewList(blob), mem_kwargs)
				if err != nil {
					return nil, err
				}
				return py.Getitem(gids, 0)
			}
			tasks.Append(store)
		}
		return cmd.Submit(tasks, py.Kw{"taskforce_id": cmd.Internal_taskforce})

	case "remember":
		ids := gids_of_(as_list(first_or_kw("entry_ids")))
		// ``persist`` means "cache back into the *peer's* alpha pool" (see
		// Remember); forward it so the peer honours it.
		fetch_kwargs := py.Kw{}
		for k, v := range mem_kwargs {
			fetch_kwargs[k] = v
		}
		if persist, ok := kwargs["persist"]; ok {
			fetch_kwargs["persist"] = py.Truthy(persist)
		}
		tasks := py.NewList()
		for _, eid := range ids {
			eid := eid
			var fetch py.CoroutineFunc = func(_ ...any) (any, error) {
				blobs, err := wire("_remote_remember", py.NewList(eid), fetch_kwargs)
				if err != nil {
					return nil, err
				}
				blob, err := py.Getitem(blobs, 0)
				if err != nil {
					return nil, err
				}
				return Build_by_scope(blob, py.Kw{"asynchronous": false})
			}
			tasks.Append(fetch)
		}
		return cmd.Submit(tasks, py.Kw{"taskforce_id": cmd.Internal_taskforce})

	case "forget":
		ids := gids_of_(as_list(first_or_kw("entry_ids")))
		var delete_async py.CoroutineFunc = func(_ ...any) (any, error) {
			return wire("_remote_forget", py.NewList(stringsToAny(ids)...), mem_kwargs)
		}
		return cmd.Submit(py.NewList(delete_async), py.Kw{"taskforce_id": cmd.Internal_taskforce})
	}

	// Any other op: pass straight through (gids are JSON-safe).
	return callm(remote_memory, op, append(append([]any(nil), args...), kwargs)...)
}

// Route_to_policy_ runs a central-memory operation against an arbitrary
// policy by ``global_id`` (``laila._route_to_policy(policy_id, op, args,
// kwargs, comm=None)``).
//
// This is the engine behind the ``policy_id=`` argument on Memorize /
// Remember. The target may be:
//
//   - a *remote* peer (a RemotePolicyProxy registered in Remote_policies_
//     after a successful handshake), in which case the operation is
//     dispatched over the peer's transport via the proxy's
//     attribute-chain. The local active policy is intentionally left
//     *unchanged* so the returned future registers into the local
//     policy's ``future_bank``.
//   - another *local* policy in this process (registered in
//     Local_policies_), in which case the op is dispatched to the
//     target's own ``central.memory``. Memory is *owner-bound* (it
//     submits to the command of the policy that owns it), so the active
//     policy is left untouched.
//
// ConnectionError if *policy_id* does not name any known local or remote
// policy.
func Route_to_policy_(policy_id any, op string, args []any, kwargs py.Kw, comm any) (any, error) {
	pid := py.Str(policy_id)
	target, found := Remote_policies_.Get(pid)
	if local, ok := Local_policies_.Get(pid); ok {
		target, found = local, true
	}
	if !found || py.IsNone(target) {
		return nil, py.ConnectionError.Newf(
			"Unknown policy_id %s: not a local policy or a connected peer. "+
				"Connect to the peer first with laila.add_peer()/add_tcpip_peer().", py.Repr(pid))
	}

	if is_remote_policy_proxy_(target) {
		if !py.IsNone(comm) {
			via, err := callm(target, "via", comm)
			if err != nil {
				return nil, err
			}
			target = via
		}
		return Route_memory_to_peer_(target, op, args, kwargs)
	}

	// The target's memory is owner-bound (it submits to *its own*
	// command), so no transient ``activate_policy(target)`` is needed; the
	// active policy -- and every other thread's view of it -- is left
	// untouched.
	memory := getattr(getattr(target, "central"), "memory")
	return callm(memory, op, append(append([]any(nil), args...), kwargs)...)
}

// Resolve_policy_ref_ resolves a policy reference to a ``global_id``
// string, or nil (``laila._resolve_policy_ref``).
//
// Accepts any of:
//
//   - nil -> nil (means "the active policy" for src, or "no remote
//     target" for dst).
//   - a live policy / RemotePolicyProxy (anything exposing ``global_id``)
//     -> its ``global_id``.
//   - a global-id string (recognised via Is_laila_resource) -> returned
//     as-is.
//   - a scoped shorthand such as ``"POLICY:trainer"`` or
//     ``"POLICY:<uuid>"`` -> expanded by Resolve_global_id.
//   - any other string -> treated as a policy *nickname* and turned into
//     a deterministic ``LAILA:POLICY:<uuid5>``. Two processes in the same
//     namespace derive the same gid from the same nickname, which is what
//     makes nicknames usable for cross-process routing.
func Resolve_policy_ref_(ref any) (any, error) {
	if py.IsNone(ref) {
		return nil, nil
	}
	if gid, err := py.Getattr(ref, "global_id"); err == nil {
		if s, ok := gid.(string); ok {
			return s, nil
		}
	}
	return Resolve_global_id(LAILA_IDENTIFIABLE_OBJECT_class, py.Str(ref), py.Kw{
		"default_scopes":  []string{POLICY_SCOPE_},
		"parse_evolution": false,
	})
}

// Extract_gids_ pulls a list of entry global_ids out of a ``(entry_ids,)``
// arg shape (``laila._extract_gids``).
func Extract_gids_(args []any, kwargs py.Kw) []string {
	var val any
	if len(args) > 0 {
		val = args[0]
	} else {
		val = kwargs.Get("entry_ids", kwargs.Get("entries", nil))
	}
	var items []any
	if is_list_(val) {
		items = py.Iter(val)
	} else {
		items = []any{val}
	}
	return gids_of_(items)
}

// Relay_transfer_ drives a 3-party transfer by commanding the *source*
// policy (``laila._relay_transfer(verb, src_gid, args, kwargs, *,
// src_pool, dst_gid, dst_pool, comm, persist=True)``).
//
// With active policy A, ``src_gid`` = B, ``dst_gid`` = C: A reaches B (A
// must be peered to B) and asks B to move the entries to/from C over B's
// *own* B<->C link. A never brokers the B<->C connection.
//
//   - ``verb == "memorize"``: B pushes ``entry_ids`` from its ``src_pool``
//     into C's ``dst_pool``.
//   - ``verb == "remember"``: B pulls ``entry_ids`` from C's ``dst_pool``
//     and stores them into B's ``src_pool``.
func Relay_transfer_(verb string, src_gid string, args []any, kwargs py.Kw, opts py.Kw) (any, error) {
	src, found := Remote_policies_.Get(src_gid)
	if local, ok := Local_policies_.Get(src_gid); ok {
		src, found = local, true
	}
	if !found || py.IsNone(src) {
		return nil, py.ConnectionError.Newf(
			"src_policy %s is not a connected peer or a local policy. "+
				"The active policy must be peered to the source policy "+
				"(connect first with laila.add_peer()).", py.Repr(src_gid))
	}
	gids := py.NewList(stringsToAny(Extract_gids_(args, kwargs))...)
	src_pool := opts.Get("src_pool", nil)
	dst_gid := opts.Get("dst_gid", nil)
	dst_pool := opts.Get("dst_pool", nil)
	comm := opts.Get("comm", nil)
	persist := opts.Get("persist", true)

	var relay any
	if is_remote_policy_proxy_(src) {
		target := src
		if !py.IsNone(comm) {
			via, err := callm(src, "via", comm)
			if err != nil {
				return nil, err
			}
			target = via
		}
		relay = getattr(getattr(target, "central"), "memory")
	} else {
		// Owner-bound memory: no transient activation of ``src`` required.
		relay = getattr(getattr(src, "central"), "memory")
	}
	if verb == "memorize" {
		return callm(relay, "_relay_memorize", gids, py.Kw{
			"src_pool": src_pool, "dst_policy": dst_gid, "dst_pool": dst_pool, "comm": comm,
		})
	}
	return callm(relay, "_relay_remember", gids, py.Kw{
		"dst_policy": dst_gid, "dst_pool": dst_pool, "src_pool": src_pool, "comm": comm, "persist": persist,
	})
}

// Lone_manifest_ returns the manifest-like object when *args* is exactly
// one bare one (``laila._lone_manifest``), nil otherwise.
//
// Used by Memorize / Remember / Forget to dispatch to the object's own
// bulk operation. Matches a bare Manifest (any subclass) and any
// ManifestBacked object (e.g. ``laila.addons.torch.Module`` / ``Optimizer``
// in Python, which turn themselves into a manifest and run its
// type-specific operation). Only a *bare* object triggers this -- a
// manifest inside a list is treated as a regular entry (its payload is
// the blueprint), which is also how ``Manifest.memorize`` stores the
// manifest itself without recursing back into the shim.
func Lone_manifest_(args []any) any {
	if len(args) == 1 && manifest_or_backed_(args[0]) {
		return args[0]
	}
	return nil
}

// Memorize persists one or more entries into a policy's memory
// (``memorize(*args, src_policy=None, src_pool=None, dst_policy=None,
// dst_pool=None, comm=None, policy_id=None, pool_id=None,
// pool_nickname=None, affinity=None, **kwargs)``).
//
// Thin top-level shim that forwards to ``policy.central.memory.memorize``.
// The work performed there:
//
//  1. Resolve the destination pool via the PoolRouter (``pool_id`` >
//     ``pool_nickname`` > alpha).
//  2. For each entry, wrap it in a Record, serialize the record + its
//     payload through the pool's TransformationSequence, and await the
//     pool's ``_write_async`` for the actual storage round-trip.
//  3. Submit one async coroutine per entry to the alpha taskforce so
//     writes proceed concurrently without blocking the caller.
//
// Returns a *future identity* you can ``await`` (async) or ``.Wait()``
// (sync): one entry -> a single Future; many entries -> a GroupFuture
// that completes when all child writes complete. Pure in-memory pools
// that perform writes synchronously inside the submission may return nil.
//
// Passing a single Manifest (``laila.memorize(manifest, ...)``) is
// equivalent to ``Manifest.memorize``: every pending entry the manifest
// was built from *and* the manifest itself are stored, and one
// GroupFuture covering all writes is returned. A manifest inside a list
// is stored as a plain entry (blueprint only). Manifest dispatch applies
// to local writes only; when ``dst_policy`` / ``src_policy`` names a peer
// the manifest travels as a regular entry.
//
//   - entries: the entry or entries to store (single entries are
//     auto-wrapped; also accepted as the ``entries=`` keyword).
//   - pool_id / pool_nickname / dst_pool: destination pool selector;
//     falls back to the alpha pool.
//   - affinity: reserved for future affinity-based routing.
//   - policy_id / dst_policy: ``global_id`` of a *peer* (or another local
//     policy) to write into (see Route_to_policy_).
//   - src_policy / src_pool: 3-party relay source (see Relay_transfer_).
func Memorize(args ...any) (any, error) {
	pos, kwargs := py.SplitKw(args)
	kwargs = kwargs.Copy()

	// Accept the leading positional via the ``entries=`` keyword too.
	if len(pos) == 0 {
		if v, ok := kwargs["entries"]; ok {
			delete(kwargs, "entries")
			pos = []any{v}
		}
	}

	src_policy := kwargs.Pop("src_policy", nil)
	src_pool := kwargs.Pop("src_pool", nil)
	dst_policy := kwargs.Pop("dst_policy", nil)
	dst_pool := kwargs.Pop("dst_pool", nil)
	comm := kwargs.Pop("comm", nil)
	policy_id := kwargs.Pop("policy_id", nil)
	pool_id := kwargs.Pop("pool_id", nil)
	pool_nickname := kwargs.Pop("pool_nickname", nil)
	affinity := kwargs.Pop("affinity", nil)

	// Back-compat: policy_id -> dst_policy, pool_id/pool_nickname -> dst_pool.
	if py.IsNone(dst_policy) {
		dst_policy = policy_id
	}
	if py.IsNone(dst_pool) {
		if !py.IsNone(pool_id) {
			dst_pool = pool_id
		} else {
			dst_pool = pool_nickname
		}
	}

	src_gid, err := Resolve_policy_ref_(src_policy)
	if err != nil {
		return nil, err
	}
	dst_gid, err := Resolve_policy_ref_(dst_policy)
	if err != nil {
		return nil, err
	}
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	active_gid := global_id_of_(policy)

	// Source is another policy -> 3-party relay (B pushes src->dst).
	if !py.IsNone(src_gid) && !py.Eq(src_gid, active_gid) {
		return Relay_transfer_("memorize", py.Str(src_gid), pos, kwargs, py.Kw{
			"src_pool": src_pool, "dst_gid": dst_gid, "dst_pool": dst_pool, "comm": comm,
		})
	}

	// Source is the active policy.
	if !py.IsNone(dst_gid) && !py.Eq(dst_gid, active_gid) {
		// active -> peer push (2-party).
		return Route_to_policy_(dst_gid, "memorize", pos, py.Kw{"pool": dst_pool}, comm)
	}

	// A bare Manifest / ManifestBacked -> its bulk operation (pending
	// entries + itself).
	if man := Lone_manifest_(pos); man != nil {
		kw := kwargs.Copy()
		kw["pool"] = dst_pool
		return manifest_bulk_op_(man, "memorize", kw)
	}

	// Purely local / standalone write into the active policy's pool.
	kw := kwargs.Copy()
	kw["pool"] = dst_pool
	kw["affinity"] = affinity
	memory := getattr(getattr(policy, "central"), "memory")
	return callm(memory, "memorize", append(append([]any(nil), pos...), kw)...)
}

// Resolve_global_id_ref expands an *entry* reference into a full
// ``global_id`` (``laila.resolve_global_id(ref, *, evolution=None,
// prefix_scopes=None, parse_evolution=True)``).
//
// Thin alias for ``Entry.resolve_global_id``, i.e. Resolve_global_id with
// ENTRY as the default scope. Accepts a full global id (returned as-is),
// a scoped shorthand ``SCOPE[:SCOPE...]:<uuid | nickname>[@attrs]`` such
// as ``"MANIFEST:my_dataset"`` or ``"ENTRY:counter@evolution=3"``, or a
// bare ``<uuid | nickname>[@attrs]`` which is taken to be an ENTRY.
// ``prefix_scopes`` (default ``["LAILA"]``) goes in front of the
// user-supplied scopes. (The module-level name collides with the
// class-level Resolve_global_id under the port's naming rule, hence the
// ``_ref`` suffix.)
func Resolve_global_id_ref(ref any, kw ...py.Kw) (string, error) {
	opts := py.MergeKw(kw...)
	return Resolve_global_id(Entry_class, ref, py.Kw{
		"evolution":       opts.Get("evolution", nil),
		"prefix_scopes":   opts.Get("prefix_scopes", nil),
		"parse_evolution": opts.Get("parse_evolution", true),
	})
}

// Resolve_entry_refs_ expands nickname shorthands in the leading
// ``entry_ids`` positional (``laila._resolve_entry_refs``).
//
// Strings (and strings inside a list/tuple) go through
// Resolve_global_id_ref; anything else (Manifest, Entry, ...) is passed
// through untouched.
func Resolve_entry_refs_(args []any, prefix_scopes any) ([]any, error) {
	if len(args) == 0 {
		return args, nil
	}
	head, rest := args[0], args[1:]
	one := func(x any) (any, error) {
		if _, ok := x.(string); ok {
			return Resolve_global_id_ref(x, py.Kw{"prefix_scopes": prefix_scopes})
		}
		return x, nil
	}
	switch h := head.(type) {
	case string:
		resolved, err := one(h)
		if err != nil {
			return nil, err
		}
		head = resolved
	case *py.List:
		out := py.NewList()
		for _, x := range h.Items() {
			r, err := one(x)
			if err != nil {
				return nil, err
			}
			out.Append(r)
		}
		head = out
	case py.Tuple:
		out := make(py.Tuple, 0, len(h))
		for _, x := range h {
			r, err := one(x)
			if err != nil {
				return nil, err
			}
			out = append(out, r)
		}
		head = out
	case []any, []string:
		out := py.NewList()
		for _, x := range py.Iter(h) {
			r, err := one(x)
			if err != nil {
				return nil, err
			}
			out.Append(r)
		}
		head = out
	}
	return append([]any{head}, rest...), nil
}

// Resolve_nickname__ translates a ``nickname=`` (and optional
// ``evolution=``) kwarg pair into the canonical ``[global_id]`` shape
// consumed by ``memory.remember`` / ``memory.forget``
// (``laila.__resolve_nickname``).
//
// The nickname may carry a scope shorthand (``"MANIFEST:my_dataset"``); a
// bare nickname resolves to an ENTRY. The resulting gid uses the *active*
// UUID-5 namespace (see Get_active_namespace), so anyone in the same
// namespace who calls with the same nickname will derive the same gid.
//
// ValueError if ``kwargs["nickname"]`` is not a string.
func Resolve_nickname__(kwargs py.Kw, prefix_scopes any) (*py.List, error) {
	nickname, ok := kwargs["nickname"].(string)
	if !ok {
		return nil, py.ValueError.New("nickname must be a string")
	}
	gid, err := Resolve_global_id_ref(nickname, py.Kw{
		"evolution":       kwargs.Get("evolution", nil),
		"prefix_scopes":   prefix_scopes,
		"parse_evolution": false,
	})
	if err != nil {
		return nil, err
	}
	return py.NewList(gid), nil
}

// Remember retrieves one or more entries from a policy's memory
// (``remember(*args, persist=True, src_policy=None, src_pool=None,
// dst_policy=None, dst_pool=None, comm=None, prefix_scopes=None,
// policy_id=None, pool_id=None, pool_nickname=None, **kwargs)``).
//
// Thin top-level shim that forwards to ``policy.central.memory.remember``:
// resolve the source pool via the PoolRouter, await the pool's
// ``_read_async`` per id, rebuild each blob through ``Record._build_async``
// and submit one coroutine per entry to the alpha taskforce.
//
// By default (``persist=True``), if the routed source pool is *not* the
// alpha pool, the fetched entries are additionally memorized into the
// alpha pool and the returned future only resolves once that write
// completes. Pass ``persist=False`` for one-shot reads.
//
// Identify entries with full ``global_id`` strings or a *nickname
// shorthand* ``SCOPE[:SCOPE...]:nickname[@key=value,...]`` expanded by
// Resolve_global_id_ref; everything after ``@`` is a list of search
// arguments (``evolution=<n>``, ``creation_timestamp=<iso>``; none ->
// the highest evolution for a variable). The keyword form
// ``nickname=...`` (+ optional ``evolution=``) is also accepted and
// consumed before the call is forwarded.
//
// Passing a single Manifest is equivalent to ``Manifest.remember``: every
// ``global_id`` the manifest references is fetched and one GroupFuture is
// returned whose ``.data`` is the list of entries in ``list(manifest)``
// order. Local reads only.
func Remember(args ...any) (any, error) {
	pos, kwargs := py.SplitKw(args)
	kwargs = kwargs.Copy()

	persist := py.Truthy(kwargs.Pop("persist", true))
	src_policy := kwargs.Pop("src_policy", nil)
	src_pool := kwargs.Pop("src_pool", nil)
	dst_policy := kwargs.Pop("dst_policy", nil)
	dst_pool := kwargs.Pop("dst_pool", nil)
	comm := kwargs.Pop("comm", nil)
	prefix_scopes := kwargs.Pop("prefix_scopes", nil)
	policy_id := kwargs.Pop("policy_id", nil)
	pool_id := kwargs.Pop("pool_id", nil)
	pool_nickname := kwargs.Pop("pool_nickname", nil)

	if _, ok := kwargs["nickname"]; ok {
		pos = nil
		ids, err := Resolve_nickname__(kwargs, prefix_scopes)
		if err != nil {
			return nil, err
		}
		kwargs["entry_ids"] = ids
		delete(kwargs, "nickname")
		delete(kwargs, "evolution")
	}

	// Accept the leading positional via the ``entry_ids=`` keyword too.
	if len(pos) == 0 {
		if v, ok := kwargs["entry_ids"]; ok {
			delete(kwargs, "entry_ids")
			pos = []any{v}
		}
	}

	// Expand nickname shorthands ("MANIFEST:my_dataset") into full gids
	// before any routing so peers always receive canonical ids.
	pos, err := Resolve_entry_refs_(pos, prefix_scopes)
	if err != nil {
		return nil, err
	}

	// Back-compat: policy_id -> dst_policy, pool_id/pool_nickname -> dst_pool.
	if py.IsNone(dst_policy) {
		dst_policy = policy_id
	}
	if py.IsNone(dst_pool) {
		if !py.IsNone(pool_id) {
			dst_pool = pool_id
		} else {
			dst_pool = pool_nickname
		}
	}

	src_gid, err := Resolve_policy_ref_(src_policy)
	if err != nil {
		return nil, err
	}
	dst_gid, err := Resolve_policy_ref_(dst_policy)
	if err != nil {
		return nil, err
	}
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	active_gid := global_id_of_(policy)

	// Source policy is another policy -> 3-party relay (B pulls from dst).
	if !py.IsNone(src_gid) && !py.Eq(src_gid, active_gid) {
		return Relay_transfer_("remember", py.Str(src_gid), pos, kwargs, py.Kw{
			"src_pool": src_pool, "dst_gid": dst_gid, "dst_pool": dst_pool, "comm": comm, "persist": persist,
		})
	}

	// Active policy pulls from a peer (2-party).
	if !py.IsNone(dst_gid) && !py.Eq(dst_gid, active_gid) {
		return Route_to_policy_(dst_gid, "remember", pos, py.Kw{"persist": persist, "pool": dst_pool}, comm)
	}

	// A bare Manifest / ManifestBacked -> fetch every entry it references
	// (a ManifestBacked loads the stored state back into itself).
	if man := Lone_manifest_(pos); man != nil {
		kw := kwargs.Copy()
		delete(kw, "resolve") // only meaningful for reads by id
		kw["pool"] = dst_pool
		kw["persist"] = persist
		return manifest_bulk_op_(man, "remember", kw)
	}

	// Purely local / standalone read from the active policy's pool.
	kw := kwargs.Copy()
	kw["persist"] = persist
	kw["pool"] = dst_pool
	memory := getattr(getattr(policy, "central"), "memory")
	return callm(memory, "remember", append(append([]any(nil), pos...), kw)...)
}

// Forget deletes one or more entries from the active policy's memory
// (``forget(*args, policy=None, pool=None, comm=None, prefix_scopes=None,
// policy_id=None, pool_id=None, pool_nickname=None, **kwargs)``).
//
// Thin top-level shim that forwards to ``policy.central.memory.forget``.
// For each gid, awaits the routed pool's ``_delete_async`` (or the batch
// path on batch-accelerated pools) and yields a future per entry.
//
// Forgetting is *pool-local*: it only removes the blob from the routed
// pool, not from any other pool that may have a copy. Identifying entries
// follows the same rules as Remember; unlike Remember, forgetting is an
// exact-key operation: an id without ``evolution`` does not resolve to
// the highest evolution.
//
// Passing a single Manifest is equivalent to ``Manifest.forget``: every
// referenced entry *and* the manifest itself are deleted from the routed
// pool, and one GroupFuture is returned. Local deletes only.
func Forget(args ...any) (any, error) {
	pos, kwargs := py.SplitKw(args)
	kwargs = kwargs.Copy()

	policy_ref := kwargs.Pop("policy", nil)
	pool := kwargs.Pop("pool", nil)
	comm := kwargs.Pop("comm", nil)
	prefix_scopes := kwargs.Pop("prefix_scopes", nil)
	policy_id := kwargs.Pop("policy_id", nil)
	pool_id := kwargs.Pop("pool_id", nil)
	pool_nickname := kwargs.Pop("pool_nickname", nil)

	if _, ok := kwargs["nickname"]; ok {
		pos = nil
		ids, err := Resolve_nickname__(kwargs, prefix_scopes)
		if err != nil {
			return nil, err
		}
		kwargs["entry_ids"] = ids
		delete(kwargs, "nickname")
		delete(kwargs, "evolution")
	}

	// Accept the leading positional via the ``entry_ids=`` keyword too.
	if len(pos) == 0 {
		if v, ok := kwargs["entry_ids"]; ok {
			delete(kwargs, "entry_ids")
			pos = []any{v}
		}
	}

	pos, err := Resolve_entry_refs_(pos, prefix_scopes)
	if err != nil {
		return nil, err
	}

	// Back-compat: policy_id -> policy, pool_id/pool_nickname -> pool.
	if py.IsNone(policy_ref) {
		policy_ref = policy_id
	}
	if py.IsNone(pool) {
		if !py.IsNone(pool_id) {
			pool = pool_id
		} else {
			pool = pool_nickname
		}
	}

	target_gid, err := Resolve_policy_ref_(policy_ref)
	if err != nil {
		return nil, err
	}
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	active_gid := global_id_of_(policy)

	if !py.IsNone(target_gid) && !py.Eq(target_gid, active_gid) {
		return Route_to_policy_(target_gid, "forget", pos, py.Kw{"pool": pool}, comm)
	}

	// A bare Manifest / ManifestBacked -> delete every referenced entry
	// plus the manifest.
	if man := Lone_manifest_(pos); man != nil {
		kw := kwargs.Copy()
		kw["pool"] = pool
		return manifest_bulk_op_(man, "forget", kw)
	}

	kw := kwargs.Copy()
	kw["pool"] = pool
	memory := getattr(getattr(policy, "central"), "memory")
	return callm(memory, "forget", append(append([]any(nil), pos...), kw)...)
}

// Set_default_directory relocates every on-disk sub-directory laila uses
// by default (``laila.set_default_directory(directory)``).
//
// Mutates LAILA_DEFAULT_DIRECTORIES in place. Pools created *after* this
// call resolve their backing directories from the new root; pools that
// have already opened files keep their existing paths (they captured the
// old root in Model_post_init).
//
// The conventional subdirectories are derived from the new root: ``root``
// (the directory itself), ``pools`` (per-pool data folders), ``logs``
// (log files written by Logger), ``secrets`` (key material loaded by the
// crypto extras) and ``indices`` (non-memorizing query helpers such as
// Manifest SQL indices, per-manifest folders).
//
// ``~`` is expanded. Call this *before* instantiating any pool that you
// want rooted at the new location -- typically at the very top of your
// program.
func Set_default_directory(directory string) {
	directory = py.Expanduser(directory)
	LAILA_DEFAULT_DIRECTORIES["root"] = directory
	LAILA_DEFAULT_DIRECTORIES["pools"] = filepath.Join(directory, "pools")
	LAILA_DEFAULT_DIRECTORIES["logs"] = filepath.Join(directory, "logs")
	LAILA_DEFAULT_DIRECTORIES["secrets"] = filepath.Join(directory, "secrets")
	LAILA_DEFAULT_DIRECTORIES["indices"] = filepath.Join(directory, "indices")
}

// Manifest_alias is the ``laila.manifest`` module alias of the Manifest
// class descriptor (``manifest = Manifest``).
var Manifest_alias = Manifest_class

// Complex_manifest is the ``laila.complex_manifest`` module alias of the
// ComplexManifest class descriptor (``complex_manifest =
// ComplexManifest``); construct one with New_ComplexManifest.
var Complex_manifest = ComplexManifest_class

// Encryption_key is ``laila.encryption_key``: the process-wide Fernet key
// used by FernetEncryption when no explicit ``key=`` is given. Alias of
// ``laila.args.encryption.key``; nil when unset.
func Encryption_key() any {
	section, ok := Args.Lookup("encryption")
	if !ok || py.IsNone(section) {
		return nil
	}
	var key any
	switch s := section.(type) {
	case *py.DotMap:
		key, _ = s.Lookup("key")
	case *py.Dict:
		key, _ = s.Get("key")
	default:
		return nil
	}
	if py.IsNone(key) || py.Eq(key, "") {
		return nil
	}
	if d, ok := key.(*py.DotMap); ok && d.Len() == 0 {
		return nil
	}
	if d, ok := key.(*py.Dict); ok && d.Len() == 0 {
		return nil
	}
	return key
}

// Set_encryption_key is the ``laila.encryption_key`` setter: stores value
// under ``laila.args.encryption.key``.
func Set_encryption_key(value any) { Args.Set("encryption.key", value) }

// Read_args loads user arguments from a file or terminal flags into
// laila.Args (``laila.read_args(source, terminal_args=None)``).
func Read_args(source string, kw ...py.Kw) error {
	return Arg_reader.Load(source, kw...)
}

var (
	activeMu           py.RLock
	active_policy_gid_ string
	active_namespace_  *py.UUID
)

// Active_policy_gid_ is ``laila._active_policy_gid`` ("" for None).
func Active_policy_gid_() string {
	activeMu.Lock_()
	defer activeMu.Unlock()
	return active_policy_gid_
}

// Get_active_namespace returns the UUID-5 namespace used to derive
// deterministic IDs from nicknames (LAILA_UNIVERSAL_NAMESPACE until
// Set_active_namespace is called).
func Get_active_namespace() py.UUID {
	activeMu.Lock_()
	defer activeMu.Unlock()
	if active_namespace_ == nil {
		ns := LAILA_UNIVERSAL_NAMESPACE
		active_namespace_ = &ns
	}
	return *active_namespace_
}

// Set_active_namespace replaces the active namespace with
// ``uuid5(NAMESPACE_DNS, namespace_key)``.
func Set_active_namespace(namespace_key string) {
	ns := py.Uuid5(py.NAMESPACE_DNS, namespace_key)
	activeMu.Lock_()
	active_namespace_ = &ns
	activeMu.Unlock()
}

// Reset_active_namespace_ is ``laila._active_namespace = None``: the next
// Get_active_namespace falls back to LAILA_UNIVERSAL_NAMESPACE.
func Reset_active_namespace_() {
	activeMu.Lock_()
	active_namespace_ = nil
	activeMu.Unlock()
}

// Active_namespace_ is the raw ``laila._active_namespace`` (nil for None,
// i.e. never set) -- what a test saves before Set_active_namespace and
// hands back to Set_active_namespace_ to restore.
func Active_namespace_() *py.UUID {
	activeMu.Lock_()
	defer activeMu.Unlock()
	return active_namespace_
}

// Set_active_namespace_ is ``laila._active_namespace = saved`` (the raw
// assignment, as opposed to Set_active_namespace which derives a uuid5).
func Set_active_namespace_(ns *py.UUID) {
	activeMu.Lock_()
	active_namespace_ = ns
	activeMu.Unlock()
}

// Set_active_policy_gid_ is ``laila._active_policy_gid = gid`` ("" for
// None: the next Get_active_policy lazily creates a fresh DefaultPolicy).
func Set_active_policy_gid_(gid string) {
	activeMu.Lock_()
	active_policy_gid_ = gid
	activeMu.Unlock()
}

// Get_active_policy returns the active policy, lazily creating a
// DefaultPolicy on first access. The result is a local policy or a
// RemotePolicyProxy ("morph mode").
func Get_active_policy() (any, error) {
	if Active_policy_gid_() == "" {
		p, err := New_DefaultPolicy()
		if err != nil {
			return nil, err
		}
		Activate_policy(p)
	}
	gid := Active_policy_gid_()
	if p, ok := Local_policies_.Get(gid); ok {
		return p, nil
	}
	p, err := Remote_policies_.Getitem(gid)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Activate_policy replaces the active policy with policy (local policy or
// RemotePolicyProxy). Equivalent to ``laila.active_policy = policy``.
func Activate_policy(policy any) {
	new_gid := py.Str(getattr(policy, "global_id"))
	activeMu.Lock_()
	active_policy_gid_ = new_gid
	activeMu.Unlock()

	policyCls, _ := py.Class_by_name("_LAILA_IDENTIFIABLE_POLICY")
	isLocal := policyCls != nil && py.Isinstance(policy, policyCls)
	if isLocal {
		Local_policies_.Set(new_gid, policy)
	}
	// Lazy: the dump happens on the next read of ``laila.args.environment``.
	_ = attempt(func() error {
		if isLocal {
			Mark_environment_dirty_(policy)
		}
		return nil
	})
}

// Set_active_policy is the ``laila.active_policy = policy`` setter.
func Set_active_policy(policy any) { Activate_policy(policy) }

// Get_active_local_policy_ returns the local policy that should own
// newly-created futures (``laila._get_active_local_policy``).
//
// Used by Future / GroupFuture (and other LAILA_IDENTIFIABLE_FUTURE_
// subclasses) at construction time so they can self-register into
// ``policy.future_bank``. Futures must always live in a *local* policy (a
// RemotePolicyProxy does not have a real ``future_bank``), even when the
// *active* policy is a remote peer in morph mode.
//
// Resolution order:
//
//  1. If no policy is active yet, lazily activate a DefaultPolicy via
//     Get_active_policy. This means importing laila and creating a future
//     works without any setup.
//  2. If the active gid maps to a *local* policy, return it.
//  3. Otherwise (active policy is a remote proxy): return the local policy
//     that *owns the link* to that peer (the proxy's communication object
//     belongs to exactly one local policy); failing that, if exactly one
//     local policy exists, return it; if multiple local policies exist and
//     none owns the proxy, raise RuntimeError.
func Get_active_local_policy_() (any, error) {
	if Active_policy_gid_() == "" {
		if _, err := Get_active_policy(); err != nil {
			return nil, err
		}
	}
	gid := Active_policy_gid_()
	if p, ok := Local_policies_.Get(gid); ok {
		return p, nil
	}
	proxy, _ := Remote_policies_.Get(gid)
	owner := getattr(getattr(proxy, "_comm"), "_local_policy")
	if policyCls, ok := py.Class_by_name("_LAILA_IDENTIFIABLE_POLICY"); ok && py.Isinstance(owner, policyCls) {
		return owner, nil
	}
	if Local_policies_.Len() == 1 {
		return Local_policies_.Values()[0], nil
	}
	return nil, py.RuntimeError.New(
		"No local policy available for future registration; " +
			"set `laila.active_policy` to a local policy first.")
}

// Future_bank_policies_ yields every local policy whose ``future_bank``
// may hold a future (``laila._future_bank_policies``).
//
// Activated policies (Local_policies_) first, then every other live one
// (Live_policies_). Bank walkers (identity handles, ``release``,
// GroupFuture children, ``runtime.wait``) use this so a future owned by a
// constructed-but-never-activated policy -- e.g. an in-process peer
// serving routed memory ops -- stays resolvable.
func Future_bank_policies_() []any {
	seen := map[uintptr]struct{}{}
	out := []any{}
	for _, pol := range Local_policies_.Values() {
		id := py.Id(pol)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, pol)
	}
	for _, pol := range Live_policies_.Values() {
		id := py.Id(pol)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, pol)
	}
	return out
}

// Owning_local_policy_ returns the local policy a future stamped with
// policy_id belongs to (``laila._owning_local_policy``).
//
// Futures carry an explicit ``policy_id`` (owner-bound memory stamps its
// futures with the memory's owner, taskforces with theirs). When that gid
// names a *local* policy -- activated (Local_policies_) or merely
// constructed (Live_policies_) -- the future is registered in that
// policy's ``future_bank`` regardless of which policy is active on the
// calling thread. Otherwise (nil, a remote gid, a nickname not yet
// registered) fall back to Get_active_local_policy_.
func Owning_local_policy_(policy_id any) (any, error) {
	pid := policy_id
	if g, ok := policy_id.(interface{ Global_id() string }); ok {
		pid = g.Global_id()
	}
	if s, ok := pid.(string); ok {
		owner, found := Local_policies_.Get(s)
		if !found {
			owner, found = Live_policies_.Get(s)
		}
		if found && !py.IsNone(owner) {
			return owner, nil
		}
	}
	return Get_active_local_policy_()
}

// Active_policy is the ``laila.active_policy`` getter.
func Active_policy() (any, error) { return Get_active_policy() }

// active_central_ is ``get_active_policy().central.<name>``: the module
// properties below re-resolve through the *current* active policy on
// every read, so swapping the active policy is instantly visible.
func active_central_(name string) (any, error) {
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	return py.Getattr_path(policy, []string{"central", name})
}

// Communication is the ``laila.communication`` property: the active
// policy's ``central.communication`` -- peers, protocols, RPC.
func Communication() (any, error) { return active_central_("communication") }

// active_communication_ is ``get_active_policy().central.communication``
// typed as the hub.
func active_communication_() (*LAILA_IDENTIFIABLE_COMMUNICATION_, error) {
	v, err := active_central_("communication")
	if err != nil {
		return nil, err
	}
	comm := As_communication_(v)
	if comm == nil {
		return nil, py.AttributeError.Newf("'%s' object has no attribute 'add_peer'", py.Pytypename(v))
	}
	return comm, nil
}

// Add_peer connects to a remote policy and registers it as a peer of the
// active policy.
//
// Delegates to LAILA_IDENTIFIABLE_COMMUNICATION_.Add_peer, which:
//
//  1. Picks a transport protocol that can handle *uri* (currently the
//     TCP/IP WebSocket protocol; matches ``ws://`` and ``wss://``).
//  2. Performs the handshake -- exchanges ``peer_secret_key`` values and
//     policy ``global_id``s.
//  3. Registers a RemotePolicyProxy in ``laila.peers[<remote_gid>]`` and
//     in Remote_policies_.
//
// Once registered, the proxy can be passed to Activate_policy ("morph
// mode") so subsequent local calls transparently RPC into the peer, or it
// can be used directly:
//
//	gid, _ := laila.Add_peer("ws://host:9000", "...")
//	peer := laila.Peers()[gid]
//	peer.Memorize(my_entry)  // explicit RPC, active policy unchanged
//
// *uri* is the URI of the remote policy; today the only supported scheme
// is WebSocket: ``"ws://host:port"`` or ``"wss://host:port"``. *secret*
// is the remote policy's ``peer_secret_key`` (configured on the remote's
// communication protocol); required for the handshake to succeed.
// Returns the ``global_id`` of the newly peered remote policy -- use it
// to index ``laila.peers``.
//
// Raises ConnectionError if no registered protocol can handle *uri*, or
// if the underlying transport refuses to connect; PermissionError if the
// *secret* is rejected by the remote's protocol.
func Add_peer(uri string, secret string) (string, error) {
	comm, err := active_communication_()
	if err != nil {
		return "", err
	}
	return comm.Add_peer(uri, secret)
}

// Add_tcpip_peer is ``laila.add_tcpip_peer(host, port, secret)``: peer
// with a remote policy over TCP/IP (WebSocket), building the ``ws://``
// URI internally.
func Add_tcpip_peer(host string, port int, secret string) (string, error) {
	comm, err := active_communication_()
	if err != nil {
		return "", err
	}
	return comm.Add_tcpip_peer(host, port, secret)
}

// Request returns a transport-bound proxy for a connected peer.
//
// The channel-aware companion to Add_peer. Given a peered policy's
// ``global_id`` it returns its RemotePolicyProxy, optionally *bound* to a
// specific transport via *comm_protocol* so that every call (and every
// follow-up on the futures it yields) travels over that channel:
//
//	// send this request over LoRa even if a TCP link also exists
//	laila.Request(gid, "lora").central.memory.remember(eid)
//
// *policy_id* is the ``global_id`` of a peer registered via Add_peer.
// *comm_protocol* (optional) is a *communication id* -- a registered
// connection's ``global_id`` or a protocol token (``"tcp"``, ``"lora"``,
// ``"ble"`` ...). When ``None`` the proxy uses the first transport
// holding the peer.
//
// Raises ConnectionError if *policy_id* is not a connected peer. (Every
// registered peer is a PeerProxy -- the ``[name]``-capable
// RemotePolicyProxy subclass -- so that is the static return type.)
func Request(policy_id any, comm_protocol ...any) (*PeerProxy, error) {
	v, _ := Remote_policies_.Get(py.Str(policy_id))
	proxy, _ := v.(*PeerProxy)
	if proxy == nil {
		return nil, py.ConnectionError.Newf(
			"Unknown peer %s: connect first with laila.add_peer().", py.Repr(py.Str(policy_id)))
	}
	if len(comm_protocol) > 0 && !py.IsNone(comm_protocol[0]) {
		return proxy.Via(comm_protocol[0]), nil
	}
	return proxy, nil
}

// Get_relay is ``laila.relay(channel, name=None, *, timeout=None)``: iterate
// the messages of a peer stream channel on the calling thread. (The
// function name collides with the Relay class under the port's naming
// rule, so -- like ``laila.logger`` -> Get_logger -- it is exposed as
// Get_relay.)
//
// The streaming companion to Request. Given a Channel (from
// ``laila.peers[gid][name]``) -- or a peer ``global_id`` plus a channel
// *name* -- it returns the channel's Relay, a plain blocking iterator
// yielding one StreamEntry per sender-side ``send()``:
//
//	video, _ := laila.Peers()[gid]["video"]
//	for entry := range laila.Get_relay(video, nil).Iter() {   // or laila.Get_relay(gid, "video")
//	    access_unit := entry.Data()                  // bytes, exactly one send()
//	    fmt.Println(entry.Stream.Seq, entry.Stream.Arrived_at)
//	}
//
// Streams are opaque bytes; laila does not interpret them. Each entry is
// a local in-memory constant (fresh ``global_id``), not memorized or
// pooled; pass it to Memorize if you want it stored.
//
// *channel* is a Channel object, or the peer ``global_id`` when *name* is
// given. *name* (optional) is the channel name, required when *channel*
// is a peer id. *timeout* (keyword, optional) is the per-message wait;
// ``None`` blocks until a message arrives or the channel closes,
// otherwise TimeoutError is raised when nothing arrives within *timeout*
// seconds.
//
// Returns the channel's single relay (the same object on every call; two
// threads iterating it split the packets).
//
// Raises ConnectionError if the peer is unknown, its transport has no
// stream lanes (e.g. the WebSocket ``tcpip`` protocol -- use ``tcp://``),
// or the lane could not be opened; RuntimeError if the active policy is a
// remote proxy (morph mode): streams are consumed where the carrier lives
// and are not forwarded.
//
// Iteration ends when the channel closes (peer loss, remote/local
// ``close()``, ``stop()``, Terminate). After the peer reconnects, re-index
// ``laila.peers[gid][name]`` and call Relay again; old channels stay
// closed. Not related to the 3-party memory *relay* helpers
// (``memory._relay_memorize`` / ``_relay_remember``), which move entries
// between pools.
func Get_relay(channel any, name any, kw ...py.Kw) (*Relay, error) {
	k := py.MergeKw(kw...)
	var timeout *float64
	if t, ok := k["timeout"]; ok && !py.IsNone(t) {
		f, err := py.Float(t)
		if err != nil {
			return nil, err
		}
		timeout = &f
	}

	if ch, ok := channel.(*Channel); ok {
		if !py.IsNone(name) {
			return nil, py.TypeError.New("Pass either a Channel or (peer_id, name), not both.")
		}
		return ch.Relay(timeout), nil
	}
	if py.IsNone(name) {
		return nil, py.TypeError.New("laila.relay(peer_id, name) requires a channel name.")
	}
	active, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	if is_remote_policy_proxy_(active) {
		return nil, py.RuntimeError.New(
			"laila.relay needs a local active policy; the active policy is a remote " +
				"proxy (morph mode). Activate the local policy that holds the peer.")
	}
	peer_gid, err := Resolve_policy_ref_(channel)
	if err != nil {
		return nil, err
	}
	comm, err := active_communication_()
	if err != nil {
		return nil, err
	}
	v, _ := comm.Peers.Get(py.Str(peer_gid))
	proxy, _ := v.(*PeerProxy)
	if proxy == nil {
		return nil, py.ConnectionError.Newf(
			"Unknown peer %s: connect first with laila.add_peer().", py.Repr(py.Str(peer_gid)))
	}
	ch, err := proxy.Channel(py.Str(name))
	if err != nil {
		return nil, err
	}
	return ch.Relay(timeout), nil
}

// Memory is the ``laila.memory`` property: the active policy's
// ``central.memory`` -- memorize/remember/forget, pools.
func Memory() (any, error) { return active_central_("memory") }

// Command is the ``laila.command`` property: the active policy's
// ``central.command`` -- taskforces, submit, futures.
func Command() (any, error) { return active_central_("command") }

// Peers is the ``laila.peers`` property: mapping of ``global_id`` ->
// RemotePolicyProxy for every connected peer.
func Peers() (any, error) {
	comm, err := active_central_("communication")
	if err != nil {
		return nil, err
	}
	return py.Getattr(comm, "peers")
}

// Local_policies is the ``laila.local_policies`` property: all local
// policies on this machine, keyed by ``global_id``.
//
// A *local* policy is one whose subsystems live in this process. At any
// time at most one is active, but several may coexist (e.g. for testing
// peer-to-peer flows in a single process).
func Local_policies() *py.Dict { return Local_policies_ }

// Remote_policies is the ``laila.remote_policies`` property: all remote
// peer policies, keyed by ``global_id``.
//
// Each value is a RemotePolicyProxy that translates attribute access into
// RPC calls against the peer. Membership is updated automatically by
// handshake / disconnect events on the underlying communication
// protocols.
func Remote_policies() *py.Dict { return Remote_policies_ }

// Universe is the ``laila.universe`` property: union of local and remote
// policies, keyed by ``global_id``.
//
// Useful when you have a gid in hand and don't care whether the owner is
// local or remote. Locals take precedence when a gid appears in both maps
// (only possible when peering local policies in the same process for
// tests).
func Universe() *py.Dict {
	out := py.NewDict()
	for _, it := range Remote_policies_.Items() {
		out.Set(it.Key, it.Value)
	}
	for _, it := range Local_policies_.Items() {
		out.Set(it.Key, it.Value)
	}
	return out
}

// Alpha_pool is the ``laila.alpha_pool`` property: the active policy's
// default (alpha) pool instance.
//
// The alpha pool is the destination for ``laila.Memorize(...)`` when no
// explicit ``pool_id`` / ``pool_nickname`` is provided, and the cache
// target for ``laila.Remember(..., persist=True)``.
func Alpha_pool() (any, error) {
	mem, err := Memory()
	if err != nil {
		return nil, err
	}
	m := As_central_memory_(mem)
	return m.router_().Pools.Getitem(m.Alpha_pool)
}

// Set_logger is the ``laila.logger = value`` setter: replace the logger
// singleton with a fully-built Logger. Resets the existing singleton first
// so any cached references (e.g. inside CLI-capable models that captured
// Get_logger() during validation) become stale and re-resolve on next
// access. The ``laila.logger`` getter is Get_logger (the property name
// collides with the Logger class under the port's naming rule).
func Set_logger(value *Logger) {
	Logger_reset_singleton()
	Logger_set_singleton_(Logger_class, value)
}

// The package installs a null logging handler at import time so that
// applications which never call Enable_logging do not see "No handlers
// could be found" warnings from the standard logging machinery.
func init() { Install_null_handler_() }

// DefaultPolicy is the class descriptor behind ``DefaultPolicy`` (nil until
// the policy module is linked).
var DefaultPolicy *py.Class

// New_DefaultPolicy is ``DefaultPolicy()``.
func New_DefaultPolicy(kw ...py.Kw) (any, error) {
	if DefaultPolicy == nil {
		return nil, py.RuntimeError.New("DefaultPolicy is not available in this build of laila-go")
	}
	return newInstance(DefaultPolicy, py.MergeKw(kw...))
}

// DefaultPool is the class descriptor behind ``DefaultPool`` (nil until
// the pool module is linked).
var DefaultPool *py.Class

// New_DefaultPool is ``DefaultPool(**kw)``.
func New_DefaultPool(kw ...py.Kw) (any, error) {
	if DefaultPool == nil {
		return nil, py.RuntimeError.New("DefaultPool is not available in this build of laila-go")
	}
	return newInstance(DefaultPool, py.MergeKw(kw...))
}
