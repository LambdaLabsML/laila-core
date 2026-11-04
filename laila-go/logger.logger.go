package laila

import (
	"fmt"
	"os"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// logger/logger.py -- LAILA Logger singleton.
//
// The Logger is a top-level subsystem (sibling to policy, data, entry)
// that emits structured log records from anywhere in the package. It has
// two sinks:
//
//  1. The standard library ``logging`` hierarchy rooted at the "laila"
//     logger (py.Get_logger). StreamHandler and/or FileHandler are
//     attached on Logger.Start; downstream code can route the "laila"
//     tree anywhere with stdlib idioms.
//  2. An optional pool sink: when ``pool_nickname`` (or ``pool_id``) is
//     set, every record is wrapped in an Entry and written into the named
//     pool. A thread-local recursion guard prevents the sink's own write
//     from generating another record.
//
// The class enforces a process-wide singleton: ``Logger()`` (New_Logger)
// always returns the same instance, mirroring how laila exposes
// ``laila.logger`` as a single global handle.

// LAILA_LOGGER_NAME_ is ``_LAILA_LOGGER_NAME``: the stdlib channel.
const LAILA_LOGGER_NAME_ = "laila"

// Install_null_handler_ attaches a py.NullHandler to the "laila" root
// once. Mirrors the standard library-author idiom: keep the package quiet
// by default so downstream code doesn't see "no handlers could be found"
// warnings, and let the user opt in via Logger.Start.
func Install_null_handler_() *py.Logger {
	root := py.Get_logger(LAILA_LOGGER_NAME_)
	has_null := false
	for _, h := range root.Handlers() {
		if _, ok := h.(*py.NullHandler); ok {
			has_null = true
			break
		}
	}
	if !has_null {
		root.Add_handler(&py.NullHandler{})
	}
	return root
}

// Logger is the process-wide LAILA logger singleton.
//
// The logger has two independent sinks gated by simple flags:
//
//   - stdout -- a py.StreamHandler writing to stderr, attached on Start
//     when ``display`` is true.
//   - pool   -- when ``pool_nickname`` (or ``pool_id``) is set, every
//     record is wrapped in an Entry and written into that pool.
//
// The two sinks are not mutually exclusive: with ``display=True`` and a
// pool configured, records flow into both. When no pool is set,
// ``display`` is forced to true on Start so logs are not silently lost.
//
// Fields: ``enabled`` (master switch; Emit is a no-op when false),
// ``level`` (stdlib level name, default "DEBUG"), ``format`` (stdlib
// formatter pattern of the console handler), ``display`` (attach a
// stderr StreamHandler), ``pool_nickname`` / ``pool_id`` (pool sink),
// ``capture_traceback`` (error/critical records include the traceback in
// ``extra["traceback"]``).
type Logger struct {
	LAILA_CLI_CAPABLE_CLASS_
	LAILA_IDENTIFIABLE_OBJECT_

	Enabled           bool    `py:"enabled" default:"false"`
	Level             string  `py:"level" default:"DEBUG"`
	Format            string  `py:"format" default:"%(asctime)s [%(levelname)s] %(name)s: %(message)s"`
	Display           bool    `py:"display" default:"false"`
	Pool_nickname     *string `py:"pool_nickname"`
	Pool_id           *string `py:"pool_id"`
	Capture_traceback bool    `py:"capture_traceback" default:"false"`

	Stdlib_root_          *py.Logger   `py:"_stdlib_root"`
	Installed_handlers_   []py.Handler `py:"_installed_handlers"`
	In_sink_              *py.Local    `py:"_in_sink"`
	Initialized_          bool         `py:"_initialized"`
	Last_pool_sink_error_ *string      `py:"_last_pool_sink_error"`
}

// Logger_class is the class descriptor of Logger. ``_scopes`` defaults to
// ``[_LOGGER_SCOPE]`` (PrivateAttr default_factory).
var Logger_class = py.NewClass("Logger", (*Logger)(nil), LAILA_CLI_CAPABLE_CLASS_class, LAILA_IDENTIFIABLE_OBJECT_class).
	Set_private_default("_scopes", func() any { return []string{LOGGER_SCOPE_} }).
	Set_private_default("_installed_handlers", func() any { return []py.Handler{} }).
	Set_private_default("_in_sink", func() any { return py.NewLocal() })

// ---------------------------------------------------------------------------
// Singleton enforcement
// ---------------------------------------------------------------------------

// Logger_singletons_ is the per-class ``_singleton`` ClassVar slot
// (``cls.__dict__.get("_singleton")``: a subclass of Logger gets its own
// slot). Guarded by Logger_singleton_lock_ because the Python attribute
// store is GIL-serialised and the Go port must be race-free too.
var (
	Logger_singleton_lock_ sync.Mutex
	Logger_singletons_     = map[*py.Class]*Logger{}
)

// Logger_singleton_of_ is ``cls.__dict__.get("_singleton")``.
func Logger_singleton_of_(cls *py.Class) *Logger {
	Logger_singleton_lock_.Lock()
	defer Logger_singleton_lock_.Unlock()
	return Logger_singletons_[cls]
}

// Logger_set_singleton_ is ``cls._singleton = value``.
func Logger_set_singleton_(cls *py.Class, value *Logger) {
	Logger_singleton_lock_.Lock()
	defer Logger_singleton_lock_.Unlock()
	if value == nil {
		delete(Logger_singletons_, cls)
		return
	}
	Logger_singletons_[cls] = value
}

// New_Logger is ``Logger(**data)``: ``__new__`` returns the existing
// singleton when there is one, then ``__init__`` either updates the
// model fields passed in *data* on the already-initialised instance or
// runs the full constructor and installs the instance as the singleton.
func New_Logger(kw ...py.Kw) (*Logger, error) {
	return New_Logger_of_(Logger_class, py.MergeKw(kw...))
}

// New_Logger_of_ is New_Logger for cls (Logger or a subclass descriptor).
func New_Logger_of_(cls *py.Class, data py.Kw) (*Logger, error) {
	var self *Logger
	if existing := Logger_singleton_of_(cls); existing != nil {
		self = existing
	} else {
		self = cls.New().(*Logger)
	}
	if err := self.Init_(data); err != nil {
		return nil, err
	}
	return self, nil
}

// Init_ is ``Logger.__init__``.
func (l *Logger) Init_(data py.Kw) error {
	cls := l.Class_()
	if cls == nil {
		cls = Logger_class
	}
	existing := Logger_singleton_of_(cls)
	if existing == l && l.Initialized_ {
		for key, value := range data {
			if cls.Field_by_name(key) != nil && !cls.Field_by_name(key).Private {
				if err := py.Setattr(l, key, value); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := l.LAILA_IDENTIFIABLE_OBJECT_.Init_(data); err != nil {
		return err
	}
	Logger_set_singleton_(cls, l)
	l.Initialized_ = true
	l.Stdlib_root_ = py.Get_logger(LAILA_LOGGER_NAME_)
	Install_null_handler_()
	if l.Enabled {
		l.Start()
	}
	return nil
}

// Model_post_init chains the identifiable hook, then the CLI-capable hook
// (Python MRO: Logger -> _LAILA_CLI_CAPABLE_CLASS ->
// _LAILA_IDENTIFIABLE_OBJECT -> _LAILA_OBJECT).
func (l *Logger) Model_post_init(ctx any) error {
	if err := l.LAILA_IDENTIFIABLE_OBJECT_.Model_post_init(ctx); err != nil {
		return err
	}
	return CLI_CAPABLE_model_post_init(l.Self())
}

// Logger_reset_singleton is ``Logger.reset_singleton()``: tear down
// handlers and clear the singleton slot. Used by Terminate and the test
// suite.
func Logger_reset_singleton() { Logger_reset_singleton_of_(Logger_class) }

// Logger_reset_singleton_of_ is ``cls.reset_singleton()`` for cls.
func Logger_reset_singleton_of_(cls *py.Class) {
	if existing := Logger_singleton_of_(cls); existing != nil {
		func() {
			defer func() { _ = recover() }()
			existing.Stop()
		}()
	}
	Logger_set_singleton_(cls, nil)
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Start installs the stdout handler on the "laila" root, if needed.
//
// The console handler is attached when ``display`` is true. If no pool
// sink is configured (neither ``pool_nickname`` nor ``pool_id``),
// ``display`` is force-set to true first so records always have at least
// one sink.
//
// Idempotent: previously installed handlers are removed first so repeated
// Start calls don't accumulate duplicates. Sets ``enabled`` to true.
func (l *Logger) Start() {
	l.Remove_installed_handlers_()

	root := l.Stdlib_root_
	if root == nil {
		root = py.Get_logger(LAILA_LOGGER_NAME_)
	}
	l.Stdlib_root_ = root
	root.Set_level(Numeric_level(l.Level))

	if l.Pool_nickname == nil && l.Pool_id == nil {
		l.Display = true
	}

	if l.Display {
		formatter := py.New_Formatter(l.Format)
		stream := py.New_StreamHandler(os.Stderr)
		stream.Set_formatter(formatter)
		stream.Set_level(Numeric_level(l.Level))
		root.Add_handler(stream)
		l.Installed_handlers_ = append(l.Installed_handlers_, stream)
	}

	l.Enabled = true
}

// Stop removes installed handlers and disables emission.
func (l *Logger) Stop() {
	l.Remove_installed_handlers_()
	l.Enabled = false
}

// Remove_installed_handlers_ detaches every handler this Logger added to
// the root.
func (l *Logger) Remove_installed_handlers_() {
	if l.Stdlib_root_ == nil {
		l.Stdlib_root_ = py.Get_logger(LAILA_LOGGER_NAME_)
	}
	for _, handler := range append([]py.Handler(nil), l.Installed_handlers_...) {
		func() {
			defer func() { _ = recover() }()
			l.Stdlib_root_.Remove_handler(handler)
			handler.Close()
		}()
	}
	l.Installed_handlers_ = l.Installed_handlers_[:0]
}

// Set_level updates both this Logger's stored level and the root level.
func (l *Logger) Set_level(level any) {
	canonical := Normalize_level(level)
	l.Level = canonical
	if l.Stdlib_root_ != nil {
		l.Stdlib_root_.Set_level(Numeric_level(canonical))
		for _, handler := range l.Installed_handlers_ {
			handler.Set_level(Numeric_level(canonical))
		}
	}
}

// ---------------------------------------------------------------------------
// Emission
// ---------------------------------------------------------------------------

// Emit sends *record* (a dict produced by Build_record) to both sinks,
// honoring the recursion guard.
func (l *Logger) Emit(record *py.Dict) {
	if !l.Enabled {
		return
	}

	record.SetDefault("logger_id", l.Global_id())

	record_level := Numeric_level(record.GetDefault("level", "INFO"))
	threshold := Numeric_level(l.Level)
	if record_level < threshold {
		return
	}

	if py.Truthy(l.In_sink_.GetOr("active", false)) {
		return
	}

	if l.Stdlib_root_ == nil {
		l.Stdlib_root_ = py.Get_logger(LAILA_LOGGER_NAME_)
	}

	message := record.GetDefault("message", nil)
	if !py.Truthy(message) {
		message = record.GetDefault("event", "")
	}
	func() {
		defer func() { _ = recover() }()
		l.Stdlib_root_.Log(record_level, "%s | %s", message, record)
	}()

	if l.Pool_nickname == nil && l.Pool_id == nil {
		return
	}

	l.In_sink_.Set("active", true)
	defer l.In_sink_.Set("active", false)
	if err := l.Memorize_record_(record); err != nil {
		l.Last_pool_sink_error_ = py.Ptr(py.Repr(err))
		func() {
			defer func() { _ = recover() }()
			l.Stdlib_root_.Warning("laila.logger pool sink failed: %r", err)
		}()
	}
}

// Memorize_record_ wraps *record* in an Entry and persists it into the
// configured pool.
//
// The entry is serialized through the same Record / transformations
// pipeline that Memorize uses, so it round-trips through Remember exactly
// like any other entry. The actual write bypasses Memorize and the
// taskforce machinery so that the worker futures and routing events
// spawned by a normal memorize call do not themselves generate additional
// log records (which would recurse forever).
func (l *Logger) Memorize_record_(record *py.Dict) (err error) {
	defer py.Recover(&err)

	nickname := fmt.Sprintf("log-%s-%s-%d",
		py.Str(record.GetDefault("ts_unix", "")),
		py.Str(record.GetDefault("event", "")),
		py.Id(record))
	entry, err := Constant(record, py.Kw{"nickname": nickname})
	if err != nil {
		return err
	}

	policy, err := Get_active_policy()
	if err != nil {
		return err
	}
	router, err := py.Getattr_path(policy, []string{"central", "memory", "pool_router"})
	if err != nil {
		return err
	}
	pools := getattr(router, "pools")
	var pool any
	if l.Pool_id != nil && mappingContains(pools, *l.Pool_id) {
		pool, err = py.Getitem(pools, *l.Pool_id)
		if err != nil {
			return err
		}
	} else if l.Pool_nickname != nil {
		// ``pools_nicknames.get(nick)`` / ``pools.get(gid)``: a direct
		// mapping lookup (dynamic dispatch would land on the Go
		// ``(*py.Dict).Get`` and hand back its ``(value, ok)`` pair).
		nicknames := getattr(router, "pools_nicknames")
		if mappingContains(nicknames, *l.Pool_nickname) {
			gid, err := py.Getitem(nicknames, *l.Pool_nickname)
			if err != nil {
				return err
			}
			if !py.IsNone(gid) && mappingContains(pools, py.Str(gid)) {
				pool, err = py.Getitem(pools, gid)
				if err != nil {
					return err
				}
			}
		}
	}
	if py.IsNone(pool) {
		return py.RuntimeError.Newf(
			"logger pool sink not found (pool_id=%s, pool_nickname=%s)",
			py.Repr(py.Deref_any(l.Pool_id)), py.Repr(py.Deref_any(l.Pool_nickname)))
	}

	recordCls, ok := py.Class_by_name("Record")
	if !ok {
		return py.RuntimeError.New("Record is not available in this build of laila-go")
	}
	policy_gid := getattr(policy, "global_id")
	record_wrapper, err := newInstance(recordCls, py.Kw{
		"entry":    entry,
		"recorder": policy_gid,
		"borrower": policy_gid,
	})
	if err != nil {
		return err
	}
	// Log records are append-only and never looked up by evolution or
	// creation timestamp, so bypass the pool's index-maintaining ``write``
	// wrapper: indexing every log line would cost an extra shard write per
	// record on the sink pool.
	serialized, err := callm(record_wrapper, "serialize", getattr(pool, "transformations"))
	if err != nil {
		return err
	}
	_, err = callm(pool, "_write", getattr(entry, "global_id"), serialized)
	return err
}

// ---------------------------------------------------------------------------
// Convenience emitters (free-form)
// ---------------------------------------------------------------------------

// Debug emits a free-form DEBUG record (``logger.debug(message, **kwargs)``).
func (l *Logger) Debug(message string, kw ...py.Kw) {
	l.Emit(Build_record("log", py.MergeKw(kw...), py.Kw{"level": "DEBUG", "message": message}))
}

// Info emits a free-form INFO record.
func (l *Logger) Info(message string, kw ...py.Kw) {
	l.Emit(Build_record("log", py.MergeKw(kw...), py.Kw{"level": "INFO", "message": message}))
}

// Warning emits a free-form WARNING record.
func (l *Logger) Warning(message string, kw ...py.Kw) {
	l.Emit(Build_record("log", py.MergeKw(kw...), py.Kw{"level": "WARNING", "message": message}))
}

// Error emits a free-form ERROR record.
func (l *Logger) Error(message string, kw ...py.Kw) {
	l.Emit(Build_record("log", py.MergeKw(kw...), py.Kw{"level": "ERROR", "message": message}))
}

// Critical emits a free-form CRITICAL record.
func (l *Logger) Critical(message string, kw ...py.Kw) {
	l.Emit(Build_record("log", py.MergeKw(kw...), py.Kw{"level": "CRITICAL", "message": message}))
}

// ---------------------------------------------------------------------------
// Structured emitters used by the rest of the codebase
// ---------------------------------------------------------------------------

// as_sequence_ is ``entries if isinstance(entries, (list, tuple, set,
// frozenset)) else [entries]``.
func as_sequence_(v any) []any {
	switch x := v.(type) {
	case *py.List:
		return x.Items()
	case py.Tuple:
		return []any(x)
	case []any:
		return x
	case *py.Set:
		return x.Items()
	}
	return []any{v}
}

// Record_memorize emits one record per entry being memorized
// (``record_memorize(*, entries, pool, policy)``).
func (l *Logger) Record_memorize(kw ...py.Kw) {
	if !l.Enabled {
		return
	}
	opts := py.MergeKw(kw...)
	entries := as_sequence_(opts.Get("entries", nil))
	pool := opts.Get("pool", nil)
	policy := opts.Get("policy", nil)
	pool_nick := l.Pool_nickname_of_(pool, policy)
	for _, entry := range entries {
		entry_id := getattr(entry, "global_id")
		entry_nick := getattr(getattr(entry, "_constitution"), "nickname")
		policy_id := getattr(policy, "global_id")
		pool_id := getattr(pool, "global_id")
		l.Emit(Build_record("memory.memorize", py.Kw{
			"level":          "INFO",
			"policy_id":      policy_id,
			"pool_id":        pool_id,
			"pool_nickname":  pool_nick,
			"entry_id":       entry_id,
			"entry_nickname": entry_nick,
			"message":        fmt.Sprintf("memorize %s -> %s", py.Str(entry_id), py.Str(pool_id)),
		}))
	}
}

// Record_remember emits one record per entry id being remembered
// (``record_remember(*, entry_ids, pool, policy)``).
func (l *Logger) Record_remember(kw ...py.Kw) {
	if !l.Enabled {
		return
	}
	opts := py.MergeKw(kw...)
	entry_ids := as_sequence_(opts.Get("entry_ids", nil))
	pool := opts.Get("pool", nil)
	policy := opts.Get("policy", nil)
	pool_nick := l.Pool_nickname_of_(pool, policy)
	policy_id := getattr(policy, "global_id")
	pool_id := getattr(pool, "global_id")
	for _, entry_id := range entry_ids {
		eid := entry_id
		if py.Hasattr(entry_id, "global_id") {
			eid = getattr(entry_id, "global_id")
		}
		l.Emit(Build_record("memory.remember", py.Kw{
			"level":         "INFO",
			"policy_id":     policy_id,
			"pool_id":       pool_id,
			"pool_nickname": pool_nick,
			"entry_id":      eid,
			"message":       fmt.Sprintf("remember %s <- %s", py.Str(eid), py.Str(pool_id)),
		}))
	}
}

// Record_forget emits one record per entry id being forgotten
// (``record_forget(*, entry_ids, pool, policy)``).
func (l *Logger) Record_forget(kw ...py.Kw) {
	if !l.Enabled {
		return
	}
	opts := py.MergeKw(kw...)
	entry_ids := as_sequence_(opts.Get("entry_ids", nil))
	pool := opts.Get("pool", nil)
	policy := opts.Get("policy", nil)
	pool_nick := l.Pool_nickname_of_(pool, policy)
	policy_id := getattr(policy, "global_id")
	pool_id := getattr(pool, "global_id")
	for _, entry_id := range entry_ids {
		eid := entry_id
		if py.Hasattr(entry_id, "global_id") {
			eid = getattr(entry_id, "global_id")
		}
		l.Emit(Build_record("memory.forget", py.Kw{
			"level":         "INFO",
			"policy_id":     policy_id,
			"pool_id":       pool_id,
			"pool_nickname": pool_nick,
			"entry_id":      eid,
			"message":       fmt.Sprintf("forget %s -- %s", py.Str(eid), py.Str(pool_id)),
		}))
	}
}

// Record_future_created emits one ``future.created`` record on future
// construction.
func (l *Logger) Record_future_created(future any) {
	if !l.Enabled {
		return
	}
	status := getattr(future, "status")
	if py.IsNone(status) {
		status = "not_started"
	}
	l.Emit(Build_record("future.created", py.Kw{
		"level":           "INFO",
		"future_id":       getattr(future, "global_id"),
		"policy_id":       getattr(future, "policy_id"),
		"taskforce_id":    getattr(future, "taskforce_id"),
		"future_group_id": getattr(future, "future_group_id"),
		"precedence":      getattr(future, "precedence"),
		"purpose":         getattr(future, "purpose"),
		"status":          py.Str(status),
		"message":         fmt.Sprintf("future created (purpose=%s)", py.Str(getattr(future, "purpose"))),
	}))
}

// enum_value_ is ``getattr(status, "value", str(status))``.
func enum_value_(status any) string {
	// ``getattr(status, "value", status)``: enum members expose ``.value``
	// as a method in Go, so call it rather than stringify the bound method.
	switch v := status.(type) {
	case interface{ Value() string }:
		return v.Value()
	case interface{ Value() int }:
		return py.Str(v.Value())
	case interface{ Value() any }:
		return py.Str(v.Value())
	}
	if v := getattr(status, "value"); !py.IsNone(v) {
		if _, isMethod := v.(interface{ Call(args []any, kw py.Kw) (any, error) }); !isMethod {
			return py.Str(v)
		}
	}
	return py.Str(status)
}

// Record_future_transition emits one ``future.status`` record on a state
// transition (``record_future_transition(future, new_status,
// prev_status=None)``).
func (l *Logger) Record_future_transition(future any, new_status any, prev_status ...any) {
	if !l.Enabled {
		return
	}
	new_value := enum_value_(new_status)
	var prev_value any
	if len(prev_status) > 0 && !py.IsNone(prev_status[0]) {
		prev_value = enum_value_(prev_status[0])
	}
	level := "INFO"
	if new_value == "error" || new_value == "cancelled" {
		level = "ERROR"
	}

	extra := py.NewDict()
	if exc, ok := getattr(future, "_exception").(error); ok && exc != nil {
		extra.Set("exc_type", py.Typename(exc))
		extra.Set("exc_repr", py.Repr(exc))
		if l.Capture_traceback {
			extra.Set("traceback", py.Format_exception(exc))
		}
	}

	// ``result_global_id`` forces the lazy Entry wrap of a raw result;
	// acceptable here because we only get this far when logging is on.
	var result_id any
	func() {
		defer func() {
			if r := recover(); r != nil {
				result_id = nil
			}
		}()
		result_id = getattr(future, "result_global_id")
	}()

	l.Emit(Build_record("future.status", py.Kw{
		"level":           level,
		"future_id":       getattr(future, "global_id"),
		"policy_id":       getattr(future, "policy_id"),
		"taskforce_id":    getattr(future, "taskforce_id"),
		"future_group_id": getattr(future, "future_group_id"),
		"precedence":      getattr(future, "precedence"),
		"purpose":         getattr(future, "purpose"),
		"status":          new_value,
		"prev_status":     prev_value,
		"result_id":       result_id,
		"message":         fmt.Sprintf("future %s -> %s", py.Str(getattr(future, "global_id")), new_value),
		"extra":           extra,
	}))
}

// Record_group_future_created emits one ``future.group_created`` record.
func (l *Logger) Record_group_future_created(group any) {
	if !l.Enabled {
		return
	}
	var children []any
	if ids := getattr(group, "future_ids"); !py.IsNone(ids) && py.Truthy(ids) {
		children = py.Iter(ids)
	}
	if children == nil {
		children = []any{}
	}
	l.Emit(Build_record("future.group_created", py.Kw{
		"level":            "INFO",
		"future_id":        getattr(group, "global_id"),
		"policy_id":        getattr(group, "policy_id"),
		"taskforce_id":     getattr(group, "taskforce_id"),
		"child_future_ids": children,
		"message":          fmt.Sprintf("group future created with %d children", len(children)),
	}))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Pool_nickname_of_ reverse-looks-up the nickname registered for *pool*
// under *policy*; nil when unknown.
func (l *Logger) Pool_nickname_of_(pool any, policy any) (nick any) {
	if py.IsNone(pool) || py.IsNone(policy) {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			nick = nil
		}
	}()
	router, err := py.Getattr_path(policy, []string{"central", "memory", "pool_router"})
	if err != nil {
		return nil
	}
	pool_gid := getattr(pool, "global_id")
	for _, it := range mappingItems(getattr(router, "pools_nicknames")) {
		if py.Eq(it.Value, pool_gid) {
			return it.Key
		}
	}
	return nil
}
