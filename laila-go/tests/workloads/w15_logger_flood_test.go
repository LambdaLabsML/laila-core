//go:build workload

// Port of tests/workloads/test_w15_logger_flood.py.
//
// W15 -- logger under flood conditions.
//
// Tens of thousands of records through the stderr and pool sinks, multi-
// threaded emission, level filtering, recursion guard when the sink pool is
// itself subject to logging, sink failure modes, handler accumulation and
// the overhead the pool sink adds to ordinary memorize/remember traffic.
package workloads

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T15_ = 120.0

// logger_reset15 is the ``logger_reset`` fixture.
func logger_reset15(t *testing.T) {
	t.Helper()
	laila.Logger_reset_singleton()
	t.Cleanup(func() {
		func() {
			defer func() { _ = recover() }()
			laila.Disable_logging()
		}()
		laila.Logger_reset_singleton()
		root := py.Get_logger(laila.LAILA_LOGGER_NAME_)
		for _, h := range root.Handlers() {
			if _, ok := h.(*py.NullHandler); !ok {
				root.Remove_handler(h)
			}
		}
	})
}

// stringio15 is ``io.StringIO()``: a goroutine-safe growing buffer.
type stringio15 struct {
	mu sync.Mutex
	sb strings.Builder
}

func (b *stringio15) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.Write(p)
}

// Getvalue is ``buf.getvalue()``.
func (b *stringio15) Getvalue() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.String()
}

// capture_stderr15 is the ``capture_stderr`` fixture: a DEBUG
// StreamHandler on the "laila" root writing into a buffer.
func capture_stderr15(t *testing.T) *stringio15 {
	t.Helper()
	buf := &stringio15{}
	root := py.Get_logger(laila.LAILA_LOGGER_NAME_)
	h := py.New_StreamHandler(buf)
	h.Set_level(py.DEBUG)
	root.Add_handler(h)
	t.Cleanup(func() { root.Remove_handler(h) })
	return buf
}

// sink_pool15 is ``_sink_pool(fresh_policy, kind, directory, nickname="logs")``.
func sink_pool15(t *testing.T, policy *laila.LAILA_IDENTIFIABLE_POLICY_, kind, directory string, nickname ...string) any {
	t.Helper()
	nick := "logs"
	if len(nickname) > 0 {
		nick = nickname[0]
	}
	pool := must(Make_pool(kind, directory))
	check(t, Mem(policy).Extend(pool, py.Kw{"pool_nickname": nick}))
	return pool
}

// log_entries15 is ``_log_entries(pool)``: ``[k for k in pool.keys()]``.
func log_entries15(pool any) []any {
	return must(laila.As_pool_(pool).Keys()).(*py.List).Items()
}

// enable_logging15 is ``enable_logging(level, **kw)``.
func enable_logging15(t *testing.T, level string, kw ...py.Kw) *laila.Logger {
	t.Helper()
	opts := py.MergeKw(kw...)
	opts["level"] = level
	lg, err := laila.Enable_logging(opts)
	if err != nil {
		t.Fatal(err)
	}
	return lg
}

// remember15 is ``unwrap(laila.remember(key, pool_nickname=nick).wait(_T))``.
func remember15(t *testing.T, key any, nick string) any {
	t.Helper()
	return Unwrap(must(Wait(must(laila.Remember(key, py.Kw{"pool_nickname": nick})), T15_)))
}

// memorize15 is ``laila.memorize(Entry.constant(data=v)).wait(_T)``.
func memorize15(t *testing.T, v any) {
	t.Helper()
	must(Wait(must(laila.Memorize(must(laila.Entry_constant(v)))), T15_))
}

// alpha_keys15 is ``len(laila.alpha_pool.keys())``.
func alpha_keys15(t *testing.T) int {
	t.Helper()
	return must(laila.As_pool_(must(laila.Alpha_pool())).Keys()).(*py.List).Len()
}

func TestStderrSink_twenty_thousand_records(t *testing.T) {
	Fresh_policy(t)
	logger_reset15(t)
	capture_stderr := capture_stderr15(t)
	lg := enable_logging15(t, "DEBUG", py.Kw{"display": false})
	assertEq(t, lg.Display, true) // forced on with no pool sink
	n := N_scaled(20_000)
	sw := Timed(func() {
		for i := 0; i < n; i++ {
			lg.Info(fmt.Sprintf("m%d", i), py.Kw{"entry_id": fmt.Sprintf("LAILA:ENTRY:%d", i)})
		}
	})
	assertTrue(t, sw.Elapsed < 60, sw.Elapsed)
	assertEq(t, strings.Count(capture_stderr.Getvalue(), "| {"), n)
}

func TestStderrSink_level_filtering_drops_before_sink(t *testing.T) {
	Fresh_policy(t)
	logger_reset15(t)
	capture_stderr := capture_stderr15(t)
	lg := enable_logging15(t, "WARNING")
	for i := 0; i < 1000; i++ {
		lg.Debug("d")
		lg.Info("i")
	}
	lg.Warning("w")
	lg.Error("e")
	lg.Critical("c")
	out := capture_stderr.Getvalue()
	assertEq(t, strings.Count(out, "| {"), 3)
	assertTrue(t, !strings.Contains(out, "'level': 'DEBUG'"))
}

func TestStderrSink_dynamic_level_change_mid_flood(t *testing.T) {
	Fresh_policy(t)
	logger_reset15(t)
	capture_stderr := capture_stderr15(t)
	lg := enable_logging15(t, "DEBUG")
	for i := 0; i < 2000; i++ {
		if i == 1000 {
			check(t, laila.Set_log_level("ERROR"))
		}
		lg.Info("x")
	}
	assertEq(t, strings.Count(capture_stderr.Getvalue(), "| {"), 1000)
	check(t, laila.Set_log_level("debug"))
	assertEq(t, lg.Level, "DEBUG")
}

func TestStderrSink_multithreaded_emission(t *testing.T) {
	Fresh_policy(t)
	logger_reset15(t)
	capture_stderr := capture_stderr15(t)
	lg := enable_logging15(t, "INFO")

	w := func(i int) error {
		for j := 0; j < N_scaled(2000); j++ {
			lg.Info(fmt.Sprintf("t%d-%d", i, j))
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	assertEq(t, strings.Count(capture_stderr.Getvalue(), "| {"), 8*N_scaled(2000))
}

// stream_handlers15 is ``[h for h in root.handlers if isinstance(h,
// logging.StreamHandler) and not isinstance(h, logging.NullHandler)]``.
func stream_handlers15(root *py.Logger) []py.Handler {
	out := []py.Handler{}
	for _, h := range root.Handlers() {
		if _, ok := h.(*py.NullHandler); ok {
			continue
		}
		switch h.(type) {
		case *py.StreamHandler, *py.FileHandler:
			out = append(out, h)
		}
	}
	return out
}

func TestStderrSink_repeated_start_does_not_accumulate_handlers(t *testing.T) {
	Fresh_policy(t)
	logger_reset15(t)
	for i := 0; i < 100; i++ {
		enable_logging15(t, "INFO", py.Kw{"display": true})
	}
	root := py.Get_logger(laila.LAILA_LOGGER_NAME_)
	streams := stream_handlers15(root)
	assertEq(t, len(streams), 1)
	laila.Disable_logging()
	streams = stream_handlers15(root)
	assertEq(t, len(streams), 0)
}

func TestStderrSink_singleton_semantics(t *testing.T) {
	Fresh_policy(t)
	logger_reset15(t)
	a := must(laila.New_Logger())
	b := must(laila.New_Logger())
	c := must(laila.Get_logger())
	assertTrue(t, a == b && b == c)
	laila.Logger_reset_singleton()
	d := must(laila.Get_logger())
	assertTrue(t, d != a)
}

func TestStderrSink_disabled_logger_is_cheap(t *testing.T) {
	Fresh_policy(t)
	logger_reset15(t)
	lg := must(laila.Get_logger())
	assertEq(t, lg.Enabled, false)
	sw := Timed(func() {
		for i := 0; i < N_scaled(100_000); i++ {
			lg.Info("x")
		}
	})
	assertTrue(t, sw.Elapsed < 5, sw.Elapsed)
}

func TestStderrSink_level_normalisation(t *testing.T) {
	Setup(t)
	assertEq(t, laila.Normalize_level("info"), "INFO")
	assertEq(t, laila.Normalize_level(py.ERROR), "ERROR")
	assertEq(t, laila.Numeric_level("CRITICAL"), 50)
	assertEq(t, laila.Normalize_level("bogus"), "INFO") // documented fallback
}

func TestStderrSink_build_record_shape(t *testing.T) {
	Setup(t)
	r := laila.Build_record(
		"x", py.Kw{"level": "warning", "message": "m", "entry_id": "LAILA:ENTRY:1", "extra": py.DictOf("a", 1)},
	)
	assertTrue(t, py.Eq(must(r.Getitem("level")), "WARNING") && py.Eq(must(r.Getitem("event")), "x") && py.Eq(must(r.Getitem("message")), "m"))
	assertTrue(t, py.Eq(must(r.Getitem("entry_id")), "LAILA:ENTRY:1") && py.Eq(must(r.Getitem("extra")), py.DictOf("a", 1)))
	assertTrue(t, strings.HasSuffix(py.Str(must(r.Getitem("ts"))), "Z") && !r.Contains("pool_id"))
}

// Unprintable15 is the test's ``class Unprintable: def __repr__(self):
// raise RuntimeError("no repr")``.
type Unprintable15 struct{}

// Repr is ``__repr__``: always raises.
func (Unprintable15) Repr() string { panic(py.RuntimeError.New("no repr")) }

func TestStderrSink_bad_record_values_do_not_raise(t *testing.T) {
	Fresh_policy(t)
	logger_reset15(t)
	capture_stderr15(t)
	lg := enable_logging15(t, "DEBUG")

	lg.Info("x", py.Kw{"extra": py.DictOf("u", Unprintable15{})})
	lg.Emit(py.DictOf("level", "INFO", "event", "raw"))
}

func TestPoolSink_records_land_in_pool_and_round_trip(t *testing.T) {
	for _, kind := range []string{"mem", "sqlite"} {
		kind := kind
		t.Run("kind="+kind, func(t *testing.T) {
			policy := Fresh_policy(t)
			logger_reset15(t)
			disk_tmp := Disk_tmp(t)
			pool := sink_pool15(t, policy, kind, disk_tmp)
			defer func() {
				laila.Disable_logging()
				Close_pool(pool)
			}()
			lg := enable_logging15(t, "INFO", py.Kw{"pool_nickname": "logs"})
			n := N_scaled(2000)
			for i := 0; i < n; i++ {
				lg.Info(fmt.Sprintf("rec-%d", i), py.Kw{"entry_id": fmt.Sprintf("LAILA:ENTRY:%d", i)})
			}
			keys := log_entries15(pool)
			assertTrue(t, len(keys) >= n, len(keys))
			// every log record is a plain remember-able entry
			out := remember15(t, keys[0], "logs").(*py.Dict)
			assertTrue(t, py.Eq(must(out.Getitem("event")), "log") && strings.HasPrefix(py.Str(must(out.Getitem("message"))), "rec-"))
			Quiesce(policy)
		})
	}
}

func TestPoolSink_pool_sink_by_pool_id(t *testing.T) {
	policy := Fresh_policy(t)
	logger_reset15(t)
	disk_tmp := Disk_tmp(t)
	pool := sink_pool15(t, policy, "mem", disk_tmp, "byid")
	lg := enable_logging15(t, "INFO", py.Kw{"pool_id": laila.As_pool_(pool).Global_id()})
	lg.Info("a")
	assertEq(t, len(log_entries15(pool)), 1)
}

func TestPoolSink_unknown_sink_does_not_raise(t *testing.T) {
	Fresh_policy(t)
	logger_reset15(t)
	capture_stderr := capture_stderr15(t)
	lg := enable_logging15(t, "INFO", py.Kw{"pool_nickname": "nope"})
	for i := 0; i < 100; i++ {
		lg.Info("x")
	}
	assertTrue(t, lg.Last_pool_sink_error_ != nil)
	assertTrue(t, strings.Contains(capture_stderr.Getvalue(), "pool sink failed"))
}

func TestPoolSink_recursion_guard_when_sink_is_the_alpha_pool(t *testing.T) {
	policy := Fresh_policy(t)
	logger_reset15(t)
	// sink into the alpha pool: memorize -> record -> pool write (bypasses
	// laila.memorize, so no infinite recursion)
	nick := Router(policy).Pools_nicknames
	alpha_gid := laila.As_pool_(must(laila.Alpha_pool())).Global_id()
	var alpha_nick string
	found := false
	for _, it := range nick.Items() {
		if py.Eq(it.Value, alpha_gid) {
			alpha_nick, found = py.Str(it.Key), true
			break
		}
	}
	assertTrue(t, found, "alpha pool has no nickname")
	lg := enable_logging15(t, "INFO", py.Kw{"pool_nickname": alpha_nick})
	before := alpha_keys15(t)
	for i := 0; i < 200; i++ {
		memorize15(t, i)
	}
	Quiesce(policy)
	after := alpha_keys15(t)
	assertTrue(t, after-before >= 400, after-before) // 200 payloads + >=1 record each
	assertTrue(t, after-before < 200*20, after-before)
	lg.Stop()
}

func TestPoolSink_multithreaded_pool_sink(t *testing.T) {
	policy := Fresh_policy(t)
	logger_reset15(t)
	disk_tmp := Disk_tmp(t)
	pool := sink_pool15(t, policy, "mem", disk_tmp)
	lg := enable_logging15(t, "INFO", py.Kw{"pool_nickname": "logs"})

	w := func(i int) error {
		for j := 0; j < N_scaled(1000); j++ {
			lg.Info(fmt.Sprintf("%d-%d", i, j))
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	assertEq(t, len(log_entries15(pool)), 8*N_scaled(1000))
}

func TestPoolSink_pool_sink_with_encrypting_pool(t *testing.T) {
	policy := Fresh_policy(t)
	logger_reset15(t)
	Disk_tmp(t)

	laila.Set_encryption_key(string(py.Fernet_generate_key()))
	pool := must(laila.New_DefaultPool(py.Kw{"transformations": must(laila.Transformation_base64_compression_encryption())}))
	check(t, Mem(policy).Extend(pool, py.Kw{"pool_nickname": "enc"}))
	lg := enable_logging15(t, "INFO", py.Kw{"pool_nickname": "enc"})
	for i := 0; i < 300; i++ {
		lg.Info(fmt.Sprintf("s%d", i))
	}
	keys := log_entries15(pool)
	assertEq(t, len(keys), 300)
	out := remember15(t, keys[len(keys)-1], "enc").(*py.Dict)
	assertTrue(t, strings.HasPrefix(py.Str(must(out.Getitem("message"))), "s"))
	Quiesce(policy)
}

func TestPoolSink_future_lifecycle_records(t *testing.T) {
	policy := Fresh_policy(t)
	logger_reset15(t)
	disk_tmp := Disk_tmp(t)
	pool := sink_pool15(t, policy, "mem", disk_tmp)
	lg := enable_logging15(t, "INFO", py.Kw{"pool_nickname": "logs", "capture_traceback": true})
	f := cpf(must(Cmd(policy).Submit([]any{func() (any, error) { return nil, py.ZeroDivisionError.New("division by zero") }})))
	if _, err := f.Wait(T15_); !errors.Is(err, py.ZeroDivisionError) {
		t.Fatalf("want ZeroDivisionError, got %v", err)
	}
	f.Release()
	Quiesce(policy)
	events := []*py.Dict{}
	for _, k := range log_entries15(pool) {
		rec := remember15(t, k, "logs").(*py.Dict)
		events = append(events, rec)
	}
	kinds := map[string]bool{}
	for _, e := range events {
		kinds[py.Str(must(e.Getitem("event")))] = true
	}
	assertTrue(t, kinds["future.created"] && kinds["future.status"], kinds)
	errs := []*py.Dict{}
	for _, e := range events {
		if py.Eq(must(e.Getitem("event")), "future.status") && py.Eq(must(e.Getitem("level")), "ERROR") {
			errs = append(errs, e)
		}
	}
	assertTrue(t, len(errs) > 0)
	assertTrue(t, strings.Contains(py.Str(must(must(errs[0].Getitem("extra")).(*py.Dict).Getitem("traceback"))), "ZeroDivisionError"))
	lg.Stop()
}

func TestPoolSink_sink_pool_closed_mid_flood(t *testing.T) {
	policy := Fresh_policy(t)
	logger_reset15(t)
	disk_tmp := Disk_tmp(t)
	capture_stderr15(t)
	pool := sink_pool15(t, policy, "sqlite", disk_tmp)
	lg := enable_logging15(t, "INFO", py.Kw{"pool_nickname": "logs"})
	for i := 0; i < 100; i++ {
		lg.Info("x")
	}
	Close_pool(pool)
	for i := 0; i < 100; i++ {
		lg.Info("y") // must not raise
	}
	assertTrue(t, lg.Last_pool_sink_error_ != nil)
}

func TestPoolSink_group_future_records(t *testing.T) {
	policy := Fresh_policy(t)
	logger_reset15(t)
	disk_tmp := Disk_tmp(t)
	pool := sink_pool15(t, policy, "mem", disk_tmp)
	enable_logging15(t, "INFO", py.Kw{"pool_nickname": "logs"})
	tasks := make([]any, 50)
	for i := range tasks {
		tasks[i] = lam(1)
	}
	gf := gfut(must(Cmd(policy).Submit(tasks)))
	must(gf.Wait(T15_))
	gf.Release()
	Quiesce(policy)
	events := []string{}
	for _, k := range log_entries15(pool) {
		events = append(events, py.Str(must(remember15(t, k, "logs").(*py.Dict).Getitem("event"))))
	}
	count := func(name string) int {
		n := 0
		for _, e := range events {
			if e == name {
				n++
			}
		}
		return n
	}
	assertEq(t, count("future.group_created"), 1)
	assertTrue(t, count("future.created") >= 50, count("future.created"))
}

func TestOverhead_pool_sink_memorize_overhead(t *testing.T) {
	policy := Fresh_policy(t)
	logger_reset15(t)
	disk_tmp := Disk_tmp(t)
	n := N_scaled(500)

	run_batch := func() float64 {
		sw := Timed(func() {
			for i := 0; i < n; i++ {
				memorize15(t, i)
			}
		})
		Quiesce(policy)
		return sw.Elapsed
	}

	base := run_batch()
	pool := sink_pool15(t, policy, "mem", disk_tmp)
	enable_logging15(t, "INFO", py.Kw{"pool_nickname": "logs"})
	logged := run_batch()
	n_records := len(log_entries15(pool))
	laila.Disable_logging()
	Close_pool(pool)
	// informative: how many records per memorize, and the slowdown factor
	per_op := float64(n_records) / float64(n)
	assertTrue(t, per_op >= 2, per_op)
	assertTrue(t, logged < base*25+5, base, logged, per_op)
}

func TestOverhead_remember_flood_with_sqlite_sink(t *testing.T) {
	policy := Fresh_policy(t)
	logger_reset15(t)
	disk_tmp := Disk_tmp(t)
	pool := sink_pool15(t, policy, "sqlite", disk_tmp)
	e := must(laila.Entry_constant("v"))
	must(Wait(must(laila.Memorize(e)), T15_))
	enable_logging15(t, "INFO", py.Kw{"pool_nickname": "logs"})
	sw := Timed(func() {
		for i := 0; i < N_scaled(300); i++ {
			must(Wait(must(laila.Remember(e.Global_id())), T15_))
		}
	})
	Quiesce(policy)
	assertTrue(t, sw.Elapsed < 120, sw.Elapsed)
	assertTrue(t, len(log_entries15(pool)) >= N_scaled(300), len(log_entries15(pool)))
	laila.Disable_logging()
	Close_pool(pool)
}

func TestOverhead_record_count_bounded_per_operation(t *testing.T) {
	policy := Fresh_policy(t)
	logger_reset15(t)
	disk_tmp := Disk_tmp(t)
	pool := sink_pool15(t, policy, "mem", disk_tmp)
	enable_logging15(t, "INFO", py.Kw{"pool_nickname": "logs"})
	e := must(laila.Entry_constant(1))
	must(Wait(must(laila.Memorize(e)), T15_))
	Quiesce(policy)
	n1 := len(log_entries15(pool))
	must(Wait(must(laila.Remember(e.Global_id())), T15_))
	Quiesce(policy)
	n2 := len(log_entries15(pool))
	must(Wait(must(laila.Forget(e.Global_id())), T15_))
	Quiesce(policy)
	n3 := len(log_entries15(pool))
	assertTrue(t, 1 <= n1 && n1 <= 12 && 1 <= n2-n1 && n2-n1 <= 12 && 1 <= n3-n2 && n3-n2 <= 12, n1, n2, n3)
	laila.Disable_logging()
}
