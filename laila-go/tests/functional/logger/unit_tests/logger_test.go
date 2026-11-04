// Port of tests/functional/logger/unit_tests/test_logger.py.
//
// Covers the parts of the suite that only need Phase 1 of the port:
//
//   - record schema and id normalization
//   - singleton enforcement and reset semantics
//   - stdlib handler installation, levels, and idempotent start/stop
//   - free-form emission and the logger_id back-fill
//   - environment mirror under laila.Args.environment.logger
//   - laila.Terminate interaction
//
// The structured-emission, pool-sink and memory-integration tests
// (34-37, 39-52, 57-60, 63, 64) need policies, pools and futures; they
// are ported alongside those modules.
package unit_tests

import (
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// capturingHandler is the test's ``_CapturingHandler``: a stdlib handler
// that stores every record it receives.
type capturingHandler struct {
	py.BaseHandler
	mu      sync.Mutex
	records []*py.LogRecord
}

func (h *capturingHandler) Handle(r *py.LogRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
}

func (h *capturingHandler) Records() []*py.LogRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*py.LogRecord(nil), h.records...)
}

func (h *capturingHandler) messages() []string {
	var out []string
	for _, r := range h.Records() {
		out = append(out, r.Get_message())
	}
	return out
}

func anyContains(msgs []string, needle string) bool {
	for _, m := range msgs {
		if strings.Contains(m, needle) {
			return true
		}
	}
	return false
}

// captureLoggerRecords attaches a capturing handler to the "laila" root.
func captureLoggerRecords() (*py.Logger, *capturingHandler) {
	root := py.Get_logger(laila.LAILA_LOGGER_NAME_)
	handler := &capturingHandler{}
	handler.Set_level(py.DEBUG)
	root.Add_handler(handler)
	return root, handler
}

// extractRecords pulls the structured record dict out of each captured
// LogRecord (``r.args[1]``).
func extractRecords(capture *capturingHandler, event string) []*py.Dict {
	var out []*py.Dict
	for _, r := range capture.Records() {
		if len(r.Args) < 2 {
			continue
		}
		payload, ok := r.Args[1].(*py.Dict)
		if !ok {
			continue
		}
		if event != "" && py.Str(payload.GetDefault("event", nil)) != event {
			continue
		}
		out = append(out, payload)
	}
	return out
}

// resetLoggerState tears down any singleton from a prior test.
func resetLoggerState() {
	laila.Logger_reset_singleton()
	root := py.Get_logger(laila.LAILA_LOGGER_NAME_)
	for _, h := range root.Handlers() {
		if _, ok := h.(*py.NullHandler); !ok {
			root.Remove_handler(h)
		}
	}
}

func mustLogger(t *testing.T, kw ...py.Kw) *laila.Logger {
	t.Helper()
	l, err := laila.New_Logger(kw...)
	if err != nil {
		t.Fatalf("Logger(): %v", err)
	}
	return l
}

func handlerKinds(root *py.Logger) []string {
	var kinds []string
	for _, h := range root.Handlers() {
		kinds = append(kinds, py.Pytypename(h))
	}
	return kinds
}

type stub struct{ gid string }

func (s *stub) Global_id() string { return s.gid }

// ---------------------------------------------------------------------------
// TestRecordBuilder
// ---------------------------------------------------------------------------

func TestRecordBuilder(t *testing.T) {
	t.Run("01_normalize_level_uppercase_string", func(t *testing.T) {
		for in, want := range map[string]string{"info": "INFO", "DEBUG": "DEBUG", "warning": "WARNING"} {
			if got := laila.Normalize_level(in); got != want {
				t.Errorf("normalize_level(%q) = %q", in, got)
			}
		}
	})
	t.Run("02_normalize_level_numeric", func(t *testing.T) {
		if laila.Normalize_level(py.INFO) != "INFO" || laila.Normalize_level(py.ERROR) != "ERROR" {
			t.Error("numeric normalisation")
		}
	})
	t.Run("03_normalize_level_unknown_falls_back_to_info", func(t *testing.T) {
		if laila.Normalize_level("not-a-level") != "INFO" || laila.Normalize_level(99999) != "INFO" {
			t.Error("fallback")
		}
	})
	t.Run("04_numeric_level_matches_stdlib", func(t *testing.T) {
		if laila.Numeric_level("INFO") != py.INFO || laila.Numeric_level("ERROR") != py.ERROR || laila.Numeric_level("DEBUG") != py.DEBUG {
			t.Error("numeric_level")
		}
	})
	t.Run("05_build_record_minimal_has_required_keys", func(t *testing.T) {
		rec := laila.Build_record("test.event")
		for _, key := range []string{"ts", "ts_unix", "level", "event", "extra"} {
			if _, ok := rec.Get(key); !ok {
				t.Errorf("missing %s", key)
			}
		}
		if rec.GetDefault("event", nil) != "test.event" || rec.GetDefault("level", nil) != "INFO" {
			t.Error("event/level")
		}
		if extra := rec.GetDefault("extra", nil).(*py.Dict); extra.Len() != 0 {
			t.Error("extra not empty")
		}
	})
	t.Run("06_build_record_omits_unset_optional_ids", func(t *testing.T) {
		rec := laila.Build_record("test.event")
		for _, key := range []string{"policy_id", "pool_id", "entry_id", "future_id", "taskforce_id"} {
			if _, ok := rec.Get(key); ok {
				t.Errorf("unexpected %s", key)
			}
		}
	})
	t.Run("07_build_record_normalizes_id_objects_to_strings", func(t *testing.T) {
		s := &stub{"ENTRY:abc:0"}
		rec := laila.Build_record("memory.memorize", py.Kw{
			"policy_id": s, "pool_id": "POOL:def:0", "entry_id": s, "taskforce_id": s,
		})
		if rec.GetDefault("policy_id", nil) != "ENTRY:abc:0" || rec.GetDefault("pool_id", nil) != "POOL:def:0" ||
			rec.GetDefault("entry_id", nil) != "ENTRY:abc:0" || rec.GetDefault("taskforce_id", nil) != "ENTRY:abc:0" {
			t.Errorf("ids: %s", py.Repr(rec))
		}
	})
	t.Run("08_build_record_ts_is_iso_z_suffixed", func(t *testing.T) {
		rec := laila.Build_record("e")
		if !strings.HasSuffix(py.Str(rec.GetDefault("ts", "")), "Z") {
			t.Error("ts suffix")
		}
		if _, ok := rec.GetDefault("ts_unix", nil).(float64); !ok {
			t.Error("ts_unix type")
		}
	})
	t.Run("09_build_record_status_and_prev_status_are_strings", func(t *testing.T) {
		rec := laila.Build_record("future.status", py.Kw{"status": "running", "prev_status": "not_started"})
		if rec.GetDefault("status", nil) != "running" || rec.GetDefault("prev_status", nil) != "not_started" {
			t.Error("status strings")
		}
	})
	t.Run("10_build_record_child_lists_normalize_ids", func(t *testing.T) {
		rec := laila.Build_record("future.group_finished", py.Kw{
			"child_future_ids": []any{&stub{"FUTURE:x:0"}, "FUTURE:y:0"},
			"child_results":    []any{"ENTRY:r1:0"},
		})
		if !py.Eq(rec.GetDefault("child_future_ids", nil), py.NewList("FUTURE:x:0", "FUTURE:y:0")) {
			t.Errorf("child_future_ids: %s", py.Repr(rec.GetDefault("child_future_ids", nil)))
		}
		if !py.Eq(rec.GetDefault("child_results", nil), py.NewList("ENTRY:r1:0")) {
			t.Error("child_results")
		}
	})
	t.Run("11_build_record_is_json_serializable", func(t *testing.T) {
		rec := laila.Build_record("memory.memorize", py.Kw{
			"policy_id": "POLICY:p:0", "pool_id": "POOL:p:0", "entry_id": "ENTRY:e:0",
			"extra": py.DictOf("shape", py.NewList(1, 2, 3), "ok", true),
		})
		encoded, err := py.Json_dumps(rec)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := py.Json_loads(encoded)
		if err != nil {
			t.Fatal(err)
		}
		d := decoded.(*py.Dict)
		if d.GetDefault("event", nil) != "memory.memorize" {
			t.Error("event")
		}
		if !py.Eq(d.GetDefault("extra", nil).(*py.Dict).GetDefault("shape", nil), py.NewList(1, 2, 3)) {
			t.Error("shape")
		}
	})
	t.Run("12_build_record_preserves_extra", func(t *testing.T) {
		rec := laila.Build_record("e", py.Kw{"extra": py.DictOf("a", 1)})
		if !py.Eq(rec.GetDefault("extra", nil), py.DictOf("a", 1)) {
			t.Error("extra")
		}
		rec.GetDefault("extra", nil).(*py.Dict).Set("b", 2)
		rec2 := laila.Build_record("e", py.Kw{"extra": py.DictOf("a", 1)})
		if !py.Eq(rec2.GetDefault("extra", nil), py.DictOf("a", 1)) {
			t.Error("extra copied")
		}
	})
}

// ---------------------------------------------------------------------------
// TestSingletonAndScope
// ---------------------------------------------------------------------------

func TestSingletonAndScope(t *testing.T) {
	setUp := func(t *testing.T) { resetLoggerState(); t.Cleanup(resetLoggerState) }

	t.Run("13_logger_scope_is_logger", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t)
		if !py.Eq(logger.Scopes(), []string{laila.LOGGER_SCOPE_}) {
			t.Errorf("scopes = %v", logger.Scopes())
		}
	})
	t.Run("14_logger_global_id_includes_logger_scope", func(t *testing.T) {
		setUp(t)
		if !strings.Contains(mustLogger(t).Global_id(), "LOGGER") {
			t.Error("global_id")
		}
	})
	t.Run("15_singleton_returned_by_call", func(t *testing.T) {
		setUp(t)
		if mustLogger(t) != mustLogger(t) {
			t.Error("not a singleton")
		}
	})
	t.Run("16_get_logger_lazy_creates", func(t *testing.T) {
		setUp(t)
		laila.Logger_reset_singleton()
		a, err := laila.Get_logger()
		if err != nil {
			t.Fatal(err)
		}
		b, _ := laila.Get_logger()
		if a != b {
			t.Error("get_logger identity")
		}
	})
	t.Run("17_laila_logger_property_returns_singleton", func(t *testing.T) {
		setUp(t)
		a, _ := laila.Get_logger()
		b, _ := laila.Get_logger()
		if a != b {
			t.Error("laila.logger identity")
		}
	})
	t.Run("18_singleton_call_with_kwargs_updates_existing", func(t *testing.T) {
		setUp(t)
		a := mustLogger(t, py.Kw{"level": "WARNING"})
		mustLogger(t, py.Kw{"level": "DEBUG"})
		if a.Level != "DEBUG" {
			t.Errorf("level = %q", a.Level)
		}
	})
	t.Run("19_reset_singleton_releases_handle", func(t *testing.T) {
		setUp(t)
		a := mustLogger(t)
		laila.Logger_reset_singleton()
		if a == mustLogger(t) {
			t.Error("same instance after reset")
		}
	})
	t.Run("20_reset_singleton_stops_handlers", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t)
		logger.Start()
		if !logger.Enabled {
			t.Error("enabled")
		}
		laila.Logger_reset_singleton()
		if mustLogger(t).Enabled {
			t.Error("new logger enabled")
		}
	})
}

// ---------------------------------------------------------------------------
// TestStdlibIntegration
// ---------------------------------------------------------------------------

func TestStdlibIntegration(t *testing.T) {
	setUp := func(t *testing.T) { resetLoggerState(); t.Cleanup(resetLoggerState) }
	root := py.Get_logger(laila.LAILA_LOGGER_NAME_)

	t.Run("21_install_null_handler_idempotent", func(t *testing.T) {
		setUp(t)
		count := func(l *py.Logger) int {
			n := 0
			for _, h := range l.Handlers() {
				if _, ok := h.(*py.NullHandler); ok {
					n++
				}
			}
			return n
		}
		before := count(laila.Install_null_handler_())
		after := count(laila.Install_null_handler_())
		if before != after || before < 1 {
			t.Errorf("before=%d after=%d", before, after)
		}
	})
	t.Run("22_start_attaches_console_handler", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t)
		logger.Start()
		if !anyContains(handlerKinds(root), "StreamHandler") {
			t.Errorf("kinds = %v", handlerKinds(root))
		}
		if !logger.Display {
			t.Error("display not forced")
		}
	})
	t.Run("23_pool_sink_suppresses_console_handler", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t, py.Kw{"pool_nickname": "some-pool"})
		logger.Start()
		if anyContains(handlerKinds(root), "StreamHandler") || logger.Display {
			t.Error("console handler attached with pool sink")
		}
	})
	t.Run("23b_pool_sink_with_display_true_attaches_console", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t, py.Kw{"pool_nickname": "some-pool", "display": true})
		logger.Start()
		if !anyContains(handlerKinds(root), "StreamHandler") {
			t.Error("both sinks should be active")
		}
	})
	t.Run("24_start_is_idempotent_no_duplicate_handlers", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t)
		logger.Start()
		first := len(root.Handlers())
		logger.Start()
		if second := len(root.Handlers()); first != second {
			t.Errorf("handlers %d -> %d", first, second)
		}
	})
	t.Run("25_stop_removes_installed_handlers", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t)
		logger.Start()
		logger.Stop()
		for _, h := range root.Handlers() {
			if _, ok := h.(*py.NullHandler); !ok {
				t.Errorf("leftover handler %s", py.Pytypename(h))
			}
		}
	})
	t.Run("26_set_level_updates_handlers_and_root", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t)
		logger.Start()
		logger.Set_level("DEBUG")
		if root.Level() != py.DEBUG {
			t.Error("root level")
		}
		for _, h := range logger.Installed_handlers_ {
			if h.Level() != py.DEBUG {
				t.Error("handler level")
			}
		}
	})
	t.Run("27_emit_below_threshold_dropped", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t, py.Kw{"level": "ERROR"})
		logger.Start()
		root, capture := captureLoggerRecords()
		defer root.Remove_handler(capture)
		logger.Info("info-msg")
		if anyContains(capture.messages(), "info-msg") {
			t.Error("info passed ERROR threshold")
		}
	})
	t.Run("28_emit_above_threshold_passes", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t, py.Kw{"level": "DEBUG"})
		logger.Start()
		root, capture := captureLoggerRecords()
		defer root.Remove_handler(capture)
		logger.Error("err-msg")
		if !anyContains(capture.messages(), "err-msg") {
			t.Error("error dropped")
		}
	})
	t.Run("29_disabled_logger_emits_nothing", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t)
		root, capture := captureLoggerRecords()
		defer root.Remove_handler(capture)
		logger.Info("should-not-appear")
		if anyContains(capture.messages(), "should-not-appear") {
			t.Error("disabled logger emitted")
		}
	})
	t.Run("30_enable_logging_helper_sets_level_and_starts", func(t *testing.T) {
		setUp(t)
		logger, err := laila.Enable_logging(py.Kw{"level": "DEBUG"})
		if err != nil {
			t.Fatal(err)
		}
		if !logger.Enabled || logger.Level != "DEBUG" {
			t.Error("enable_logging")
		}
	})
	t.Run("31_disable_logging_helper_stops", func(t *testing.T) {
		setUp(t)
		if _, err := laila.Enable_logging(py.Kw{"level": "INFO"}); err != nil {
			t.Fatal(err)
		}
		laila.Disable_logging()
		l, _ := laila.Get_logger()
		if l.Enabled {
			t.Error("still enabled")
		}
	})
	t.Run("32_set_log_level_helper_changes_level", func(t *testing.T) {
		setUp(t)
		if _, err := laila.Enable_logging(py.Kw{"level": "INFO"}); err != nil {
			t.Fatal(err)
		}
		if err := laila.Set_log_level("WARNING"); err != nil {
			t.Fatal(err)
		}
		l, _ := laila.Get_logger()
		if l.Level != "WARNING" {
			t.Errorf("level = %q", l.Level)
		}
	})
	t.Run("38_logger_id_attached_automatically", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t, py.Kw{"level": "DEBUG"})
		logger.Start()
		root, capture := captureLoggerRecords()
		defer root.Remove_handler(capture)
		logger.Info("hi")
		records := extractRecords(capture, "log")
		if len(records) < 1 {
			t.Fatal("no records")
		}
		if records[len(records)-1].GetDefault("logger_id", nil) != logger.Global_id() {
			t.Error("logger_id")
		}
	})
}

// ---------------------------------------------------------------------------
// TestEnvironmentMirror
// ---------------------------------------------------------------------------

func TestEnvironmentMirror(t *testing.T) {
	setUp := func(t *testing.T) { resetLoggerState(); t.Cleanup(resetLoggerState) }

	t.Run("53_logger_construction_writes_env_mirror", func(t *testing.T) {
		setUp(t)
		mustLogger(t, py.Kw{"level": "DEBUG"})
		envAny, _ := laila.Args.Lookup("environment")
		env, ok := envAny.(*py.DotMap)
		if !ok {
			t.Fatalf("environment = %T", envAny)
		}
		dump, ok := env.Lookup("logger")
		if !ok || py.IsNone(dump) {
			t.Fatal("no logger mirror")
		}
		if got, _ := dump.(*py.DotMap).Lookup("level"); got != "DEBUG" {
			t.Errorf("level = %v", got)
		}
	})
	t.Run("54_args_path_for_logger_is_top_level", func(t *testing.T) {
		if laila.SCOPE_TO_ARGS_PATH_["LOGGER"] != "logger" {
			t.Error("path")
		}
	})
	t.Run("55_args_input_injection_pulls_logger_fields", func(t *testing.T) {
		setUp(t)
		laila.Logger_reset_singleton()
		laila.Args.Set("logger", py.Kw{"level": "ERROR", "capture_traceback": true})
		defer laila.Args.Set("logger", py.Kw{})
		logger := mustLogger(t)
		if logger.Level != "ERROR" || !logger.Capture_traceback {
			t.Errorf("level=%q capture=%v", logger.Level, logger.Capture_traceback)
		}
	})
	t.Run("65_singleton_survives_args_round_trip", func(t *testing.T) {
		setUp(t)
		laila.Logger_reset_singleton()
		laila.Args.Set("logger", py.Kw{"level": "DEBUG"})
		defer laila.Args.Set("logger", py.Kw{})
		a := mustLogger(t)
		if a.Level != "DEBUG" {
			t.Error("level")
		}
		if a != mustLogger(t) {
			t.Error("identity")
		}
	})
}

// ---------------------------------------------------------------------------
// TestTerminateInteraction / TestEdgeCases (phase-1 subset)
// ---------------------------------------------------------------------------

func TestTerminateInteraction(t *testing.T) {
	resetLoggerState()
	t.Cleanup(resetLoggerState)
	old := mustLogger(t)
	old.Start()
	laila.Terminate(py.Kw{"wait": true, "cancel_pending": false})
	nw := mustLogger(t)
	if nw == old {
		t.Error("terminate did not reset the singleton")
	}
	if nw.Enabled {
		t.Error("new logger enabled")
	}
}

func TestEdgeCases(t *testing.T) {
	setUp := func(t *testing.T) { resetLoggerState(); t.Cleanup(resetLoggerState) }

	t.Run("61_emit_unset_logger_id_is_filled_in_emit", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t, py.Kw{"level": "DEBUG"})
		logger.Start()
		root, capture := captureLoggerRecords()
		defer root.Remove_handler(capture)
		rec := laila.Build_record("custom.event", py.Kw{"level": "INFO"})
		if _, ok := rec.Get("logger_id"); ok {
			t.Fatal("logger_id preset")
		}
		logger.Emit(rec)
		if rec.GetDefault("logger_id", nil) != logger.Global_id() {
			t.Error("logger_id not filled")
		}
	})
	t.Run("62_set_level_helper_changes_threshold", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t, py.Kw{"level": "ERROR"})
		logger.Start()
		root, capture := captureLoggerRecords()
		defer root.Remove_handler(capture)
		logger.Info("dropped")
		logger.Set_level("DEBUG")
		logger.Info("kept")
		msgs := capture.messages()
		if anyContains(msgs, "dropped") || !anyContains(msgs, "kept") {
			t.Errorf("messages = %v", msgs)
		}
	})
	t.Run("66_recording_missing_pool_does_not_break_emission", func(t *testing.T) {
		setUp(t)
		logger := mustLogger(t, py.Kw{"level": "DEBUG", "pool_nickname": "ghost-pool-not-registered"})
		logger.Start()
		root, capture := captureLoggerRecords()
		defer root.Remove_handler(capture)
		logger.Info("ghost-msg")
		if !anyContains(capture.messages(), "ghost-msg") {
			t.Error("message lost")
		}
		if logger.Last_pool_sink_error_ == nil {
			t.Error("_last_pool_sink_error not set")
		}
	})
}
