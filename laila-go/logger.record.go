package laila

import (
	"strings"
	"time"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// logger/record.py -- structured log record builder for the LAILA Logger
// singleton.
//
// A laila log record is a JSON-trivial dict with a *fixed* schema of
// optional well-known fields (``policy_id``, ``pool_id``, ``future_id``,
// ``status``, ...) plus a free-form ``extra`` bag. Records are always
// composed of strings, numbers, lists and dicts only; time-stamped with an
// ISO-8601 string (``ts``, UTC, ``Z`` suffix) and a Unix epoch float
// (``ts_unix``); and carry a normalised string ``level``.

// LEVEL_NUMERIC_ is ``_LEVEL_NUMERIC``: canonical level name -> number.
var LEVEL_NUMERIC_ = map[string]int{
	"DEBUG":    10,
	"INFO":     20,
	"WARNING":  30,
	"ERROR":    40,
	"CRITICAL": 50,
}

// level_names_ordered_ keeps _LEVEL_NUMERIC's insertion order for the
// numeric -> name search.
var level_names_ordered_ = []string{"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

// Normalize_level coerces a level (str or int) into a canonical upper-case
// name; unknown values fall back to "INFO".
func Normalize_level(level any) string {
	switch x := level.(type) {
	case int:
		for _, name := range level_names_ordered_ {
			if LEVEL_NUMERIC_[name] == x {
				return name
			}
		}
		return "INFO"
	case int64:
		return Normalize_level(int(x))
	case string:
		upper := strings.ToUpper(x)
		if _, ok := LEVEL_NUMERIC_[upper]; ok {
			return upper
		}
	}
	return "INFO"
}

// Numeric_level returns the integer logging level for a name or numeric
// value.
func Numeric_level(level any) int { return LEVEL_NUMERIC_[Normalize_level(level)] }

// Coerce_id_ converts any identifiable-like object to a string global_id
// (nil stays nil).
func Coerce_id_(value any) any {
	if py.IsNone(value) {
		return nil
	}
	if s, ok := value.(string); ok {
		return s
	}
	if g, ok := value.(interface{ Global_id() string }); ok {
		return g.Global_id()
	}
	if gid := getattr(value, "global_id"); !py.IsNone(gid) {
		return py.Str(gid)
	}
	return py.Str(value)
}

// Build_record builds a structured LAILA log record dict
// (``build_record(event, *, level="INFO", message=None, policy_id=None,
// pool_id=None, pool_nickname=None, entry_id=None, entry_nickname=None,
// future_id=None, future_group_id=None, precedence=None, purpose=None,
// taskforce_id=None, logger_id=None, status=None, prev_status=None,
// result_id=None, child_future_ids=None, child_results=None, extra=None)``).
//
// All ``*_id`` fields accept either a string global_id or any identifiable
// instance and are normalised to strings. Unspecified optional fields are
// omitted so records stay compact. The result always contains ``ts``,
// ``ts_unix``, ``level``, ``event`` and ``extra``.
func Build_record(event string, kw ...py.Kw) *py.Dict {
	opts := py.MergeKw(kw...)
	now := time.Now()
	record := py.NewDict()
	record.Set("ts", strings.Replace(py.Isoformat(now.UTC()), "+00:00", "Z", 1))
	record.Set("ts_unix", float64(now.UnixNano())/1e9)
	record.Set("level", Normalize_level(opts.Get("level", "INFO")))
	record.Set("event", event)
	extra := py.NewDict()
	if e := opts.Get("extra", nil); !py.IsNone(e) && py.Truthy(e) {
		if d, ok := asDict(e); ok {
			extra = d.Copy()
		}
	}
	record.Set("extra", extra)
	if message := opts.Get("message", nil); !py.IsNone(message) {
		record.Set("message", message)
	}

	optional_ids := []string{
		"policy_id", "pool_id", "pool_nickname", "entry_id", "entry_nickname",
		"future_id", "future_group_id", "precedence", "purpose", "taskforce_id",
		"logger_id", "peer_id", "result_id",
	}
	for _, key := range optional_ids {
		value := opts.Get(key, nil)
		if py.IsNone(value) {
			continue
		}
		switch key {
		case "pool_nickname", "entry_nickname", "purpose":
			record.Set(key, py.Str(value))
		default:
			record.Set(key, Coerce_id_(value))
		}
	}

	if status := opts.Get("status", nil); !py.IsNone(status) {
		record.Set("status", py.Str(status))
	}
	if prev := opts.Get("prev_status", nil); !py.IsNone(prev) {
		record.Set("prev_status", py.Str(prev))
	}

	if children := opts.Get("child_future_ids", nil); !py.IsNone(children) {
		out := py.NewList()
		for _, c := range py.Iter(children) {
			out.Append(Coerce_id_(c))
		}
		record.Set("child_future_ids", out)
	}
	if results := opts.Get("child_results", nil); !py.IsNone(results) {
		out := py.NewList()
		for _, c := range py.Iter(results) {
			out.Append(Coerce_id_(c))
		}
		record.Set("child_results", out)
	}
	return record
}
