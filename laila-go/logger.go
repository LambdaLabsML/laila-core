package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// logger/__init__.py -- LAILA Logger subsystem.
//
// A top-level singleton (sibling to policy, data, entry) that fans
// structured log records out to two independent sinks:
//
//   - The standard-library logging hierarchy under the LAILA_LOGGER_NAME_
//     channel, with optional stderr streaming via the ``display=True``
//     flag.
//   - A laila pool, by memorize-ing each record as a JSON-shaped entry.
//     The pool can be selected by nickname or global id; missing pool
//     configuration triggers an automatic fall-back to ``display=True`` so
//     records are never silently dropped.
//
// The two sinks are independent and can both be active at once -- a
// typical setup is "always display, also persist into Postgres" so an
// operator can watch the live stream while history accumulates in a pool.
//
// This file holds the convenience top-level functions Get_logger,
// Enable_logging, Disable_logging, Set_log_level -- the same names
// re-exported on the laila package. Logger itself lives in
// logger.logger.go; Build_record / Normalize_level / Numeric_level in
// logger.record.go.

// Get_logger returns the process-wide Logger singleton, creating it
// lazily.
//
// The first call constructs the singleton with default settings (silent:
// no display, no pool). Subsequent calls return the same instance. Use
// Enable_logging to actually start emitting records.
func Get_logger() (*Logger, error) {
	if existing := Logger_singleton_of_(Logger_class); existing != nil {
		return existing, nil
	}
	return New_Logger()
}

// Enable_logging configures and starts the singleton logger
// (``enable_logging(level="DEBUG", *, pool_nickname=None, pool_id=None,
// display=False, capture_traceback=False)``).
//
// The logger has two independent sinks: a stdout py.StreamHandler enabled
// by ``display=True``, and a pool sink enabled by passing
// ``pool_nickname`` (or ``pool_id``). They are not mutually exclusive --
// you can have both at once. When no pool is configured, ``display`` is
// forced to true so records are not silently dropped.
//
// Returns the (now-running) singleton.
func Enable_logging(kw ...py.Kw) (*Logger, error) {
	opts := py.MergeKw(kw...)
	logger, err := Get_logger()
	if err != nil {
		return nil, err
	}
	logger.Level = py.Str(opts.Get("level", "DEBUG"))
	logger.Display = py.Truthy(opts.Get("display", false))
	if pool_nickname := opts.Get("pool_nickname", nil); !py.IsNone(pool_nickname) {
		logger.Pool_nickname = py.Ptr(py.Str(pool_nickname))
	}
	if pool_id := opts.Get("pool_id", nil); !py.IsNone(pool_id) {
		logger.Pool_id = py.Ptr(py.Str(pool_id))
	}
	logger.Capture_traceback = py.Truthy(opts.Get("capture_traceback", false))
	logger.Start()
	return logger, nil
}

// Disable_logging stops the singleton logger if one is alive.
//
// Idempotent: if no logger has been constructed yet (no Get_logger /
// Enable_logging calls), this is a no-op. Closes both sinks.
func Disable_logging() {
	if existing := Logger_singleton_of_(Logger_class); existing != nil {
		existing.Stop()
	}
}

// Set_log_level sets the singleton logger's minimum level.
//
// Creates the singleton lazily if needed. Accepts any of the standard
// string level names ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL").
// Records below the configured level are discarded *before* being shipped
// to either sink.
func Set_log_level(level any) error {
	logger, err := Get_logger()
	if err != nil {
		return err
	}
	logger.Set_level(level)
	return nil
}
