/**
 * Python stdlib ``logging`` subset: hierarchical loggers, levels, handlers,
 * formatter, propagation. laila's ``Logger`` sink writes to the ``"laila"``
 * stdlib logger, and tests capture it with ``assertLogs``-style handlers.
 */
import { format } from "node:util";
import { ValueError, TypeError as PyTypeError } from "./errors.js";
import { ts_to_iso_z } from "./datetime.js";
import { str } from "./pytypes.js";
import { repr } from "./pyrepr.js";

export const CRITICAL = 50;
export const FATAL = CRITICAL;
export const ERROR = 40;
export const WARNING = 30;
export const WARN = WARNING;
export const INFO = 20;
export const DEBUG = 10;
export const NOTSET = 0;

const _level_names = new Map([
  [CRITICAL, "CRITICAL"],
  [ERROR, "ERROR"],
  [WARNING, "WARNING"],
  [INFO, "INFO"],
  [DEBUG, "DEBUG"],
  [NOTSET, "NOTSET"],
]);
const _name_levels = new Map([...[..._level_names.entries()].map(([k, v]) => [v, k]), ["WARN", WARNING], ["FATAL", CRITICAL]]);

export function getLevelName(level) {
  if (typeof level === "string") return _name_levels.has(level) ? _name_levels.get(level) : `Level ${level}`;
  return _level_names.get(level) ?? `Level ${level}`;
}

function _check_level(level) {
  if (typeof level === "number") return level;
  if (typeof level === "string") {
    if (!_name_levels.has(level)) throw new ValueError(`Unknown level: '${level}'`);
    return _name_levels.get(level);
  }
  throw new PyTypeError(`Level not an integer or a valid string: ${level}`);
}

export class LogRecord {
  constructor(name, level, msg, args, exc_info = null, extra = null) {
    this.name = name;
    this.levelno = level;
    this.levelname = getLevelName(level);
    this.msg = msg;
    this.args = args;
    this.exc_info = exc_info;
    this.created = Date.now() / 1000;
    this.process = process.pid;
    this.threadName = "MainThread";
    if (extra) Object.assign(this, extra);
  }
  getMessage() {
    const msg = String(this.msg);
    if (!this.args || this.args.length === 0) return msg;
    // Python %-formatting (subset: %s %d %r %f, %(name)s)
    if (this.args.length === 1 && this.args[0] && typeof this.args[0] === "object" && !Array.isArray(this.args[0]) && /%\(\w+\)/.test(msg)) {
      return msg.replace(/%\((\w+)\)([sdrf])/g, (_m, k, c) => (c === "r" ? repr(this.args[0][k]) : str(this.args[0][k])));
    }
    let i = 0;
    return msg.replace(/%([sdrfi])/g, (_m, c) => {
      const v = this.args[i++];
      if (c === "r") return repr(v);
      return str(v);
    });
  }
}

export class Formatter {
  constructor(fmt = null, datefmt = null) {
    this._fmt = fmt ?? "%(message)s";
    this.datefmt = datefmt;
  }
  formatTime(record) {
    return ts_to_iso_z(record.created);
  }
  format(record) {
    const message = record.getMessage();
    let s = this._fmt.replace(/%\((\w+)\)([-\d.]*)([sdf])/g, (_m, k) => {
      if (k === "message") return message;
      if (k === "asctime") return this.formatTime(record);
      return String(record[k]);
    });
    if (record.exc_info) s += "\n" + (record.exc_info.stack ?? String(record.exc_info));
    return s;
  }
}

const _default_formatter = new Formatter();

export class Handler {
  constructor(level = NOTSET) {
    this.level = _check_level(level);
    this.formatter = null;
    this.filters = [];
  }
  setLevel(level) {
    this.level = _check_level(level);
  }
  setFormatter(fmt) {
    this.formatter = fmt;
  }
  addFilter(f) {
    this.filters.push(f);
  }
  filter(record) {
    return this.filters.every((f) => (typeof f === "function" ? f(record) : f.filter(record)));
  }
  format(record) {
    return (this.formatter ?? _default_formatter).format(record);
  }
  handle(record) {
    if (record.levelno < this.level) return false;
    if (!this.filter(record)) return false;
    this.emit(record);
    return true;
  }
  emit(_record) {}
  flush() {}
  close() {}
}

export class NullHandler extends Handler {
  handle() {
    return false;
  }
  emit() {}
}

export class StreamHandler extends Handler {
  constructor(stream = null, level = NOTSET) {
    super(level);
    this.stream = stream ?? process.stderr;
    this.terminator = "\n";
  }
  emit(record) {
    try {
      this.stream.write(this.format(record) + this.terminator);
    } catch {
      /* handleError: ignore */
    }
  }
}

/** Handler that keeps records in memory (``assertLogs`` / tests). */
export class MemoryHandler extends Handler {
  constructor(level = NOTSET) {
    super(level);
    this.records = [];
    this.output = [];
  }
  emit(record) {
    this.records.push(record);
    this.output.push(`${record.levelname}:${record.name}:${record.getMessage()}`);
  }
  clear() {
    this.records = [];
    this.output = [];
  }
}

const _LOG_KWARGS = new Set(["exc_info", "extra", "stack_info", "stacklevel"]);

/**
 * The trailing options object of ``log(level, msg, *args, **kwargs)``: a
 * plain object whose keys are *all* logging keyword names (a positional
 * dict argument such as a laila record has other keys and is left alone).
 */
function _is_log_kwargs(last) {
  if (!last || typeof last !== "object" || Array.isArray(last) || Object.getPrototypeOf(last) !== Object.prototype) return false;
  const keys = Object.keys(last);
  return keys.length > 0 && keys.every((k) => _LOG_KWARGS.has(k));
}

export class Logger {
  constructor(name, level = NOTSET) {
    this.name = name;
    this.level = _check_level(level);
    this.parent = null;
    this.propagate = true;
    this.handlers = [];
    this.disabled = false;
  }
  setLevel(level) {
    this.level = _check_level(level);
  }
  getEffectiveLevel() {
    let l = this;
    while (l) {
      if (l.level) return l.level;
      l = l.parent;
    }
    return NOTSET;
  }
  isEnabledFor(level) {
    if (this.disabled) return false;
    if (_manager.disable >= level) return false;
    return level >= this.getEffectiveLevel();
  }
  addHandler(h) {
    if (!this.handlers.includes(h)) this.handlers.push(h);
  }
  removeHandler(h) {
    const i = this.handlers.indexOf(h);
    if (i >= 0) this.handlers.splice(i, 1);
  }
  hasHandlers() {
    let l = this;
    while (l) {
      if (l.handlers.length) return true;
      if (!l.propagate) break;
      l = l.parent;
    }
    return false;
  }
  getChild(suffix) {
    return getLogger(`${this.name}.${suffix}`);
  }
  log(level, msg, ...args) {
    level = _check_level(level);
    if (this.isEnabledFor(level)) this._log(level, msg, args);
  }
  debug(msg, ...args) {
    if (this.isEnabledFor(DEBUG)) this._log(DEBUG, msg, args);
  }
  info(msg, ...args) {
    if (this.isEnabledFor(INFO)) this._log(INFO, msg, args);
  }
  warning(msg, ...args) {
    if (this.isEnabledFor(WARNING)) this._log(WARNING, msg, args);
  }
  warn(msg, ...args) {
    this.warning(msg, ...args);
  }
  error(msg, ...args) {
    if (this.isEnabledFor(ERROR)) this._log(ERROR, msg, args);
  }
  exception(msg, ...args) {
    let exc_info = null;
    let extra = null;
    const last = args[args.length - 1];
    if (_is_log_kwargs(last)) {
      args = args.slice(0, -1);
      exc_info = last.exc_info ?? null;
      extra = last.extra ?? null;
    }
    if (this.isEnabledFor(ERROR)) this._log(ERROR, msg, args, exc_info ?? true, extra);
  }
  critical(msg, ...args) {
    if (this.isEnabledFor(CRITICAL)) this._log(CRITICAL, msg, args);
  }
  fatal(msg, ...args) {
    this.critical(msg, ...args);
  }
  _log(level, msg, args, exc_info = null, extra = null) {
    // trailing ``{exc_info, extra, stack_info}`` options object (Python kwargs)
    const last = args[args.length - 1];
    if (_is_log_kwargs(last)) {
      args = args.slice(0, -1);
      if (exc_info === null) exc_info = last.exc_info ?? null;
      if (extra === null) extra = last.extra ?? null;
    }
    const record = new LogRecord(this.name, level, msg, args, exc_info instanceof Error ? exc_info : null, extra);
    this.handle(record);
  }
  handle(record) {
    if (this.disabled) return;
    let l = this;
    let found = 0;
    while (l) {
      for (const h of l.handlers) {
        found += 1;
        if (record.levelno >= h.level) h.handle(record);
      }
      if (!l.propagate) break;
      l = l.parent;
    }
    if (found === 0 && lastResort && record.levelno >= lastResort.level) lastResort.handle(record);
  }
  __repr__() {
    return `<Logger ${this.name} (${getLevelName(this.getEffectiveLevel())})>`;
  }
}

class RootLogger extends Logger {
  constructor(level) {
    super("root", level);
  }
}

class _Manager {
  constructor(root) {
    this.root = root;
    this.loggers = new Map();
    this.disable = 0;
  }
  getLogger(name) {
    if (typeof name !== "string") throw new PyTypeError("A logger name must be a string");
    let l = this.loggers.get(name);
    if (l) return l;
    l = new Logger(name);
    this.loggers.set(name, l);
    this._fixup_parents(l);
    return l;
  }
  _fixup_parents(logger) {
    const name = logger.name;
    let i = name.lastIndexOf(".");
    let parent = null;
    while (i > 0 && !parent) {
      const sub = name.slice(0, i);
      const p = this.loggers.get(sub);
      if (p) parent = p;
      i = sub.lastIndexOf(".");
    }
    logger.parent = parent ?? this.root;
    // re-parent existing children of this logger
    for (const [n, l] of this.loggers) {
      if (n !== name && n.startsWith(name + ".")) {
        if (l.parent === logger.parent || (l.parent && l.parent.name.length < name.length)) l.parent = logger;
      }
    }
  }
}

export const root = new RootLogger(WARNING);
const _manager = new _Manager(root);
Logger.manager = _manager;

/** ``logging.lastResort`` (stderr, WARNING) used when no handler is configured. */
export const lastResort = new StreamHandler(process.stderr, WARNING);

/** ``logging.getLogger(name=None)`` */
export function getLogger(name = null) {
  if (name === null || name === undefined || name === "root") return root;
  return _manager.getLogger(name);
}

/** ``logging.basicConfig(level=..., format=..., stream=...)`` */
export function basicConfig(opts = {}) {
  const { level = null, format: fmt = null, stream = null, force = false, handlers = null } = opts;
  if (force) root.handlers = [];
  if (root.handlers.length === 0) {
    const hs = handlers ?? [new StreamHandler(stream)];
    const f = new Formatter(fmt ?? "%(levelname)s:%(name)s:%(message)s");
    for (const h of hs) {
      if (!h.formatter) h.setFormatter(f);
      root.addHandler(h);
    }
  }
  if (level !== null) root.setLevel(level);
}

/** ``logging.disable(level)`` */
export function disable(level = CRITICAL) {
  _manager.disable = _check_level(level);
}

// module-level convenience functions (root logger)
export const debug = (...a) => root.debug(...a);
export const info = (...a) => root.info(...a);
export const warning = (...a) => root.warning(...a);
export const error = (...a) => root.error(...a);
export const critical = (...a) => root.critical(...a);
export const exception = (...a) => root.exception(...a);
export const log = (...a) => root.log(...a);

export { format as _format };
