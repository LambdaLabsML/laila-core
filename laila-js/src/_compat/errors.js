/**
 * Python built-in exception hierarchy (the subset laila raises or catches).
 *
 * Each class keeps the Python name so `assertRaises(KeyError)` ports verbatim
 * and error text stays identical. `TypeError` deliberately extends the global
 * JS `TypeError` so a JS-native type failure and a Python-style one are caught
 * by the same clause.
 */
import { repr } from "./pyrepr.js";

export class PyException extends Error {
  /** @param {...any} args */
  constructor(...args) {
    super(args.length === 0 ? "" : args.length === 1 ? _msg(args[0]) : args.map(_msg).join(", "));
    this.name = new.target.name;
    this.args = args;
    // Python's ``raise X from Y`` -> ``__cause__``; JS uses ``cause``.
    this.__cause__ = null;
  }
  /** Python ``str(exc)`` */
  __str__() {
    return this.message;
  }
  /** Python ``repr(exc)`` -> ``Name('message')`` */
  __repr__() {
    return `${this.name}(${this.args.map((a) => repr(a)).join(", ")})`;
  }
  toString() {
    return this.message === "" ? this.name : `${this.name}: ${this.message}`;
  }
}

function _msg(a) {
  if (typeof a === "string") return a;
  if (a instanceof Error) return a.message;
  return String(a);
}

export class LookupError extends PyException {}
export class KeyError extends LookupError {
  constructor(...args) {
    super(...args);
    // ``str(KeyError('x'))`` is ``"'x'"`` (repr of the single argument).
    if (args.length === 1) this.message = repr(args[0]);
  }
}
export class IndexError extends LookupError {}
export class ValueError extends PyException {}
export class UnicodeError extends ValueError {}
export class UnicodeDecodeError extends UnicodeError {}
export class UnicodeEncodeError extends UnicodeError {}
export class JSONDecodeError extends ValueError {
  /** ``e.msg`` -- the bare message without the ``: line N column M (char K)`` suffix. */
  get msg() {
    return this.message.replace(/: line \d+ column \d+ \(char \d+\)$/, "");
  }
}
/** Python ``SyntaxError`` (kept distinct from the JS builtin). */
export class SyntaxError extends PyException {}
/** ``xml.etree.ElementTree.ParseError`` (a ``SyntaxError`` subclass in CPython). */
export class ParseError extends SyntaxError {}
export class ArithmeticError extends PyException {}
export class OverflowError extends ArithmeticError {}
export class ZeroDivisionError extends ArithmeticError {}
export class RuntimeError extends PyException {}
export class NotImplementedError extends RuntimeError {}
export class RecursionError extends RuntimeError {}
export class AttributeError extends PyException {}
export class AssertionError extends PyException {}
export class StopIteration extends PyException {}
export class ImportError extends PyException {}
export class ModuleNotFoundError extends ImportError {}
export class OSError extends PyException {}
/** ``sqlite3.Error`` family (``sqlite3.ProgrammingError`` etc.). */
export class SqliteError extends PyException {}
export class SqliteDatabaseError extends SqliteError {}
export class SqliteProgrammingError extends SqliteDatabaseError {}
export class FileNotFoundError extends OSError {}
export class FileExistsError extends OSError {}
export class PermissionError extends OSError {}
export class IsADirectoryError extends OSError {}
export class NotADirectoryError extends OSError {}
export class ConnectionError extends OSError {}
export class ConnectionRefusedError extends ConnectionError {}
export class ConnectionResetError extends ConnectionError {}
export class BrokenPipeError extends ConnectionError {}
export class TimeoutError extends OSError {}
/** ``concurrent.futures.TimeoutError`` is the builtin ``TimeoutError`` since 3.11. */
export const FutureTimeoutError = TimeoutError;
export class CancelledError extends PyException {}
export class InvalidStateError extends PyException {}
/** ``concurrent.futures.process.BrokenProcessPool`` */
export class BrokenExecutor extends RuntimeError {}
export class BrokenProcessPool extends BrokenExecutor {}
export class KeyboardInterrupt extends PyException {}
export class SystemExit extends PyException {}

/** Python ``TypeError`` -- also a JS ``TypeError`` so both kinds share a catch. */
export class TypeError extends globalThis.TypeError {
  constructor(...args) {
    super(args.length === 0 ? "" : args.map(_msg).join(", "));
    this.name = "TypeError";
    this.args = args;
    this.__cause__ = null;
  }
  __str__() {
    return this.message;
  }
  __repr__() {
    return `TypeError(${this.args.map((a) => repr(a)).join(", ")})`;
  }
}

/**
 * Python ``raise exc from cause`` -- records the cause and returns ``exc`` so
 * callers can write ``throw raise_from(new ValueError("..."), err)``.
 */
export function raise_from(exc, cause) {
  exc.__cause__ = cause;
  if (exc.cause === undefined) exc.cause = cause;
  return exc;
}

/** True for anything Python would catch with ``except Exception``. */
export function is_exception(x) {
  return x instanceof Error;
}

const _ERRNO = {
  ENOENT: [2, "No such file or directory", () => FileNotFoundError],
  EEXIST: [17, "File exists", () => FileExistsError],
  EACCES: [13, "Permission denied", () => PermissionError],
  EPERM: [1, "Operation not permitted", () => PermissionError],
  EISDIR: [21, "Is a directory", () => IsADirectoryError],
  ENOTDIR: [20, "Not a directory", () => NotADirectoryError],
  ECONNREFUSED: [111, "Connection refused", () => ConnectionRefusedError],
  ECONNRESET: [104, "Connection reset by peer", () => ConnectionResetError],
  EPIPE: [32, "Broken pipe", () => BrokenPipeError],
  ETIMEDOUT: [110, "Connection timed out", () => TimeoutError],
  ENOTEMPTY: [39, "Directory not empty", () => OSError],
};

/**
 * Translate a Node ``fs`` / ``net`` error (``err.code === "ENOENT"`` ...)
 * into the ``OSError`` subclass CPython raises, with CPython's message
 * (``[Errno 2] No such file or directory: 'path'``). Non-system errors are
 * returned unchanged.
 */
export function os_error(err, path = undefined) {
  if (!err || typeof err.code !== "string" || !(err.code in _ERRNO)) return err;
  const [errno, strerror, cls] = _ERRNO[err.code];
  const p = path !== undefined ? path : err.path;
  const e = new (cls())(p !== undefined ? `[Errno ${errno}] ${strerror}: ${repr(String(p))}` : `[Errno ${errno}] ${strerror}`);
  e.errno = errno;
  e.strerror = strerror;
  e.filename = p ?? null;
  e.__cause__ = err;
  return e;
}
