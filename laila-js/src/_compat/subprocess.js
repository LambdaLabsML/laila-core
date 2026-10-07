/**
 * Python ``subprocess`` / ``shutil.which`` subset used by the managed-server
 * pools (``mkfs.ext4``, ``mount``, ``initdb``, ``postgres``, ``mongod``,
 * ``redis-server``) and the interop harness.
 */
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { PyException, TimeoutError as PyTimeoutError, FileNotFoundError } from "./errors.js";
import { blocking_wait } from "./threading.js";

export const PIPE = -1;
export const STDOUT = -2;
export const DEVNULL = -3;

export class SubprocessError extends PyException {}

export class CalledProcessError extends SubprocessError {
  constructor(returncode, cmd, output = null, stderr = null) {
    super(`Command '${Array.isArray(cmd) ? JSON.stringify(cmd) : cmd}' returned non-zero exit status ${returncode}.`);
    this.returncode = returncode;
    this.cmd = cmd;
    this.output = output;
    this.stdout = output;
    this.stderr = stderr;
  }
}

export class TimeoutExpired extends SubprocessError {
  constructor(cmd, timeout, output = null, stderr = null) {
    super(`Command '${Array.isArray(cmd) ? JSON.stringify(cmd) : cmd}' timed out after ${timeout} seconds`);
    this.cmd = cmd;
    this.timeout = timeout;
    this.output = output;
    this.stdout = output;
    this.stderr = stderr;
  }
}

export class CompletedProcess {
  constructor(args, returncode, stdout = null, stderr = null) {
    this.args = args;
    this.returncode = returncode;
    this.stdout = stdout;
    this.stderr = stderr;
  }
  check_returncode() {
    if (this.returncode !== 0) throw new CalledProcessError(this.returncode, this.args, this.stdout, this.stderr);
  }
  __repr__() {
    return `CompletedProcess(args=${JSON.stringify(this.args)}, returncode=${this.returncode})`;
  }
}

function _stdio(spec, dflt = "inherit") {
  if (spec === PIPE) return "pipe";
  if (spec === DEVNULL) return "ignore";
  if (spec === STDOUT) return "pipe";
  if (spec === null || spec === undefined) return dflt;
  return spec; // fd / stream
}

/**
 * ``subprocess.run(args, capture_output=False, check=False, timeout=None, input=None,
 *                  cwd=None, env=None, text=False, stdout=None, stderr=None)``
 */
export function run(args, opts = {}) {
  const { capture_output = false, check = false, timeout = null, input = null, cwd = null, env = null, text = false, shell = false } = opts;
  const argv = Array.isArray(args) ? args : [args];
  const stdout = capture_output ? "pipe" : _stdio(opts.stdout);
  const stderr = capture_output ? "pipe" : _stdio(opts.stderr);
  const r = spawnSync(argv[0], argv.slice(1), {
    cwd: cwd ?? undefined,
    env: env ?? process.env,
    input: input ?? undefined,
    timeout: timeout ? timeout * 1000 : undefined,
    stdio: [input !== null ? "pipe" : _stdio(opts.stdin), stdout, stderr],
    shell,
    encoding: text ? "utf8" : "buffer",
    maxBuffer: 1024 * 1024 * 256,
  });
  if (r.error) {
    if (r.error.code === "ENOENT") throw new FileNotFoundError(`[Errno 2] No such file or directory: '${argv[0]}'`);
    if (r.error.code === "ETIMEDOUT") throw new TimeoutExpired(argv, timeout, r.stdout, r.stderr);
    throw new SubprocessError(r.error.message);
  }
  const code = r.status === null ? -(r.signal ? 1 : 0) : r.status;
  const cp = new CompletedProcess(argv, code, stdout === "pipe" ? r.stdout : null, stderr === "pipe" ? r.stderr : null);
  if (check && code !== 0) throw new CalledProcessError(code, argv, cp.stdout, cp.stderr);
  return cp;
}

/** ``subprocess.check_output(args, ...)`` */
export function check_output(args, opts = {}) {
  return run(args, { ...opts, check: true, stdout: PIPE }).stdout;
}

/** ``subprocess.check_call(args, ...)`` */
export function check_call(args, opts = {}) {
  run(args, { ...opts, check: true });
  return 0;
}

/**
 * ``subprocess.Popen(args, stdout=None, stderr=None, stdin=None, cwd=None, env=None)``
 * Non-blocking spawn with blocking ``wait(timeout)`` through the pump.
 */
export class Popen {
  constructor(args, opts = {}) {
    const argv = Array.isArray(args) ? args : [args];
    this.args = argv;
    this.returncode = null;
    this._exited = false;
    const child = spawn(argv[0], argv.slice(1), {
      cwd: opts.cwd ?? undefined,
      env: opts.env ?? process.env,
      stdio: [_stdio(opts.stdin), _stdio(opts.stdout), _stdio(opts.stderr)],
      detached: opts.start_new_session === true,
      shell: opts.shell === true,
    });
    this._child = child;
    this.pid = child.pid;
    this.stdin = child.stdin;
    this.stdout = child.stdout;
    this.stderr = child.stderr;
    this._error = null;
    child.on("error", (e) => {
      this._error = e;
      this._exited = true;
      if (this.returncode === null) this.returncode = -1;
    });
    child.on("exit", (code, signal) => {
      this._exited = true;
      this.returncode = code !== null ? code : -(_signum(signal) || 1);
    });
    // Rule U1: a managed server must not keep the parent alive by itself.
    child.unref();
    if (child.stdout) child.stdout.unref?.();
    if (child.stderr) child.stderr.unref?.();
    if (this._error && this._error.code === "ENOENT") throw new FileNotFoundError(`[Errno 2] No such file or directory: '${argv[0]}'`);
  }
  /** ``poll()`` -> returncode or None */
  poll() {
    return this._exited ? this.returncode : null;
  }
  /** Blocking ``wait(timeout=None)`` */
  wait(timeout = null) {
    if (!this._exited) {
      const ok = blocking_wait(() => this._exited, timeout);
      if (!ok) throw new TimeoutExpired(this.args, timeout);
    }
    return this.returncode;
  }
  async wait_async(timeout = null) {
    if (this._exited) return this.returncode;
    await new Promise((resolve, reject) => {
      const t = timeout ? setTimeout(() => reject(new TimeoutExpired(this.args, timeout)), timeout * 1000) : null;
      this._child.once("exit", () => {
        if (t) clearTimeout(t);
        resolve();
      });
    });
    return this.returncode;
  }
  send_signal(sig) {
    if (!this._exited) {
      try {
        this._child.kill(sig);
      } catch {
        /* already gone */
      }
    }
  }
  terminate() {
    this.send_signal("SIGTERM");
  }
  kill() {
    this.send_signal("SIGKILL");
  }
  /** ``communicate(input=None, timeout=None)`` -> [stdout, stderr] (blocking) */
  communicate(input = null, timeout = null) {
    const out = [];
    const err = [];
    if (this.stdout) this.stdout.on("data", (d) => out.push(d));
    if (this.stderr) this.stderr.on("data", (d) => err.push(d));
    if (input !== null && this.stdin) this.stdin.end(input);
    else if (this.stdin) this.stdin.end();
    this.wait(timeout);
    return [this.stdout ? Buffer.concat(out) : null, this.stderr ? Buffer.concat(err) : null];
  }
  __enter__() {
    return this;
  }
  __exit__() {
    if (!this._exited) this.terminate();
    return false;
  }
}

function _signum(name) {
  const map = { SIGHUP: 1, SIGINT: 2, SIGQUIT: 3, SIGKILL: 9, SIGTERM: 15 };
  return map[name] ?? 0;
}

/** ``shutil.which(cmd)`` */
export function which(cmd) {
  if (cmd.includes("/")) return fs.existsSync(cmd) ? cmd : null;
  const dirs = (process.env.PATH ?? "").split(path.delimiter);
  const extra = ["/usr/lib/postgresql", "/usr/local/pgsql/bin", "/sbin", "/usr/sbin"];
  for (const d of [...dirs, ...extra]) {
    if (!d) continue;
    const p = path.join(d, cmd);
    try {
      fs.accessSync(p, fs.constants.X_OK);
      if (fs.statSync(p).isFile()) return p;
    } catch {
      /* next */
    }
  }
  return null;
}

export { PyTimeoutError as _TimeoutError };
