/**
 * Subprocess peer used by ``utils.test.js`` (port of ``_child_main`` in
 * ``tests/functional/utils/unit_tests/test_guarantee_multiprocess.py``).
 *
 * Runs a laila policy with a WebSocket (``tcpip``) transport bound to an
 * auto-assigned port and attaches the RPC helpers the parent exercises:
 * ``sleep_and_return`` / ``raise_after`` / ``spawn_group`` /
 * ``spawn_error_group`` / ``finished_future`` / ``ping`` / ``echo`` /
 * ``long_sleep``. Each returns the policy's own ``Future`` / ``GroupFuture``
 * (serialized as a ``__laila_future__`` envelope on the wire), so the parent
 * sees a ``RemoteFuture`` per call.
 *
 * Prints ``HOST=`` / ``PORT=`` / ``SECRET=`` / ``POLICY_ID=`` then ``READY``
 * (the ``ready_q.put`` of the Python child) and idles until it reads a line
 * on stdin or stdin closes (the ``cmd_q.get()`` of the Python child), then
 * stops the transport and shuts the command down.
 */
import readline from "node:readline";

const laila = (await import("../../../src/index.js")).default;
const { DefaultPolicy, DefaultTCPIPProtocol } = await import("../../../src/macros/defaults.js");
const { object_setattr } = await import("../../../src/_compat/pydantic.js");
const { RuntimeError } = await import("../../../src/_compat/errors.js");
const asyncio = await import("../../../src/_compat/asyncio.js");
const { range } = await import("../../../src/_compat/pytypes.js");

// Top-level module code runs from a microtask, where blocking waits cannot
// pump the event loop; run the body on a fresh macrotask instead.
setImmediate(() => {
  const policy = new DefaultPolicy();
  laila.activate_policy(policy);
  const tcp = new DefaultTCPIPProtocol({ host: "127.0.0.1", port: 0 });
  policy.central.communication.add_connection(tcp);

  /** Submit a zero-arg callable and return the resulting future. */
  const _submit = (fn) => policy.central.command.submit([fn], { wait: false });

  // ``_time.sleep(seconds)`` inside a taskforce body -> ``await asyncio.sleep``
  // (a blocking sleep on Node's single thread would stall the taskforce).
  function sleep_and_return(seconds, value) {
    return _submit(async () => {
      await asyncio.sleep(seconds);
      return value;
    });
  }

  function raise_after(seconds, message) {
    const _work = async () => {
      await asyncio.sleep(seconds);
      throw new RuntimeError(message);
    };
    return _submit(_work);
  }

  function spawn_group(n, seconds, value) {
    const tasks = range(n).map((i) => async () => {
      await asyncio.sleep(seconds);
      return [value, i];
    });
    return policy.central.command.submit(tasks, { wait: false });
  }

  function spawn_error_group(n, failing_index, seconds) {
    const _factory = (i) => async () => {
      await asyncio.sleep(seconds);
      if (i === failing_index) throw new RuntimeError(`boom-${i}`);
      return i;
    };
    return policy.central.command.submit(range(n).map(_factory), { wait: false });
  }

  /** Return a future that is already completed by the time wait is called. */
  function finished_future(value) {
    const fut = _submit(() => value);
    fut.wait(null);
    return fut;
  }

  function ping() {
    return "pong";
  }

  function echo(x) {
    return x;
  }

  function long_sleep(seconds) {
    return _submit(async () => {
      await asyncio.sleep(seconds);
      return "done";
    });
  }

  for (const [name, fn] of [
    ["sleep_and_return", sleep_and_return],
    ["raise_after", raise_after],
    ["spawn_group", spawn_group],
    ["spawn_error_group", spawn_error_group],
    ["finished_future", finished_future],
    ["ping", ping],
    ["echo", echo],
    ["long_sleep", long_sleep],
  ]) {
    object_setattr(policy, name, fn);
  }

  process.stdout.write(`HOST=${tcp.host}\n`);
  process.stdout.write(`PORT=${tcp.bound_port}\n`);
  process.stdout.write(`SECRET=${tcp.peer_secret_key}\n`);
  process.stdout.write(`POLICY_ID=${policy.global_id}\n`);
  process.stdout.write("READY\n");

  const keepalive = setInterval(() => {}, 1000);
  let stopping = false;
  const _stop = () => {
    if (stopping) return;
    stopping = true;
    clearInterval(keepalive);
    setImmediate(() => {
      try {
        policy.central.communication.stop();
      } catch {
        /* pass */
      }
      try {
        policy.central.command.shutdown({ wait: false, cancel_pending: true });
      } catch {
        /* pass */
      }
      process.exit(0);
    });
  };

  const rl = readline.createInterface({ input: process.stdin });
  rl.on("line", _stop);
  rl.on("close", _stop);
  process.on("SIGTERM", _stop);
});
