/**
 * Subprocess peer used by ``routing.test.js`` (port of the ``_REMOTE_SCRIPT``
 * in ``test_peer_routing.py``): a policy with a ``remote-store`` pool holding
 * one entry, listening on a WebSocket (``tcpip``) transport.
 *
 * Prints ``PORT=`` / ``SECRET=`` / ``ENTRY_ID=`` / ``POLICY_ID=`` then ``READY``
 * and idles until killed. ``LAILA_PEER_TRANSPORT=tcp`` switches the listener
 * to the raw TCP transport.
 */
import crypto from "node:crypto";

const laila = (await import("../../../src/index.js")).default;
const { DefaultPolicy, DefaultPool, DefaultTCPIPProtocol, DefaultTCPProtocol } = await import("../../../src/macros/defaults.js");
const { with_ } = await import("../../../src/_compat/contextlib.js");

// Top-level module code runs from a microtask, where blocking waits cannot
// pump the event loop; run the body on a fresh macrotask instead.
setImmediate(() => {
const policy = new DefaultPolicy();
laila.activate_policy(policy);
laila.memory.extend(new DefaultPool(), { pool_nickname: "remote-store" });

const entry = laila.constant({ message: "hello-from-remote" }, { nickname: "remote-entry" });
with_(laila.guarantee, () => {
  laila.memorize(entry, { pool_nickname: "remote-store" });
});

const Transport = process.env.LAILA_PEER_TRANSPORT === "tcp" ? DefaultTCPProtocol : DefaultTCPIPProtocol;
const proto = new Transport({ host: "127.0.0.1", port: 0, peer_secret_key: crypto.randomUUID().replace(/-/g, "") });
laila.communication.add_connection(proto);

process.stdout.write(`PORT=${proto.bound_port}\n`);
process.stdout.write(`SECRET=${proto.peer_secret_key}\n`);
process.stdout.write(`ENTRY_ID=${entry.global_id}\n`);
process.stdout.write(`POLICY_ID=${policy.global_id}\n`);
process.stdout.write("READY\n");

setInterval(() => {}, 1000);
});
