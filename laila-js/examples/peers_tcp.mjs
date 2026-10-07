/**
 * Two policies peering over TCP and sharing memory.
 *
 * JS counterpart of ``examples/communication/tcpip/peer_cross_machine.ipynb``
 * squeezed into one process: "node 1" stores an entry in its own pool and
 * listens; "node 2" peers with it and remembers the entry by ``policy_id``.
 * Point ``NODE1_HOST`` / the port at another machine (Python or JS -- the
 * wire protocol is identical) to run it for real.
 *
 *     node examples/peers_tcp.mjs
 */
import crypto from "node:crypto";
import laila from "../src/index.js";
import * as time from "../src/_compat/time.js";

function main() {
  const SECRET = crypto.randomUUID().replaceAll("-", "");

  // ── Node 1 ────────────────────────────────────────────────────────────────
  const node1 = new laila.DefaultPolicy();
  const node1_tcp = new laila.DefaultTCPIPProtocol({ host: "127.0.0.1", port: 0, peer_secret_key: SECRET });
  laila.active_policy = node1;
  laila.communication.add_connection(node1_tcp);

  const node1_pool = new laila.DefaultPool();
  laila.memory.extend(node1_pool, { affinity: 1.0, pool_nickname: "node1-store" });

  const entry = laila.constant({ message: "stored on Node 1" }, { nickname: "cross-machine-entry" });
  laila.memorize(entry, { pool_nickname: "node1-store" }).wait();
  console.log("Node 1 policy :", node1.global_id);
  console.log("Entry         :", entry.global_id);
  console.log("Node 1 listens:", `${node1_tcp.host}:${node1_tcp.bound_port}`);

  // ── Node 2 ────────────────────────────────────────────────────────────────
  const node2 = new laila.DefaultPolicy();
  const node2_tcp = new laila.DefaultTCPIPProtocol({ host: "127.0.0.1", port: 0, peer_secret_key: SECRET });
  laila.active_policy = node2;
  laila.communication.add_connection(node2_tcp);

  const remote_node1_id = laila.communication.add_tcpip_peer("127.0.0.1", node1_tcp.bound_port, SECRET);
  time.sleep(0.3);
  console.log("Node 2 peered with:", remote_node1_id);

  // Remember straight out of Node 1's pool, from Node 2.
  const remembered = laila.remember(entry.global_id, { policy_id: remote_node1_id, pool_nickname: "node1-store", persist: false });
  console.log("Remote data   :", remembered.data);

  // Or drive Node 1's policy through its proxy, like a local object.
  const remote_node1 = laila.peers[remote_node1_id];
  // (remote calls return plain JSON dumps of the remote object)
  const routed_pool = remote_node1.central.memory.pool_router.route(["any"], { pool_nickname: "node1-store" });
  console.log("Remote pool   :", `${Object.keys(routed_pool.resource).length} entries stored, index_enabled=${routed_pool.index_enabled}`);

  node2.central.communication.stop();
  node1.central.communication.stop();
  laila.terminate();
}

setImmediate(main);
