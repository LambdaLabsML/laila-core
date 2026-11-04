"""Two policies in one process, peered over loopback TCP.

Script form of ``examples/communication/localhost/peer_active_policy.ipynb``:
open a ``DefaultTCPIPProtocol`` on each policy, store an entry on the
remote node, peer the local node to it, and query the remote pool router
through the peer proxy; the call travels over the socket as an RPC.

    python laila-rs/examples/peers_tcp.py
    cargo run -p laila-core --example peers_tcp
"""

import time
import uuid

import laila
from laila.macros.defaults import DefaultPolicy, DefaultPool, DefaultTCPIPProtocol

local_node = DefaultPolicy()
remote_node = DefaultPolicy()

# Both ends must share the peer secret; a random one is enough on loopback.
shared_secret = uuid.uuid4().hex
local_tcp = DefaultTCPIPProtocol(host="127.0.0.1", port=0, peer_secret_key=shared_secret)
remote_tcp = DefaultTCPIPProtocol(host="127.0.0.1", port=0, peer_secret_key=shared_secret)

laila.active_policy = local_node
laila.communication.add_connection(local_tcp)

laila.active_policy = remote_node
laila.communication.add_connection(remote_tcp)
# Start the listeners so each node has a bound port to peer against.
local_node.central.communication.start()
remote_node.central.communication.start()

print("local node: ", local_node.global_id)
print("remote node:", remote_node.global_id)

# Store something on the remote node.
remote_pool = DefaultPool()
laila.memory.extend(pool=remote_pool, pool_nickname="remote-store")
entry = laila.constant(data={"message": "stored on the remote node"}, nickname="remote-entry")
with laila.guarantee:
    laila.memorize(entries=entry, pool_nickname="remote-store")
print("remote pool has entry:", entry.global_id in remote_pool.resource)

# Peer the local node to the remote one over TCP.
laila.active_policy = local_node
laila.communication.add_tcpip_peer("127.0.0.1", remote_tcp.bound_port, remote_tcp.peer_secret_key)
time.sleep(0.3)
print("local peers: ", list(local_node.central.communication.peers) == [remote_node.global_id])
print("remote peers:", list(remote_node.central.communication.peers) == [local_node.global_id])

# The peer registry hands back a proxy whose attribute chain is forwarded
# over the socket: this ``route`` runs on the remote node.
remote_proxy = laila.peers[remote_node.global_id]
routed = remote_proxy.central.memory.pool_router.route(
    entries=[entry.global_id], pool_nickname="remote-store"
)
print("remote route returned a", type(routed).__name__)
print("remote resource holds", len(routed["resource"]), "records")
print("remote batch_accelerated:", routed.get("batch_accelerated"))

# And the other way round: the remote node queries the local node's router.
local_pool = DefaultPool()
laila.memory.extend(pool=local_pool, pool_nickname="local-store")
laila.active_policy = remote_node
local_proxy = laila.peers[local_node.global_id]
local_routed = local_proxy.central.memory.pool_router.route(
    entries=["any-id"], pool_nickname="local-store"
)
print("local resource holds", len(local_routed["resource"]), "records")

laila.active_policy = local_node
laila.communication.remove_connection(local_tcp)
laila.active_policy = remote_node
laila.communication.remove_connection(remote_tcp)
print("connections closed")
laila.terminate()
